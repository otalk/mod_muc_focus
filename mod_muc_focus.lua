-- Module: mod_muc_focus
-- Author: Peter Saint-Andre

--[[

OVERVIEW

This module enables Prosody to act as a "focus agent" for a 
multimedia conference. 

Basically, when this module is enabled, a Multi-User Chat room 
(XEP-0045) speaks Jingle (XEP-0166, XEP-0167, XEP-0176, etc.) to 
XMPP clients and speaks COLIBRI (XEP-0340) to media bridges (e.g.
media mixers and selective forwarding units).

In particular, when a participant joins the room and advertises 
support for the urn:xmpp:multimedia-muc feature, the module:
  
1. allocates a set of RTP/RTCP ports (called a "channel") on 
   the media bridge, typically with one channel for audio and 
   one for video (and potentially other channels, such as SCTP 
   for WebRTC datachannels); this is done with COLIBRI (XEP-0340)

2. sends a Jingle session-initiate message from the MUC room's 
   bare JID to the participant's real JID

]]

--[[

CONFIGURATION

Add mod_muc_focus to the "modules_enabled" for the relevant
MUC domain...

    Component "conference.example.com" "muc"
              modules_enabled = { "mod_muc_focus" }

              focus_mmuc = true
              focus_media_bridge = domain.name.of.bridge

]]

-- invoke various utilities
local st = require "util.stanza";
local config = require "core.configmanager";
local setmetatable = setmetatable;
--local host = module.get_host();

-- get data from the configuration file
local focus_mmuc = module:get_option_string("focus_mmuc"); -- all rooms do MMUC
-- FIXME: at some point we might want to change focus_media_bridge to support multiple bridges, but for bootstrapping purposes we support only one
local focus_media_bridge = module:get_option_string("focus_media_bridge");
-- FIXME: better to get the content types from room configuration or Jingle sessions?
--local focus_content_types = module:get_option_array("focus_content_types");
local focus_datachannels = true


local iterators = require "util.iterators"
local serialization = require "util.serialization"

-- define namespaces
local xmlns_colibri = "http://jitsi.org/protocol/colibri";
local xmlns_jingle = "urn:xmpp:jingle:1";
local xmlns_jingle_ice = "urn:xmpp:jingle:transports:ice-udp:1";
local xmlns_jingle_dtls = "urn:xmpp:jingle:apps:dtls:0";
local xmlns_jingle_rtp = "urn:xmpp:jingle:apps:rtp:1";
local xmlns_jingle_rtp_headerext = "urn:xmpp:jingle:apps:rtp:rtp-hdrext:0";
local xmlns_jingle_rtp_feedback = "urn:xmpp:jingle:apps:rtp:rtcp-fb:0";
local xmlns_jingle_rtp_ssma = "urn:xmpp:jingle:apps:rtp:ssma:0";
local xmlns_jingle_sctp = "urn:xmpp:jingle:transports:dtls-sctp:1";
local xmlns_mmuc = "urn:xmpp:mmuc:0";

-- advertise features
module:add_feature(xmlns_colibri);
module:add_feature(xmlns_jingle);
module:add_feature(xmlns_jingle_ice);
module:add_feature(xmlns_jingle_rtp);
module:add_feature(xmlns_jingle_dtls);
module:add_feature(xmlns_mmuc);

-- we need an array that associates a room with a conference ID
local conference_array = {};

-- map room jid to conference id
local roomjid2conference = {} -- should probably be roomjid2conference?

-- map muc jid to room object -- we should not need this
local jid2room = {}

-- map jid to channels
local jid2channels = {} -- should actually contain the participant muc jid or be tied to the room

-- all the a=ssrc lines
local participant2sources = {}

-- people waiting to join
local pending = {}

-- endpoints associated with that
local endpoints = {}

-- our custom *cough* iq callback mechanism
local callbacks = {}

--
-- when a MUC room is created, we request a conference on the media bridge
--
local function create_conference(event)
        module:log("info", ("creating a conference for the following room: " .. room));
--        local confcreate = st.iq({ type="set", from=room..@..host, to=focus_media_bridge }):conference(xmlns_colibri);
        -- FIXME: how do we determine the number and types of content?
        -- FIXME: hardcode to audio and video for now, sigh
        -- these aren't really media types, so we give them silly names...
--        confcreate:tag("content", { name= "sights" }):up();
--        confcreate:tag("content", { name= "sounds" }):up();
        -- for now we don't request any channels; we'll do that as participants join
--        module:send(confcreate);
        return true;
end
-- not in prosody-trunk? but we dont want to create the conference on room creation anyway
-- muc-room-pre-create maybe?
--module:hook("muc-room-created", create_conference, 2);

-- only generated for non-persistent rooms
--module:hook("muc-room-destroyed", function(event)
--    module:log("info", "muc room destroyed %s", event.room)
--end)

local function create_channels(stanza, endpoints)
    stanza:tag("content", { name = "audio" })
    for i = 1,#endpoints do
        stanza:tag("channel", { initiator = "true", endpoint = endpoints[i] }):up()
    end
    stanza:up()
    
    stanza:tag("content", { name = "video" })
    for i = 1,#endpoints do
        stanza:tag("channel", { initiator = "true", endpoint = endpoints[i] }):up()
    end
    stanza:up()

    if focus_datachannels then
        -- note: datachannels will soon not be inside content anymore
        stanza:tag("content", { name = "data" })
        for i = 1,#endpoints do
            stanza:tag("sctpconnection", { initiator = "true", 
                 endpoint = endpoints[i], -- FIXME: I want the msid which i dont know here yet
                 port = 5000 -- it should not be port, this is the sctpmap
            }):up()
        end
        stanza:up()
    end
    stanza:up():up()
end

--
-- when someone joins the room, we request a channel for them on the bridge
-- (eventually we will also send a Jingle invitation - see handle_colibri...)
--
local function handle_join(event)
        local room, nick, stanza = event.room, event.nick, event.stanza
        local count = iterators.count(room:each_occupant());
		module:log("debug", "handle_join %s %s %s", 
                   tostring(room), tostring(nick), tostring(stanza))

        -- if there are now two occupants, create a conference
        -- look at room._occupants size?
        module:log("debug", "handle join #occupants %s %d", tostring(room._occupants), count)
        module:log("debug", "room jid %s bridge %s", room.jid, focus_media_bridge)

        -- check client caps
        -- currently hardcoded
		local caps = stanza:get_child("c", "http://jabber.org/protocol/caps")
        if caps then
            module:log("debug", "caps ver %s", caps.attr.ver)
            -- currently jts2118m3Eaq5FILAt7qGmRc+8M= is firefox without colibri/multistream support
            if caps.attr.ver == "jts2118m3Eaq5FILAt7qGmRc+8M=" then
                return true
            end
        end

        -- FIXME: this doesn't allow us to clean up on leave
        if pending[room.jid] == nil then pending[room.jid] = {}; end
        if endpoints[room.jid] == nil then endpoints[room.jid] = {}; end
        pending[room.jid][#pending[room.jid]+1] = stanza.attr.from
        endpoints[room.jid][#endpoints[room.jid]+1] = nick
        module:log("debug", "pending %d count %d", #pending[room.jid], count)
        if count == 1 then return true; end

        jid2room[room.jid] = room

        local confcreate = st.iq({ from = room.jid, to = focus_media_bridge, type = "set" })
        -- for now, just create a conference for each participant and then ... initiate a jingle session with them
        if roomjid2conference[room.jid] == nil then -- create a conference
            module:log("debug", "creating conference for %s", room.jid)
            confcreate:tag("conference", { xmlns = xmlns_colibri })
            roomjid2conference[room.jid] = -1 -- pending
            --confcreate:tag("recording", { state = "true", token = "recordersecret" }):up() -- recording
        elseif roomjid2conference[room.jid] == -1 then
            -- FIXME push to a queue that is sent once we get the callback 
            -- for the create request
            module:log("debug", "FIXME new participant while conference creation is pending")
            return true;
        else -- update existing conference
            module:log("debug", "existing conf id %s", roomjid2conference[room.jid])
            confcreate:tag("conference", { xmlns = xmlns_colibri, id = roomjid2conference[room.jid] })
        end

        -- FIXME: careful about marking this as in progress and dealing with the following scenario:
        -- join, join, create conf iq-get, part, join, create conf iq-result
        -- this should not trigger a new conference to be created but can reuse the created on
        -- just with different participants

        create_channels(confcreate, endpoints[room.jid])
        module:send(confcreate);
        callbacks[confcreate.attr.id] = pending[room.jid]
        pending[room.jid] = nil
        endpoints[room.jid] = nil
        module:log("debug", "send_colibri %s", tostring(confcreate))
        return true
end
module:hook("muc-occupant-joined", handle_join, 2);
-- possibly we need to hook muc-occupant-session-new instead 
-- for cases where a participant joins with twice

local function handle_leave(event)
        -- why doesn't this pass the stanza?
        local room, nick, stanza, jid = event.room, event.nick, event.stanza, event.jid
        local count = iterators.count(room:each_occupant());
		module:log("debug", "handle_leave %s %s %s %s, #occupants %d", 
                   tostring(room), tostring(nick), tostring(stanza), tostring(jid), count);
        -- same here, remove conference when there are now
        -- less than two participants in the room
        -- optimization: keep the conference a little longer
        -- to allow for fast rejoins

        -- we might close those immediately by setting their expire to 0
        local channels = jid2channels[jid] 
        if channels then
            jid2channels[jid] = nil
        else
            --module:log("debug", "handle_leave: no channels found")
        end

        if participant2sources[room.jid] and participant2sources[room.jid][jid] then
            local sources = participant2sources[room.jid][jid]
            if sources then
                -- we need to send source-remove for these
                module:log("debug", "source-remove")
                local sid = "a73sjjvkla37jfea" -- should be a random string
                local sourceremove = st.iq({ from = room.jid, type = "set" })
                    :tag("jingle", { xmlns = "urn:xmpp:jingle:1", action = "source-remove", initiator = room.jid, sid = sid })
                for name, sourcelist in pairs(sources) do
                    sourceremove:tag("content", { creator = "initiator", name = name, senders = "both" })
                        :tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = name })
                        for i, source in ipairs(sourcelist) do
                            sourceremove:add_child(source)
                        end
                        sourceremove:up() -- description
                    :up() -- content
                end

                participant2sources[room.jid][jid] = nil

                for occupant_jid in iterators.keys(participant2sources[room.jid]) do
                    if occupant_jid ~= jid then -- cant happen i think
                        module:log("debug", "send source-remove to %s", tostring(occupant_jid))
                        local occupant = room:get_occupant_by_real_jid(occupant_jid)
                        room:route_to_occupant(occupant, sourceremove)
                    end
                end
            end
        end

        -- look whether the participant leaving is on our pending list
        -- and clean that up
        if pending[room.jid] then
            for i, value in ipairs(pending[room.jid]) do
                if (value == jid) then
                    pending[room.jid][i] = nil
                    endpoints[room.jid][i] = nil
                    break
                end
            end
        end

        if count == 1 then -- the room is empty
            local sid = "a73sjjvkla37jfea" -- should be a random string
            local terminate = st.iq({ from = room.jid, type = "set" })
                :tag("jingle", { xmlns = "urn:xmpp:jingle:1", action = "session-terminate", initiator = room.jid, sid = sid })
                  :tag("reason")
                    :tag("busy"):up()
                  :up()
                :up()
            for occupant_jid in iterators.keys(participant2sources[room.jid]) do
                local occupant = room:get_occupant_by_real_jid(occupant_jid)
                if occupant then room:route_to_occupant(occupant, terminate) end
            end

            -- set remaining participant as pending
            pending[room.jid] = {}
            endpoints[room.jid] = {}
            for nick, occupant in room:each_occupant() do
                pending[room.jid][#pending[room.jid]+1] = occupant.jid
                endpoints[room.jid][#endpoints[room.jid]+1] = nick
            end
        end
        if count <= 1 then
            roomjid2conference[room.jid] = nil
            jid2room[room.jid] = nil
            participant2sources[room.jid] = nil
        end
        if count == 0 then
            pending[room.jid] = nil
            endpoints[room.jid] = nil
        end
        return true;
end
module:hook("muc-occupant-left", handle_leave, 2);

--
-- things we do when a room receives a COLIBRI stanza from the bridge 
--
local function handle_colibri(event)
        local stanza = event.stanza

        local conf = stanza:get_child("conference", xmlns_colibri)
        if conf == nil then return; end


        if stanza.attr.type ~= "result" then return true; end
        module:log("debug", "%s %s %s", stanza.attr.from, stanza.attr.to, stanza.attr.type)

        local confid = conf.attr.id
        module:log("debug", "conf id %s", confid)

        local roomjid = stanza.attr.to
        if callbacks[stanza.attr.id] == nil then return true; end
        module:log("debug", "handle_colibri %s", tostring(event.stanza))

        roomjid2conference[roomjid] = confid
        local room = jid2room[roomjid]

        --local occupant_jid = callbacks[stanza.attr.id]
        local occupants = {}
        for idx, occupant_jid in pairs(callbacks[stanza.attr.id]) do
            -- FIXME: actually we want to get a particular session of an occupant, not all of them
            local occupant = room:get_occupant_by_real_jid(occupant_jid)
            module:log("debug", "occupant is %s", tostring(occupant))
            occupants[idx] = occupant
        end
        callbacks[stanza.attr.id] = nil


        -- FIXME: get_room_from_jid from the muc module? how do we know our muc module?

        for channelnumber = 1, #occupants do
            local sid = "a73sjjvkla37jfea" -- should be a random string
            local initiate = st.iq({ from = roomjid, type = "set" })
                :tag("jingle", { xmlns = "urn:xmpp:jingle:1", action = "session-initiate", initiator = roomjid, sid = sid })

            local occupant = occupants[channelnumber]
            local occupant_jid = occupant.jid
            jid2channels[occupant_jid] = {}

            if participant2sources[room.jid] == nil then
                participant2sources[room.jid] = {}
            end

            for content in conf:childtags("content", xmlns_colibri) do
                module:log("debug", "  content name %s", content.attr.name)
                local channel = nil
                if content.attr.name == "audio" or content.attr.name == "video" then
                    channel = iterators.to_array(content:childtags("channel", xmlns_colibri))[channelnumber]

                    initiate:tag("content", { creator = "initiator", name = content.attr.name, senders = "both" })
                    module:log("debug", "    channel id %s", channel.attr.id)
                    jid2channels[occupant_jid][content.attr.name] = channel.attr.id

                    if content.attr.name == "audio" then -- build audio descrіption
                        initiate:tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = "audio" })
                            :tag("payload-type", { id = "111", name = "opus", clockrate = "48000", channels = "2" })
                                :tag("parameter", { name = "minptime", value = "10" }):up()
                            :up()
                            :tag("payload-type", { id = "0", name = "PCMU", clockrate = "8000" }):up()
                            :tag("payload-type", { id = "8", name = "PCMA", clockrate = "8000" }):up()
                            :tag("payload-type", { id = "103", name = "ISAC", clockrate = "16000" }):up()
                            :tag("payload-type", { id = "104", name = "ISAC", clockrate = "32000" }):up()

                            :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "1", uri = "urn:ietf:params:rtp-hdrext:ssrc-audio-level" }):up()
                            :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "3", uri = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time" }):up()

                            for jid, sources in pairs(participant2sources[room.jid]) do
                                if sources[content.attr.name] then
                                    for i, source in ipairs(sources[content.attr.name]) do
                                        initiate:add_child(source)
                                    end
                                end
                            end
                        initiate:up()
                    elseif content.attr.name == "video" then -- build video description
                        initiate:tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = "video" })
                            :tag("payload-type", { id = "100", name = "VP8", clockrate = "90000" })
                                :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'ccm', subtype = 'fir' }):up()
                                :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'nack' }):up()
                                :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'nack', subtype = 'pli' }):up()
                                :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'goog-remb' }):up()
                            :up()
                            :tag("payload-type", { id = "116", name = "red", clockrate = "90000" }):up()
                            --:tag("payload-type", { id = "117", name = "ulpfec", clockrate = "90000" }):up()

                            :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "2", uri = "urn:ietf:params:rtp-hdrext:toffset" }):up()
                            :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "3", uri = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time" }):up()

                            for jid, sources in pairs(participant2sources[room.jid]) do
                                if sources[content.attr.name] then
                                    for i, source in ipairs(sources[content.attr.name]) do
                                        initiate:add_child(source)
                                    end
                                end
                            end
                        initiate:up()
                    end
                elseif content.attr.name == "data" then
                    -- data channels are handled differently -- currently
                    channel = iterators.to_array(content:childtags("sctpconnection", xmlns_colibri))[channelnumber]
                    initiate:tag("content", { creator = "initiator", name = content.attr.name, senders = "both" })
                    initiate:tag("description", { xmlns = "http://talky.io/ns/datachannel" })
                        -- no description yet. describe the channels?
                        :up()
                end
                if channel then -- add transport
                    for transport in channel:childtags("transport", xmlns_jingle_ice) do
                        -- add a XEP-0343 sctpmap element
                        for fingerprint in transport:childtags("fingerprint", xmlns_jingle_dtls) do
                            fingerprint.attr.setup = "actpass"
                        end
                        if content.attr.name == "data" then
                            transport:tag("sctpmap", { xmlns = xmlns_jingle_sctp, number = channel.attr.port, protocol = "webrtc-datachannel", streams = 1024 }):up()
                        end
                        initiate:add_child(transport)

                        -- actually we just need to copy the transports
                        -- but this is so much fun
                        module:log("debug", "      transport ufrag %s pwd %s", transport.attr.ufrag, transport.attr.pwd)
                        for fingerprint in transport:childtags("fingerprint", xmlns_jingle_dtls) do
                            module:log("debug", "        dtls fingerprint hash %s %s", fingerprint.attr.hash, fingerprint:get_text())
                        end
                        for candidate in transport:childtags("candidate", xmlns_jingle_ice) do
                            module:log("debug", "        candidate ip %s port %s", candidate.attr.ip, candidate.attr.port)
                        end
                    end
                end
                initiate:up() -- content
            end
            initiate:up() -- jingle
            initiate:up()
            room:route_to_occupant(occupant, initiate)

            -- preoccupy here
            participant2sources[room.jid][occupant_jid] = {}
        end
        -- if receive conference element with unknown ID, associate the room with this conference ID
--        if not conference_array[confid] then
--                conference_array[id] = stanza.attr.to; -- FIXME: test first to see if the room exists?
--        else 
                -- this is a conference we know about, what next?? ;-)
                -- well, it seems we need to parse the <conference/> element;
                -- thus we will inspect various channels in order to:
                -- 1. update existing channel definitions
                -- 2. process new channels
--        end

        -- if receive conference with known ID but unknown channel ID...
        return true
end
module:hook("iq/bare", handle_colibri, 2);

local function handle_jingle(event)
        -- process incoming Jingle stanzas from clients
        local session, stanza = event.origin, event.stanza;
        local jingle = stanza:get_child("jingle", xmlns_jingle)
        if jingle == nil then return; end
        --module:log("debug", "handle_jingle %s %s", tostring(session), tostring(stanza))
        --module:log("info", ("sending a Jingle invitation to the following participant: " .. origin.from));

        -- FIXME: this is not the in-muc from so we need to either change the handler
        -- or look up the participant based on the real jid
        module:log("debug", "handle_jingle %s from %s", jingle.attr.action, stanza.attr.from)
        local roomjid = stanza.attr.to
        local room = jid2room[roomjid]
        local confid = roomjid2conference[roomjid]
        local action = jingle.attr.action
        local sender = room:get_occupant_by_real_jid(stanza.attr.from)

        -- iterate again to look at the SSMA source elements
        -- FIXME: only for session-accept and source-add / source-remove?
        local sources = {}

        -- FIXME: there could be multiple msids per participant and content
        -- but we try to avoid that currently
        local msids = {} 
        for content in jingle:childtags("content", xmlns_jingle) do
            for description in content:childtags("description", xmlns_jingle_rtp) do
                local sourcelist = {}
                for source in description:childtags("source", xmlns_jingle_rtp_ssma) do
                    -- note those and add the msid to the participants presence
                    for parameter in source:childtags("parameter", xmlns_jingle_rtp_ssma) do
                        if parameter.attr.name == "msid" then
                            local msid = string.match(parameter.attr.value, "[a-zA-Z0-9]+") -- FIXME: token-char
                            -- second part is the track
                            msids[msid] = true
                            module:log("debug", "msid %s content %s", msid, content.attr.name)
                        end
                    end

                    -- and also to subsequent offers (full elements)
                    sourcelist[#sourcelist+1] = source
                    module:log("debug", "source %s content %s", source.attr.ssrc, content.attr.name)
                end
                sources[content.attr.name] = sourcelist 
            end
        end

        module:log("debug", "confid %s", tostring(confid))

        local channels = jid2channels[stanza.attr.from]
        local confupdate = st.iq({ from = roomjid, to = focus_media_bridge, type = "set" })
            :tag("conference", { xmlns = xmlns_colibri, id = confid })

        for content in jingle:childtags("content", xmlns_jingle) do
            module:log("debug", "    content name %s", content.attr.name)
            confupdate:tag("content", { name = content.attr.name })
            if content.attr.name == "data" then
                confupdate:tag("sctpconnection", { initiator = "true", endpoint = sender.nick })
            else
                confupdate:tag("channel", { initiator = "true", id = channels[content.attr.name], endpoint = sender.nick })
            end
            for description in content:childtags("description", xmlns_jingle_rtp) do
                module:log("debug", "      description media %s", description.attr.media)
                for payload in description:childtags("payload-type", xmlns_jingle_rtp) do
                    module:log("debug", "        payload name %s", payload.attr.name)
                    confupdate:add_child(payload)
                end
            end
            for transport in content:childtags("transport", xmlns_jingle_ice) do
                module:log("debug", "      transport ufrag %s pwd %s", transport.attr.ufrag, transport.attr.pwd)
                for fingerprint in transport:childtags("fingerprint", xmlns_jingle_dtls) do
                  module:log("debug", "        dtls fingerprint hash %s %s", fingerprint.attr.hash, fingerprint:get_text())
                end
                for candidate in transport:childtags("candidate", xmlns_jingle_ice) do
                  module:log("debug", "        candidate ip %s port %s", candidate.attr.ip, candidate.attr.port)
                end
                confupdate:add_child(transport)
            end
            confupdate:up() -- channel
            confupdate:up() -- content
        end
        module:log("debug", "confupdate is %s", tostring(confupdate))
        module:send(confupdate);


        if participant2sources[room.jid] == nil then
            participant2sources[room.jid] = {}
        end

        if action == "session-accept" or action == "source-add" or action == "source-remove" then
            -- update participant presence with a <media xmlns=...><source type=audio ssrc=... direction=sendrecv/>...</media>
            -- or the new plan to tell the MSID
            local pr = sender:get_presence()
            for msid, foo in pairs(msids) do
                pr:tag("mediastream", { xmlns = "http://andyet.net/xmlns/mmuc", msid = msid }):up()
            end
            --pr:tag("media", {xmlns = "http://.../ns/mjs"})
            --for name, source in pairs(sources) do
            --    pr:tag("source", { type = name, ssrc = source.attr.ssrc, direction = "sendrecv" }):up();
            --end
            sender:set_session(stanza.attr.from, pr)
			local x = st.stanza("x", {xmlns = "http://jabber.org/protocol/muc#user";});
            room:publicise_occupant_status(sender, x);


            -- FIXME handle updates and removals
            participant2sources[room.jid][stanza.attr.from] = sources
            local sid = "a73sjjvkla37jfea" -- should be a random string
            local sendaction = "source-add"
            if action == "source-remove" then
                sendaction = "source-remove"
            end
            local sourceadd = st.iq({ from = roomjid, type = "set" })
                :tag("jingle", { xmlns = "urn:xmpp:jingle:1", action = sendaction, initiator = roomjid, sid = sid })
            for name, sourcelist in pairs(sources) do
                sourceadd:tag("content", { creator = "initiator", name = name, senders = "both" })
                    :tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = name })
                    for i, source in ipairs(sourcelist) do
                        sourceadd:add_child(source)
                    end

                    sourceadd:up() -- description
                :up() -- content
            end

            -- sent to everyone but the sender
            for occupant_jid in iterators.keys(participant2sources[room.jid]) do
                if occupant_jid ~= stanza.attr.from then
                    module:log("debug", "send %s to %s", sendaction, tostring(occupant_jid))
                    local occupant = room:get_occupant_by_real_jid(occupant_jid)
                    if (occupant) then -- FIXME: when does this happen
                        room:route_to_occupant(occupant, sourceadd)
                    else
                        module:log("debug", "not found %s", sendaction)
                    end
                end
            end
        end
        return true;
end
module:hook("iq/bare", handle_jingle, 2);

--
-- end Jingle functions
--

log("info", "mod_muc_focus loaded");
