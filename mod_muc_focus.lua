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


local iterators = require "util.iterators"
local serialization = require "util.serialization"

-- define namespaces
local xmlns_colibri = "http://jitsi.org/protocol/colibri";
local xmlns_jingle = "urn:xmpp:jingle:1";
local xmlns_jingle_ice = "urn:xmpp:jingle:transports:ice-udp:1";
local xmlns_jingle_dtls = "urn:xmpp:jingle:apps:dtls:0";
local xmlns_jingle_rtp = "urn:xmpp:jingle:apps:rtp:1";
local xmlns_jingle_rtp_headerext = "urn:xmpp:jingle:apps:rtp:rtp-hdrext";
local xmlns_jingle_rtp_feedback = "urn:xmpp:jingle:apps:rtp:rtcp-fb:0";
local xmlns_jingle_rtp_ssma = "urn:xmpp:jingle:apps:rtp:ssma:0";
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

local participant2sources = {}

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

--
-- when someone joins the room, we request a channel for them on the bridge
-- (eventually we will also send a Jingle invitation - see handle_colibri...)
--
local function handle_join(event)
        local room, nick, stanza = event.room, event.nick, event.stanza
        local count = iterators.count(room:each_occupant());
		module:log("debug", "handle_join %s %s %s", 
                   tostring(room), tostring(nick), tostring(stanza));

        -- if there are now two occupants, create a conference
        -- look at room._occupants size?
        module:log("debug", "handle join #occupants %s %d", tostring(room._occupants), count);
        module:log("debug", "room jid %s bridge %s", room.jid, focus_media_bridge)

        -- FIXME: careful about marking this as in progress and dealing with the following scenario:
        -- join, join, create conf iq-get, part, join, create conf iq-result
        -- this should not trigger a new conference to be created but can reuse the created on
        -- just with different participants

        -- do focus stuff only if the client can do multimedia MUC
--        if stanza:get_child("x", xmlns_mmuc) then
--                module:log("info", ("creating a channel for the following participant: " .. origin.from));
--                local channeladd = st.iq({ type="set", from=room..@..host, to=focus_media_bridge }):tag("conference", { xmlns = xmlns_colibri });
--                channelad:tag("content", { name = "sights" }):up();
--                channelad:tag("content", { name = "sounds" }):up();
--                module:send(channeladd);
--        end

        jid2room[room.jid] = room

        local confcreate = st.iq({ from = room.jid, to = focus_media_bridge, type = "set" })
        -- for now, just create a conference for each participant and then ... initiate a jingle session with them
        if roomjid2conference[room.jid] then
            -- update existing conference
            -- FIXME handle -1 aka pending
            module:log("debug", "existing conf id %s", roomjid2conference[room.jid])
            confcreate:tag("conference", { xmlns = "http://jitsi.org/protocol/colibri", id = roomjid2conference[room.jid] })
        else
            confcreate:tag("conference", { xmlns = "http://jitsi.org/protocol/colibri" })
            roomjid2conference[room.jid] = -1 -- pending
        end
        confcreate:tag("content", { name = "audio" })
                :tag("channel", { initiator = "true" }):up():up()
            :tag("content", { name = "video" })
                :tag("channel", { initiator = "true" }):up():up()
            :up():up()

        module:send(confcreate);
        callbacks[confcreate.attr.id] = stanza.attr.from
        module:log("debug", "send_colibri %s", tostring(confcreate))
        return true;
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

        jid2channels[jid] = nil
        participant2sources[jid] = nil
        if count == 0 then
            roomjid2conference[room.jid] = nil
            jid2room[room.jid] = nil
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
        -- for now we're just interested in the result of confcreate
        if callbacks[stanza.attr.id] == nil then return true; end
        module:log("debug", "handle_colibri %s", tostring(event.stanza))

        roomjid2conference[roomjid] = confid
        local room = jid2room[roomjid]

        local occupant_jid = callbacks[stanza.attr.id]
        local occupant = room:get_occupant_by_real_jid(occupant_jid)
        -- FIXME: actually we want to get a particular session of an occupant, not all of them
        module:log("debug", "occupant is %s", tostring(occupant))
        callbacks[stanza.attr.id] = nil

        -- the point is to create a jingle offer from this. at least for results of a 
        -- channel create

        -- FIXME: get_room_from_jid from the muc module? how do we know our muc module?

        local sid = "a73sjjvkla37jfea" -- should be a random string
        local initiate = st.iq({ from = roomjid, type = "set" })
            :tag("jingle", { xmlns = "urn:xmpp:jingle:1", action = "session-initiate", initiator = roomjid, sid = sid })

        --module:log("debug", "MO2 %s", serialization.serialize(participant2sources))
        jid2channels[occupant_jid] = {}
        for jid, sources in pairs(participant2sources) do
            for name, channel in pairs(sources) do
                module:log("debug", "MOO2 %s %s", name, tostring(channel))
            end
        end
        -- iterating the result
        -- should actually be inserting stuff into the offer
        -- or the static parts of the offer get inserted here?
        for content in conf:childtags("content", xmlns_colibri) do
            module:log("debug", "  content name %s", content.attr.name)
            for channel in content:childtags("channel", xmlns_colibri) do
                initiate:tag("content", { creator = "initiator", name = content.attr.name, senders = "both" })
                module:log("debug", "    channel id %s", channel.attr.id)
                jid2channels[occupant_jid][content.attr.name] = channel.attr.id

                if content.attr.name == "audio" then
                    initiate:tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = "audio" })
                        :tag("payload-type", { id = "111", name = "opus", clockrate = "48000", channels = "2" })
                            :tag("parameter", { name = "minptime", value = "10" }):up()
                        :up()
                        :tag("payload-type", { id = "0", name = "PCMU", clockrate = "8000" }):up()
                        :tag("payload-type", { id = "8", name = "PCMA", clockrate = "8000" }):up()

                        :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "1", uri = "urn:ietf:params:rtp-hdrext:ssrc-audio-level" }):up()
                        for jid, sources in pairs(participant2sources) do
                            if sources[content.attr.name] then
                                module:log("debug", 'MOO %s', content.attr.name)
                                initiate:add_child(sources[content.attr.name])
                            end
                        end
                    initiate:up()
                elseif content.attr.name == "video" then
                    initiate:tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = "video" })
                        :tag("payload-type", { id = "100", name = "VP8", clockrate = "90000" })
                            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'ccm', subtype = 'fir' }):up()
                            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'nack' }):up()
                            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'nack', subtype = 'pli' }):up()
                            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'ccm', subtype = 'fir' }):up()
                        :up()
                        -- FIXME: a=rtcp-fb
                        :tag("payload-type", { id = "116", name = "red", clockrate = "90000" }):up()
                        :tag("payload-type", { id = "117", name = "ulpfec", clockrate = "90000" }):up()

                        :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "2", uri = "urn:ietf:params:rtp-hdrext:toffset" }):up()
                        :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "2", uri = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time" }):up()
                        for jid, sources in pairs(participant2sources) do
                            if sources[content.attr.name] then
                                initiate:add_child(sources[content.attr.name])
                            end
                        end
                    initiate:up()
                end
                for transport in channel:childtags("transport", xmlns_jingle_ice) do
                    -- actually we just need to copy the transports
                    -- but this is so much fun
                    initiate:add_child(transport)
                    module:log("debug", "      transport ufrag %s pwd %s", transport.attr.ufrag, transport.attr.pwd)
                    for fingerprint in transport:childtags("fingerprint", xmlns_jingle_dtls) do
                        module:log("debug", "        dtls fingerprint hash %s %s", fingerprint.attr.hash, fingerprint:get_text())
                    end
                    for candidate in transport:childtags("candidate", xmlns_jingle_ice) do
                        module:log("debug", "        candidate ip %s port %s", candidate.attr.ip, candidate.attr.port)
                    end
                end
                initiate:up() -- content
            end
        end
        initiate:up() -- jingle
        initiate:up()
        room:route_to_occupant(occupant, initiate)
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
        local confid = roomjid2conference[roomjid]
        module:log("debug", "confid %s", tostring(confid))

        local channels = jid2channels[stanza.attr.from]
        local confupdate = st.iq({ from = roomjid, to = focus_media_bridge, type = "set" })
            :tag("conference", { xmlns = "http://jitsi.org/protocol/colibri", id = confid })

        for content in jingle:childtags("content", xmlns_jingle) do
            module:log("debug", "    content name %s", content.attr.name)
            confupdate:tag("content", { name = content.attr.name })
            confupdate:tag("channel", { initiator = "true", id = channels[content.attr.name] })
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

        -- iterate again to look at the SSMA source elements
        if participant2sources[stanza.attr.from] == nil then
            participant2sources[stanza.attr.from] = {}
        end
        for content in jingle:childtags("content", xmlns_jingle) do
            for description in content:childtags("description", xmlns_jingle_rtp) do
                for source in description:childtags("source", xmlns_jingle_rtp_ssma) do
                    -- note those and add them to the participants presence
                    -- FIXME: just the msid

                    -- and also to subsequent offers (full elements)
                    participant2sources[stanza.attr.from][content.attr.name] = source
                    module:log("debug", "source %s content %s", source.attr.ssrc, content.attr.name)
                end
            end
        end
        module:send(confupdate);
        return true;
end
module:hook("iq/bare", handle_jingle, 2);

--
-- end Jingle functions
--

log("info", "mod_muc_focus loaded");
