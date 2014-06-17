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
--local host = module.get_host();

-- get data from the configuration file
local focus_mmuc = module:get_option_string("focus_mmuc"); -- all rooms do MMUC
-- FIXME: at some point we might want to change focus_media_bridge to support multiple bridges, but for bootstrapping purposes we support only one
local focus_media_bridge = module:get_option_string("focus_media_bridge");
-- FIXME: better to get the content types from room configuration or Jingle sessions?
--local focus_content_types = module:get_option_array("focus_content_types");


local iterators = require "util.iterators";

-- define namespaces
local xmlns_colibri = "http://jitsi.org/protocol/colibri";
local xmlns_jingle = "urn:xmpp:jingle:1";
local xmlns_jingle_ice = "urn:xmpp:jingle:transports:ice-udp:1";
local xmlns_jingle_dtls = "urn:xmpp:jingle:apps:dtls:0";
local xmlns_jingle_rtp = "urn:xmpp:jingle:apps:rtp:1";
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

--
-- when a MUC room is created, we request a conference on the media bridge
--
local function create_conference(event)
        log("info", ("creating a conference for the following room: " .. room));
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
--module:hook("muc-room-created", create_conference, 2);

--
-- when someone joins the room, we request a channel for them on the bridge
-- (eventually we will also send a Jingle invitation - see handle_colibri...)
--
local function handle_join(event)
        local room, nick, stanza = event.room, event.nick, event.stanza
        local count = iterators.count(room:each_occupant());
		log("debug", "handle_join %s %s %s", 
                   tostring(room), tostring(nick), tostring(stanza));

        -- if there are now two occupants, create a conference
        -- look at room._occupants size?
        log("debug", "handle join #occupants %s %d", tostring(room._occupants), count);
        log("debug", "room jid %s bridge %s", room.jid, focus_media_bridge)

        -- FIXME: careful about marking this as in progress and dealing with the following scenario:
        -- join, join, create conf iq-get, part, join, create conf iq-result
        -- this should not trigger a new conference to be created but can reuse the created on
        -- just with different participants

        -- do focus stuff only if the client can do multimedia MUC
--        if stanza:get_child("x", xmlns_mmuc) then
--                log("info", ("creating a channel for the following participant: " .. origin.from));
--                local channeladd = st.iq({ type="set", from=room..@..host, to=focus_media_bridge }):tag("conference", { xmlns = xmlns_colibri });
--                channelad:tag("content", { name = "sights" }):up();
--                channelad:tag("content", { name = "sounds" }):up();
--                module:send(channeladd);
--        end


        -- for now, just create a conference for each participant and then ... initiate a jingle session with them
        -- stanzaconv f4w
        local confcreate = st.iq({ from = room.jid, to = focus_media_bridge, type = "set" })
            :tag("conference", { xmlns = "http://jitsi.org/protocol/colibri" })
                :tag("content", { name = "audio" })
                    :tag("channel", { initiator = "true" }):up():up()
                :tag("content", { name = "video" })
                    :tag("channel", { initiator = "true" }):up():up()
            :up():up()

        module:log("debug", "to bridge %s", tostring(confcreate))
        module:send(confcreate);
        return true;
end
module:hook("muc-occupant-joined", handle_join, 2);

local function handle_leave(event)
        local room, nick, stanza = event.room, event.nick, event.stanza
        local count = iterators.count(room:each_occupant());
		module:log("debug", "handle_leave %s %s %s", 
                   tostring(room), tostring(nick), tostring(stanza));
        -- same here, remove conference when there are now
        -- less than two participants in the room
        -- optimization: keep the conference a little longer
        -- to allow for fast rejoins
        return true;
end
module:hook("muc-occupant-left", handle_leave, 2);

--
-- things we do when a room receives a COLIBRI stanza from the bridge 
--
local function handle_colibri(event)
        local stanza = event.stanza
        if stanza.attr.type ~= "result" then return; end
        -- FIXME: actually the bridge sends funny set's sometimes
        if stanza:get_child("conference", xmlns_colibri) == nil then return; end

        log("debug", "handle_colibri %s", tostring(event.stanza))
        log("debug", "%s %s %s", stanza.attr.from, stanza.attr.to, stanza.attr.type)
        local conf = stanza:find("{" .. xmlns_colibri .."}conference");
        local confid = conf.attr.id;
        log("debug", "conf id %s", confid)


        -- the point is to create a jingle offer from this. at least for results of a 
        -- channel create

        local roomjid = stanza.attr.to
        -- FIXME: get_room_from_jid from the muc module? how do we know our muc module?
        local sid = "a73sjjvkla37jfea" -- should be a random string
        local initiate = st.iq({ from = roomjid, to = "juliet@capulet.lit/balcony", type = "set" })
            :tag("jingle", { xmlns = "urn:xmpp:jingle:1", action = "session-initiate", initiator = roomjid, sid = sid })
                :tag("content", { creator = "initiator", name = "voice", senders = "both" })
                    -- FIXME add the static offer stuff for audio
                    :tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = "audio" })
                        :tag("payload-type", { id = "96", name = "speex", clockrate = "16000" }):up()
                        :tag("payload-type", { id = "97", name = "speex", clockrate = "8000" }):up()
                        :tag("payload-type", { id = "18" }):up()
                        :tag("payload-type", { id = "103", name = "L16", clockrate = "16000" }):up()
                        :tag("payload-type", { id = "98", name = "x-ISAC", clockrate = "8000" }):up()
                        -- FIXME: a=extmap
                    :up()
                    :tag("transport", { xmlns = "urn:xmpp:jingle:transports:ice-udp:1", pwd = "asd88fgpdd777uzjYhagZg", ufrag = "8hhy" })
                        :tag("candidate", { component = "1", foundation = "1", generation = "0", id = "el0747fg11", ip = "10.0.1.1", network = "1", port = "8998", protocol = "udp", type = "host" }):up()
                        :tag("candidate", { component = "1", generation = "0", network = "1", port = "45664", priority = "1694498815", protocol = "udp", ["rel-addr"] = "10.0.1.1", ["rel-port"] = "8998", type = "srflx" }):up()
                    :up()
                :up()
                :tag("content", { creator = "initiator", name = "video", senders = "both" })
                    -- FIXME add the static offer stuff for video
                    :tag("description", { xmlns = "urn:xmpp:jingle:apps:rtp:1", media = "video" })
                        -- FIXME: a=extmap
                        -- FIXME: a=rtcp-fb
                    :up()
                :up()
            :up()
        :up()

        -- iterating the result
        -- should actually be inserting stuff into the offer
        -- or the static parts of the offer get inserted here?
        for content in conf:childtags("content", xmlns_colibri) do
            log("debug", "  content name %s", content.attr.name)
            for channel in content:childtags("channel", xmlns_colibri) do
                log("debug", "    channel id %s", channel.attr.id)
                for transport in channel:childtags("transport", xmlns_jingle_ice) do
                    -- actually we just need to copy the transports
                    -- but this is so much fun
                    log("debug", "      transport ufrag %s pwd %s", transport.attr.ufrag, transport.attr.pwd)
                    for fingerprint in transport:childtags("fingerprint", xmlns_jingle_dtls) do
                      log("debug", "        dtls fingerprint hash %s %s", fingerprint.attr.hash, fingerprint:get_text())
                    end
                    for candidate in transport:childtags("candidate", xmlns_jingle_ice) do
                      log("debug", "        candidate ip %s port %s", candidate.attr.ip, candidate.attr.port)
                    end
                end
            end
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
        if stanza:get_child("jingle", xmlns_jingle) == nil then return; end
        log("debug", "handle_jingle %s %s", tostring(session), tostring(stanza))
        return true;
        --log("info", ("sending a Jingle invitation to the following participant: " .. origin.from));
end
module:hook("iq/bare", handle_jingle, 2);

--
-- end Jingle functions
--

log("info", "mod_muc_focus loaded");
