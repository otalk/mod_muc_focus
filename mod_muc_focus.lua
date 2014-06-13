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
local config = require "core.configmanager";
local host = module.get_host();

-- get data from the configuration file
local focus_mmuc = module:get_option_string("focus_mmuc"); -- all rooms do MMUC
-- FIXME: at some point we might want to change focus_media_bridge to support multiple bridges, but for bootstrapping purposes we support only one
local focus_media_bridge = module:get_option_string("focus_media_bridge");
-- FIXME: better to get the content types from room configuration or Jingle sessions?
--local focus_content_types = module:get_option_array("focus_content_types");

-- define namespaces
local xmlns_colibri = "http://jitsi.org/protocol/colibri";
local xmlns_jingle = "urn:xmpp:jingle:1";
local xmlns_jingle_ice = "urn:xmpp:jingle:transports:ice-udp:1";
local xmlns_jingle_rtp = "urn:xmpp:jingle:apps:rtp:1";
local xmlns_mmuc = "urn:xmpp:mmuc:0";

-- advertise features
module:add_feature(xmlns_colibri);
module:add_feature(xmlns_jingle);
module:add_feature(xmlns_jingle_ice);
module:add_feature(xmlns_jingle_rtp);
module:add_feature(xmlns_mmuc);

-- we need an array that associates a room with a conference ID
local conference_array = {};

--
-- when a MUC room is created, we request a conference on the media bridge
--
local function create_conference(event)
        log("info", ("creating a conference for the following room: " .. room));
        local confcreate = st.iq({ type="set", from=room..@..host, to=focus_media_bridge }):conference(xmlns_colibri);
        -- FIXME: how do we determine the number and types of content?
        -- FIXME: hardcode to audio and video for now, sigh
        -- these aren't really media types, so we give them silly names...
        confcreate:tag("content", { name= "sights" }):up();
        confcreate:tag("content", { name= "sounds" }):up();
        -- for now we don't request any channels; we'll do that as participants join
        module:send(confcreate);
        return true;
end
module:hook("muc-room-created", create_conference, 2);

--
-- when someone joins the room, we request a channel for them on the bridge
-- (eventually we will also send a Jingle invitation - see handle_colibri...)
--
local function handle_join(event)
        -- FIXME: we don't care about presence or nick changes, only room joins

        -- do focus stuff only if the client can do multimedia MUC
        if stanza:get_child("x", xmlns_mmuc) then
                log("info", ("creating a channel for the following participant: " .. origin.from));
                local channeladd = st.iq({ type="set", from=room..@..host, to=focus_media_bridge }):tag("conference", { xmlns = xmlns_colibri });
                channelad:tag("content", { name = "sights" }):up();
                channelad:tag("content", { name = "sounds" }):up();
                module:send(channeladd);
        end
        return true;
end
module:hook("presence/full", handle_join, 2);

--
-- things we do when a room receives a COLIBRI stanza from the bridge 
--
local function handle_colibri(event)
        local session, stanza = event.origin, event.stanza;
        local conf = stanza:find("{xmlns_colibri}conference");
        local confid = conf.attr.id;
        -- if receive conference element with unknown ID, associate the room with this conference ID
        if not conference_array[confid] then
                conference_array[id] = stanza.attr.to; -- FIXME: test first to see if the room exists?
        else 
                -- this is a conference we know about, what next?? ;-)
                -- well, it seems we need to parse the <conference/> element;
                -- thus we will inspect various channels in order to:
                -- 1. update existing channel definitions
                -- 2. process new channels
        end

        -- if receive conference with known ID but unknown channel ID...
        log("info", ("sending a Jingle invitation to the following participant: " .. origin.from));
end
module:hook("iq-result/bare/http://jitsi.org/protocol/colibri", handle_colibri, 2);

local function handle_jingle(event)
        -- process incoming Jingle stanzas from clients
end
module:hook("iq/bare/urn:xmpp:jingle:1", handle_jingle, 2);

--
-- end Jingle functions
--

