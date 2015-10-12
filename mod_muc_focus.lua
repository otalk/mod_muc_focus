-- Module: mod_muc_focus
-- Author: Peter Saint-Andre

--[[

OVERVIEW

This module enables Prosody to act as a "focus agent" for a 
multimedia conference. 

Basically, when this module is enabled, a Multi-User Chat room 
(XEP-0045) speaks Jingle (XEP-0166, XEP-0167, XEP-0176, etc.) to 
XMPP clients and speaks COLIBRI (XEP-0340) to media bridges (e.g.
media mixers and selective forwarding units like the Jitsi
Videobridge).

]]

--[[

CONFIGURATION

Add mod_muc_focus to the "modules_enabled" for the relevant
MUC domain...

    Component "conference.example.com" "muc"
              modules_enabled = { "mod_muc_focus" }
              focus_media_bridge = domain.name.of.bridge

]]

-- invoke various utilities
local st = require "util.stanza";
local jid  = require "util.jid";
local config = require "core.configmanager";
local os_time = os.time;
local difftime = os.difftime;
local setmetatable = setmetatable;
local jid_split = require "util.jid".split;
--local host = module.get_host();

-- get data from the configuration file
-- FIXME: at some point we might want to change focus_media_bridge to support multiple bridges, but for bootstrapping purposes we support only one
local focus_media_bridge = module:get_option_string("focus_media_bridge");

-- FIXME: better to get the content types from room configuration or Jingle sessions?
--local focus_content_types = module:get_option_array("focus_content_types");

local focus_datachannels = module:get_option_boolean("focus_feature_datachannel", true);
local usebundle = module:get_option_boolean("focus_feature_bundle", true);
local usertx = module:get_option_boolean("focus_feature_rtx", false);

-- a pubsub service and node to be subscribed for getting stats
local focus_pubsub_service = module:get_option_string("focus_pubsub_service");
local focus_pubsub_node = module:get_option_string("focus_pubsub_node", "videobridge");

-- minimum number of participants to start doing the call
local focus_min_participants = module:get_option_number("focus_min_participants", 2);

-- time to wait before terminate a conference after the number of particpants has dropped
-- below the minimum number. Off by default until this is fully tested
local focus_linger_time = module:get_option_number("focus_linger_time", 0);

-- time interval within which bridges are considered active
local focus_liveliness = module:get_option_number("focus_bridge_liveliness", 60);

local iterators = require "util.iterators"
local serialization = require "util.serialization"

-- define namespaces
local xmlns_colibri = "http://jitsi.org/protocol/colibri";
local xmlns_jingle = "urn:xmpp:jingle:1";
local xmlns_jingle_ice = "urn:xmpp:jingle:transports:ice-udp:1";
local xmlns_jingle_dtls = "urn:xmpp:jingle:apps:dtls:0";
local xmlns_jingle_rtp = "urn:xmpp:jingle:apps:rtp:1";
local xmlns_jingle_rtp_info = "urn:xmpp:jingle:apps:rtp:info:1";
local xmlns_jingle_rtp_headerext = "urn:xmpp:jingle:apps:rtp:rtp-hdrext:0";
local xmlns_jingle_rtp_feedback = "urn:xmpp:jingle:apps:rtp:rtcp-fb:0";
local xmlns_jingle_rtp_ssma = "urn:xmpp:jingle:apps:rtp:ssma:0";
local xmlns_jingle_grouping = "urn:xmpp:jingle:apps:grouping:0";
local xmlns_jingle_sctp = "urn:xmpp:jingle:transports:dtls-sctp:1";
local xmlns_mmuc = "http://andyet.net/xmlns/mmuc"; -- multimedia muc
local xmlns_pubsub = "http://jabber.org/protocol/pubsub";
local xmlns_pubsub_event = "http://jabber.org/protocol/pubsub#event";

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

-- all the msids
local participant2msids = {}

-- bridge associated with a room
local roomjid2bridge = {}

-- sessions inside a room
local sessions = {}

-- our custom *cough* iq callback mechanism
local callbacks = {}

-- bridges that have sent statistics recently
local bridge_stats = {}

-- for people joining while a conference is created
local pending_create = {}

local HEX = {"0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"};


function byte_to_hex(byte)
    local first = math.floor(byte / 16);
    local second = byte - (first * 16);
    return HEX[first + 1] .. HEX[second +1];
end

function hex_decode(hex)
    return string.gsub(hex, "([0-9A-Fa-f][0-9A-Fa-f])", function (hex)
        return string.char(tonumber(hex, 16));
    end);
end

function hex_encode(bytes)
    return string.gsub(bytes, "(.)", function (byte)
        return byte_to_hex(byte:byte(1));
    end);
end


-- base64 room jids to avoid unicode choking
local function encode_roomjid(jid)
    local node, host = jid_split(jid);
    return host .. "/" .. hex_encode(node);
end

local function decode_roomjid(jid)
    local node, host, res = jid_split(jid);
    local room_name = hex_decode(res);
    return room_name.. "@" .. host;
end


-- channel functions: create, update, expire 
-- create channels for multiple endpoints
local function create_channels(stanza, endpoints)
    stanza:tag("content", { name = "audio" })
    for i = 1,#endpoints do
        stanza:tag("channel", { initiator = "true", endpoint = endpoints[i], ["channel-bundle-id"] = (usebundle and endpoints[i] or nil) }):up()
    end
    stanza:up()
    
    stanza:tag("content", { name = "video" })
    for i = 1,#endpoints do
        stanza:tag("channel", { initiator = "true", endpoint = endpoints[i], ["channel-bundle-id"] = (usebundle and endpoints[i] or nil) }):up()
    end
    stanza:up()

    if focus_datachannels then
        -- note: datachannels will soon not be inside content anymore
        stanza:tag("content", { name = "data" })
        for i = 1,#endpoints do
            stanza:tag("sctpconnection", { initiator = "true", 
                 endpoint = endpoints[i], -- FIXME: I want the msid which i dont know here yet
                 port = 5000, -- it should not be port, this is the sctpmap
                 ["channel-bundle-id"] = (usebundle and endpoints[i] or nil)
            }):up()
        end
        stanza:up()
    end
    stanza:up():up()
end

-- updates channels for a single endpoint
local function update_channels(stanza, contents, channels, endpoint)
    for content in contents do
        module:log("debug", "    content name %s", content.attr.name)
        stanza:tag("content", { name = content.attr.name })
        if content.attr.name == "data" then
            stanza:tag("sctpconnection", { initiator = "true", id = channels[content.attr.name], endpoint = endpoint })
        else
            stanza:tag("channel", { initiator = "true", id = channels[content.attr.name], endpoint = endpoint })
        end
        local hasrtcpmux = nil
        for description in content:childtags("description", xmlns_jingle_rtp) do
            module:log("debug", "      description media %s", description.attr.media)
            for payload in description:childtags("payload-type", xmlns_jingle_rtp) do
                module:log("debug", "        payload name %s", payload.attr.name)
                stanza:add_child(payload)
            end
            hasrtcpmux = description:get_child("rtcp-mux")
            for group in description:childtags("ssrc-group", xmlns_jingle_rtp_ssma) do
                stanza:add_child(group)
            end
            for ext in description:childtags("rtp-hdrext", xmlns_jingle_rtp_headerext) do
                stanza:add_child(ext)
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
            -- colibri puts rtcp-mux inside transport (which is probably the right thing to do)
            if hasrtcpmux then
                transport:tag("rtcp-mux"):up()
            end
            stanza:add_child(transport)
        end
        stanza:up() -- channel
        stanza:up() -- content
    end
end

-- expires channels for a single endpoint
local function expire_channels(stanza, channels, endpoint)
    -- FIXME: endpoint should not be required
    for name, id in pairs(channels) do
        stanza:tag("content", { name = name })
        if name == "data" then
            stanza:tag("sctpconnection", { id = id, expire = 0, endpoint = endpoint }):up()
        else
            stanza:tag("channel", { id = id, expire = 0, endpoint = endpoint }):up()
        end
        stanza:up()
    end
end

-- picking a bridge, simplistic version
local function pick_bridge(roomjid)
    local choice = nil
    local minval = nil

    -- only consider live bridges from which we have seen data recently
    local live_bridges = {}
    for bridge, stats in pairs(bridge_stats) do
        local age = difftime(os_time(), stats["timestamp"])
        if age < focus_liveliness then
            live_bridges[bridge] = stats
        end
    end

    -- look at bridge stats, search for the bridge with the minimum
    -- up/download, participants, cpu
    -- FIXME: currently min bitrate
    for bridge, stats in pairs(live_bridges) do
        if not choice then
            choice = bridge
            minval = stats
        else
            if stats["bit_rate_upload"] + stats["bit_rate_download"] < minval["bit_rate_upload"] + minval["bit_rate_download"] then
                choice = bridge
                minval = stats
            end
        end
    end
    if minval then
        module:log("debug", "picking bridge %s", choice)
        module:log("debug", "metrics bitrate=%d",
                   minval["bit_rate_upload"] + minval["bit_rate_download"])
        module:log("debug", "bridge stat age %d", os_time() - minval["timestamp"])
    else
        module:log("debug", "picking default bridge %s", focus_media_bridge)
    end
    -- FIXME: choosing a bridge should move it down in the preference
    return choice or focus_media_bridge
end

-- remove a conference which is no longer needed
local function linger_timeout(room)
    local count = iterators.count(pairs(sessions[room.jid]))
    -- count_capable_clients(room)?
    module:log("debug", "linger timeout %d", count)
    if count < focus_min_participants then
        destroy_conference(room)
    end
end

-- clean up any local state we have for this room
local function cleanup_room(room)
    module:log("debug", "cleaning up %s", room.jid);
    jid2room[room.jid] = nil

    jid2channels[room.jid] = nil;
    sessions[room.jid] = nil
    -- possibly also, just to make sure they are cleaned up
    roomjid2bridge[room.jid] = nil
    roomjid2conference[room.jid] = nil
    participant2sources[room.jid] = nil
    participant2msids[room.jid] = nil
    pending_create[room.jid] = nil
end

-- determines whether a participant is capable
local function is_capable(occupant)
    local stanza = occupant:get_presence()
    if not stanza then return false; end
    local caps = stanza:get_child("conf", xmlns_mmuc)
    return caps and (caps.attr.bridged == "1" or caps.attr.bridged == "true")
end

-- counts number of capable occupants in a room
local function count_capable_clients(room)
    local count = 0
    -- FIXME: probably optimize this
    for nick, occupant in room:each_occupant() do
        if is_capable(occupant) then
            count = count + 1
        end
    end
    return count
end

-- terminate the jingle sessions, 
-- and expire any channels for a conference,
local function destroy_conference(room)
    -- check that the conditions why we called this still apply
    local count = count_capable_clients(room)
    if count >= focus_min_participants then return; end

    -- tell everyone to go back to p2p mode
    -- only on transition min_participants -> min_participants - 1?
    local mode = st.message({ from = room.jid, type = "groupchat" })
    mode:tag("status", { xmlns = xmlns_mmuc, mode = "p2p" })
    room:broadcast_message(mode);

    -- terminate the jingle sessions
    local sid = roomjid2conference[room.jid] -- uses the id from the bridge
    if not sid then return; end
    local terminate = st.iq({ from = room.jid, type = "set" })
        :tag("jingle", { xmlns = xmlns_jingle, action = "session-terminate", initiator = room.jid, sid = sid })
          :tag("reason")
            :tag("success"):up()
          :up()
        :up()
    if participant2sources[room.jid] then -- FIXME: will not work for listen-only participants
        -- the intent is to send a session-terminate to anyone we have a session with
        for occupant_jid in iterators.keys(participant2sources[room.jid]) do
            if sessions[room.jid] and sessions[room.jid][occupant_jid] then
                local occupant = room:get_occupant_by_nick(occupant_jid)
                if occupant then room:route_to_occupant(occupant, terminate) end
            end
        end
    end
    sessions[room.jid] = nil

    local confid = roomjid2conference[room.jid]

    -- expire any channels
    local count = 0
    local bridge = roomjid2bridge[room.jid]
    local confupdate = st.iq({ from = encode_roomjid(room.jid), to = bridge, type = "set" })
        :tag("conference", { xmlns = xmlns_colibri, id = confid })
    if jid2channels[room.jid] then
        for nick, occupant in room:each_occupant() do
            channels = jid2channels[room.jid][nick]
            if (channels) then
                expire_channels(confupdate, channels, nick)
                jid2channels[room.jid][nick] = nil
                count = count + 1
            end
        end
        if count > 0 then
            module:send(confupdate);
        end
    end

    -- do all the cleanup stuff
    roomjid2bridge[room.jid] = nil
    roomjid2conference[room.jid] = nil
    participant2sources[room.jid] = nil
    participant2msids[room.jid] = nil

    -- final cleanup, just in case
    cleanup_room(room)
end

-- before someone joins we tell everyone that we're going to switch to 
-- relayed mode soon
module:hook("muc-occupant-pre-join", function(event)
        local room, stanza = event.room, event.stanza;
        --if jid2room[room.jid] then return; end -- already in a conf
        -- check if we are going to start a conference soon
        local count = count_capable_clients(room)
        local mode = st.message({ from = room.jid, type = "groupchat" })
        local caps = stanza:get_child("conf", xmlns_mmuc)
        local new_capable = caps and (caps.attr.bridged == "1" or caps.attr.bridged == "true")
        if new_capable and count >= focus_min_participants - 1 then
            mode:tag("status", { xmlns = xmlns_mmuc, mode = "relay" })
            room:broadcast_message(mode);
        else
            mode:tag("status", { xmlns = xmlns_mmuc, mode = "p2p" })
        end

        -- also send to joining participant
        mode.attr.to = stanza.attr.from
        module:send(mode);
end, -100)

-- prevent multiple sessions from the same user because that is going
-- to be very complicated
module:hook("muc-occupant-pre-join", function(event)
    module:log("debug", "pre-join %s is first %s is last %s", tostring(event.room), tostring(event.is_first_session), tostring(event.is_last_session))
    local room, stanza = event.room, event.stanza;
    if not event.is_first_session then
        local from, to = stanza.attr.from, stanza.attr.to;
        module:log("debug", "%s couldn't join due to duplicate session: %s", from, to);
        local reply = st.error_reply(stanza, "modify", "resource-constraint"):up();
        event.origin.send(reply:tag("x", {xmlns = "http://jabber.org/protocol/muc"}));
        return true;
    end
end, 101)

-- when someone joins the room, we request a channel for them on the bridge
-- (eventually we will also send a Jingle invitation - see handle_colibri...)
-- possibly we need to hook muc-occupant-session-new instead 
-- for cases where a participant joins with twice
module:hook("muc-occupant-joined", function (event)
        local room, nick, occupant = event.room, event.nick, event.occupant
        local stanza = occupant:get_presence()
        --local count = iterators.count(sessions[room.jid] or {})
        local count = count_capable_clients(room)
		module:log("debug", "handle_join %s %s %s", 
                   tostring(room), tostring(nick), tostring(stanza))

        -- check client mmuc capabilities
        if not is_capable(occupant) then
            return
        end

        -- if there are now enough occupants, create a conference
        -- look at room._occupants size?
        module:log("debug", "handle join #occupants %d out of %d", count, iterators.count(pairs(room._occupants)))
        if count < focus_min_participants then return; end

        local bridge = roomjid2bridge[room.jid]
        if not bridge then -- pick a bridge 
            roomjid2bridge[room.jid] = pick_bridge(room.jid)
            bridge = roomjid2bridge[room.jid] 
        end

        module:log("debug", "room jid %s bridge %s", room.jid, bridge)

        jid2room[room.jid] = room

        if roomjid2conference[room.jid] == -1 then
            -- keep them in a list until we get a conference id to create additional channels
            if not pending_create[room.jid] then
                pending_create[room.jid] = {}
            end
            pending_create[room.jid][#pending_create+1] = nick
            return
        end

        local confcreate = st.iq({ from = encode_roomjid(room.jid), to = bridge, type = "set" })
        -- for now, just create a conference for each participant and then ... initiate a jingle session with them
        if roomjid2conference[room.jid] == nil then -- create a conference
            module:log("debug", "creating conference for %s", room.jid)
            confcreate:tag("conference", { xmlns = xmlns_colibri })
            roomjid2conference[room.jid] = -1 -- pending
            --confcreate:tag("recording", { state = "true", token = "recordersecret" }):up() -- recording
        else -- update existing conference
            module:log("debug", "existing conf id %s", roomjid2conference[room.jid])
            confcreate:tag("conference", { xmlns = xmlns_colibri, id = roomjid2conference[room.jid] })
        end

        local pending = {}
        -- anyone not currently in a session but capable of
        -- this includes people who sent session-terminate
        if not sessions[room.jid] then sessions[room.jid] = {}; end
        for nick_, occupant_ in room:each_occupant() do 
            if is_capable(occupant_) and not sessions[room.jid][nick_] then
                pending[#pending+1] = nick_
            end
        end
        --module:log("debug", "pending %s", serialization.serialize(pending))

        create_channels(confcreate, pending)
        callbacks[confcreate.attr.id] = pending
        module:log("debug", "send_colibri %s", tostring(confcreate))
        module:send(confcreate);
end, 2)

local function remove_session(event) 
        -- same here, remove conference when there are now
        -- less than the minimum required number of participants in the room
        -- optimization: keep the conference a little longer
        -- to allow for fast rejoins
        local room, nick = event.room, event.nick

        if sessions[room.jid] then
            sessions[room.jid][nick] = nil 
        end
        local count = iterators.count(pairs(sessions[room.jid] or {}))

        local bridge = roomjid2bridge[room.jid]

        if participant2sources[room.jid] and participant2sources[room.jid][nick] then
            local sources = participant2sources[room.jid][nick]
            if sources then
                local removed = 0
                -- we need to send source-remove for these
                module:log("debug", "source-remove")
                local sid = roomjid2conference[room.jid] -- uses the id from the bridge
                local sourceremove = st.iq({ from = room.jid, type = "set" })
                    :tag("jingle", { xmlns = xmlns_jingle, action = "source-remove", initiator = room.jid, sid = sid })
                for name, sourcelist in pairs(sources) do
                    sourceremove:tag("content", { creator = "initiator", name = name, senders = "both" })
                        :tag("description", { xmlns = xmlns_jingle_rtp, media = name })
                        for i, source in ipairs(sourcelist) do
                            sourceremove:add_child(source)
                            removed = removed + 1
                        end
                        sourceremove:up() -- description
                    :up() -- content
                end

                participant2sources[room.jid][nick] = nil
                participant2msids[room.jid][nick] = nil

                if count > 1 and removed > 0 then -- will terminate session otherwise
                    for occupant_jid in iterators.keys(participant2sources[room.jid]) do
                        if occupant_jid ~= jid then -- cant happen i think
                            module:log("debug", "send source-remove to %s", tostring(occupant_jid))
                            local occupant = room:get_occupant_by_nick(occupant_jid)
                            room:route_to_occupant(occupant, sourceremove)
                        end
                    end
                end
            end
        end

        -- we close those channels by setting their expire to 0
        local confid = roomjid2conference[room.jid]
        if jid2channels[room.jid] then
            local channels = jid2channels[room.jid][nick] 
            if channels then
                local confupdate = st.iq({ from = encode_roomjid(room.jid), to = bridge, type = "set" })
                    :tag("conference", { xmlns = xmlns_colibri, id = confid })
                expire_channels(confupdate, channels, nick)
                jid2channels[room.jid][nick] = nil
                module:send(confupdate);
            else
                --module:log("debug", "handle_leave: no channels found")
            end
            if #jid2channels[room.jid] == 0 then
                jid2channels[room.jid] = nil
            end
        end

        if count < focus_min_participants then -- not enough participants any longer
            -- ѕtart downgrade process
            if focus_linger_time > 0 then
                module:add_timer(focus_linger_time, function () 
                    destroy_conference(room)
                end);
            else -- immediate destroy, default for now
                destroy_conference(room)
            end
        end

        -- final cleanup
        if count == 0 then
            cleanup_room(room)
        end
end
module:hook("muc-occupant-left", remove_session, 2)

module:hook("muc-occupant-pre-change", function (event)
    local room, origin, stanza = event.room, event.origin, event.stanza
    -- occupant, actor, reason
    if stanza.attr.type == "unavailable" then return; end
    local occupant = room:get_occupant_by_real_jid(stanza.attr.from)
    if not occupant then return; end
    local nick = occupant.nick;
    if not participant2msids[room.jid] then return; end
    local msids = participant2msids[room.jid][nick]
	if not msids then return; end

    -- filter any mediastream mmuc tags
    stanza:maptags(function (tag)
        if not (tag.name == "mediastream" and tag.attr.xmlns == xmlns_mmuc) then
            return tag
        end
    end);

    -- stamp them onto it
    for msid, info in pairs(msids) do
        stanza:tag("mediastream", { xmlns = xmlns_mmuc, msid = msid, audio = info.audio, video = info.video }):up()
    end
end, 2);


-- the static parts of the audio description we send
local function add_audio_description(stanza)
    stanza:tag("payload-type", { id = "111", name = "opus", clockrate = "48000", channels = "2" })
            :tag("parameter", { name = "minptime", value = "10" }):up()
        :up()
        :tag("payload-type", { id = "103", name = "ISAC", clockrate = "16000" }):up()
        :tag("payload-type", { id = "104", name = "ISAC", clockrate = "32000" }):up()
        :tag("payload-type", { id = "9", name = "G722", clockrate = "8000" }):up()
        :tag("payload-type", { id = "0", name = "PCMU", clockrate = "8000" }):up()
        :tag("payload-type", { id = "8", name = "PCMA", clockrate = "8000" }):up()

        :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "1", uri = "urn:ietf:params:rtp-hdrext:ssrc-audio-level" }):up()
        :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "3", uri = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time" }):up()
    if usebundle then
        stanza:tag("rtcp-mux"):up()
    end
end

-- the static parts of the audio description we send
local function add_video_description(stanza)
    stanza:tag("payload-type", { id = "100", name = "VP8", clockrate = "90000" })
            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'ccm', subtype = 'fir' }):up()
            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'nack' }):up()
            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'nack', subtype = 'pli' }):up()
            :tag("rtcp-fb", { xmlns = xmlns_jingle_rtp_feedback, type = 'goog-remb' }):up()
        :up()
        :tag("payload-type", { id = "116", name = "red", clockrate = "90000" }):up()
        :tag("payload-type", { id = "117", name = "ulpfec", clockrate = "90000" }):up()
    if usertx then
        stanza:tag("payload-type", { id = "96", name = "rtx", clockrate = "90000" })
            :tag("parameter", { name = "apt", value = "100" }):up()
        :up()
    end

    stanza:tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "2", uri = "urn:ietf:params:rtp-hdrext:toffset" }):up()
        :tag("rtp-hdrext", { xmlns= xmlns_jingle_rtp_headerext, id = "3", uri = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time" }):up()

    if usebundle then
        stanza:tag("rtcp-mux"):up()
    end
end

-- things we do when a room receives a COLIBRI stanza from the bridge 
module:hook("iq/host", function (event)
        local stanza = event.stanza

        if stanza.attr.type == "error" then
            module:log("debug", "handle_colibri error %s", tostring(stanza))
            return true
        end

        local conf = stanza:get_child("conference", xmlns_colibri)
        if conf == nil then return; end


        if stanza.attr.type ~= "result" then return true; end
        module:log("debug", "%s %s %s", stanza.attr.from, stanza.attr.to, stanza.attr.type)

        local confid = conf.attr.id
        module:log("debug", "conf id %s", confid)

        local roomjid = decode_roomjid(stanza.attr.to)

        -- assert the sender is the bridge associated with this room
        if stanza.attr.from ~= roomjid2bridge[roomjid] then
            if roomjid2bridge[roomjid] ~= nil then
                module:log("debug", "handle_colibri fake sender %s expected %s", stanza.attr.from, tostring(roomjid2bridge[roomjid]))
            end
            return
        end

        if callbacks[stanza.attr.id] == nil then return true; end
        module:log("debug", "handle_colibri %s", tostring(event.stanza))

        roomjid2conference[roomjid] = confid
        local room = jid2room[roomjid]
        if not room then
            module:log("debug", "handle_colibri room %s already destroyed", roomjid)
            return true
        end

        --local occupant_jid = callbacks[stanza.attr.id]
        local occupants = {}
        for idx, nick in pairs(callbacks[stanza.attr.id]) do
            -- FIXME: actually we want to get a particular session of an occupant, not all of them
            local occupant = room:get_occupant_by_nick(nick)
            module:log("debug", "occupant is %s", tostring(occupant))
            if occupant then -- can be null sometimes apparently
                occupants[#occupants+1] = occupant
            end
        end
        callbacks[stanza.attr.id] = nil


        if sessions[room.jid] == nil then
            sessions[room.jid] = {}
        end
        if participant2msids[room.jid] == nil then
            participant2msids[room.jid] = {}
        end
        if not jid2channels[room.jid] then
            jid2channels[room.jid] = {}
        end
        if participant2sources[room.jid] == nil then
            participant2sources[room.jid] = {}
        end

        for channelnumber = 1, #occupants do
            local sid = roomjid2conference[room.jid] -- uses the id from the bridge
            local initiate = st.iq({ from = roomjid, type = "set" })
                :tag("jingle", { xmlns = xmlns_jingle, action = "session-initiate", initiator = roomjid, sid = sid })

            local occupant = occupants[channelnumber]
            local occupant_jid = occupant.nick
            jid2channels[room.jid][occupant_jid] = {}

            local bundlegroup = {} 

            for content in conf:childtags("content", xmlns_colibri) do
                module:log("debug", "  content name %s", content.attr.name)
                local channel = nil
                initiate:tag("content", { creator = "initiator", name = content.attr.name, senders = "both" })
                if content.attr.name == "audio" or content.attr.name == "video" then
                    channel = iterators.to_array(content:childtags("channel", xmlns_colibri))[channelnumber]
                    jid2channels[room.jid][occupant_jid][content.attr.name] = channel.attr.id

                    initiate:tag("description", { xmlns = xmlns_jingle_rtp, media = content.attr.name })
                    if content.attr.name == "audio" then
                        add_audio_description(initiate)
                    elseif content.attr.name == "video" then
                        add_video_description(initiate)
                    end
                    -- copy ssrcs
                    for jid, sources in pairs(participant2sources[room.jid]) do
                        if sources[content.attr.name] then
                            for i, source in ipairs(sources[content.attr.name]) do
                                initiate:add_child(source)
                            end
                        end
                    end
                    initiate:up()
                elseif content.attr.name == "data" then
                    -- data channels are handled slightly different
                    channel = iterators.to_array(content:childtags("sctpconnection", xmlns_colibri))[channelnumber]
                    jid2channels[room.jid][occupant_jid][content.attr.name] = channel.attr.id
                    initiate:tag("description", { xmlns = "http://talky.io/ns/datachannel" })
                        -- no description yet. describe the channels?
                    :up()
                end

                if channel then -- add transport
                    local transports
                    if channel.attr["channel-bundle-id"] then
                        bundlegroup[#bundlegroup+1] = content.attr.name
                        for bundle in conf:childtags("channel-bundle") do
                            if bundle.attr.id == channel.attr["channel-bundle-id"] then
                                transports = bundle:childtags("transport", xmlns_jingle_ice)
                                break
                            end
                        end
                    else
                        transports = channel:childtags("transport", xmlns_jingle_ice)
                    end
                    -- FIXME: check that a transport was found?
                    for transport in transports do
                        for fingerprint in transport:childtags("fingerprint", xmlns_jingle_dtls) do
                            fingerprint.attr.setup = "actpass"
                        end
                        -- add a XEP-0343 sctpmap element
                        if content.attr.name == "data" then
                            transport = st.clone(transport) -- need to clone before modifying
                            transport:tag("sctpmap", { xmlns = xmlns_jingle_sctp, number = channel.attr.port, protocol = "webrtc-datachannel", streams = 1024 }):up()
                        end
                        initiate:add_child(transport)
                    end
                end
                initiate:up() -- content
            end
            if #bundlegroup > 0 then
                initiate:tag("group", { xmlns = xmlns_jingle_grouping, semantics = "BUNDLE" })
                module:log("debug", "BUNDLE %d", #bundlegroup)
                for i, name in ipairs(bundlegroup) do
                    module:log("debug", "BUNDLE %s %s", tostring(i), name)
                    initiate:tag("content", { name = name }):up()
                end
                initiate:up()
            end
            initiate:up() -- jingle
            initiate:up()

            -- preoccupy here
            participant2sources[room.jid][occupant_jid] = {}
            sessions[room.jid][occupant_jid] = true

            room:route_to_occupant(occupant, initiate)
            --module:log("debug", "send_jingle %s", tostring(initiate))
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

        -- if there are pending participants that joined while the conference was created
        -- create channels for them here
        if pending_create[room.jid] then
            local update = st.iq({ from = room.jid, to = stanza.attr.from, type = "set" })
            update:tag("conference", { xmlns = xmlns_colibri, id = roomjid2conference[room.jid] })
            create_channels(update, pending_create[room.jid])
            callbacks[update.attr.id] = pending_create
            module:log("debug", "send_colibri %s late", tostring(update))
            module:send(update);
            pending_create[room.jid] = nil
        end

        return true
end, 2);

-- process incoming Jingle stanzas from clients
module:hook("iq/bare", function (event) 
        local session, stanza = event.origin, event.stanza;
        local jingle = stanza:get_child("jingle", xmlns_jingle)
        if jingle == nil then return; end

        -- only handle things addressed to the room, not participants
        local node, host, resource = jid.split(stanza.attr.to)
        if resource ~= nil then return; end

        if host ~= module:get_host() then return; end -- TODO is that necessary?

        --module:log("debug", "handle_jingle %s %s", tostring(session), tostring(stanza))
        --module:log("info", ("sending a Jingle invitation to the following participant: " .. origin.from));

        -- FIXME: this is not the in-muc from so we need to either change the handler
        -- or look up the participant based on the real jid
        module:log("debug", "handle_jingle %s from %s", jingle.attr.action, stanza.attr.from)
        local roomjid = stanza.attr.to
        local action = jingle.attr.action
        -- FIXME: ignore jingle not addressed to this host
        -- and stanzas not addressed to the rooms bare jid
        local room = jid2room[roomjid]
        if not room then
            if action == "session-terminate" then
                module:log("debug", "session-terminate while room is dead already, ignoring")
                return
            end
        end
        local confid = roomjid2conference[room.jid]
        local sender = room:get_occupant_by_real_jid(stanza.attr.from)
        local bridge = roomjid2bridge[room.jid]

        -- iterate again to look at the SSMA source elements
        -- FIXME: only for session-accept and source-add / source-remove?
        local sources = {}

        if action == "session-terminate" then
            remove_session({room = room, nick = sender })
            return
        end

        if participant2sources[room.jid] == nil then
            participant2sources[room.jid] = {}
        end
        if participant2msids[room.jid] == nil then
            participant2msids[room.jid] = {}
        end

        if action == "session-info" then
            local msids = participant2msids[room.jid][sender.nick];

            for muted in jingle:childtags("mute", xmlns_jingle_rtp_info) do
                local mediastream_specified = false;
                for mediastream in jingle:childtags("mediastream", xmlns_mmuc) do
                    mediastream_specified = true;

                    local msid = mediastream.attr.msid;

                    if msids and msids[msid] then
                        if muted.attr.name then
                            if msids[msid][muted.attr.name] then
                                msids[msid][muted.attr.name] = "muted";
                            end
                        else
                            if msids[msid].audio then
                                msids[msid].audio = "muted";
                            end
                            if msids[msid].video then
                                msids[msid].video = "muted";
                            end
                        end
                    end
                end
                if not mediastream_specified then
                    for msid, info in pairs(msids) do
                        if muted.attr.name then
                            if msids[msid][muted.attr.name] then
                                msids[msid][muted.attr.name] = "muted";
                            end
                        else
                            if msids[msid].audio then
                                msids[msid].audio = "muted";
                            end
                            if msids[msid].video then
                                msids[msid].video = "muted";
                            end
                        end
                    end
                end
            end

            for unmuted in jingle:childtags("unmute", xmlns_jingle_rtp_info) do
                local mediastream_specified = false;
                for mediastream in jingle:childtags("mediastream", xmlns_mmuc) do
                    mediastream_specified = true;

                    local msid = mediastream.attr.msid;

                    if msids and msids[msid] then
                        if unmuted.attr.name then
                            if msids[msid][unmuted.attr.name] then
                                msids[msid][unmuted.attr.name] = "true";
                            end
                        else
                            if msids[msid].audio then
                                msids[msid].audio = "true";
                            end
                            if msids[msid].video then
                                msids[msid].video = "true";
                            end
                        end
                    end
                end
                if not mediastream_specified then
                    for msid, info in pairs(msids) do
                        if unmuted.attr.name then
                            if msids[msid][unmuted.attr.name] then
                                msids[msid][unmuted.attr.name] = "true";
                            end
                        else
                            if msids[msid].audio then
                                msids[msid].audio = "true";
                            end
                            if msids[msid].video then
                                msids[msid].video = "true";
                            end
                        end
                    end
                end
            end

            session.send(st.reply(stanza))

            local pr = sender:get_presence()
            -- filter any existing mediastream mmuc tags
            pr:maptags(function (tag)
                if not (tag.name == "mediastream" and tag.attr.xmlns == xmlns_mmuc) then
                    return tag
                end
            end);
            for msid, info in pairs(msids) do
                pr:tag("mediastream", { xmlns = xmlns_mmuc, msid = msid, audio = info.audio, video = info.video }):up()
            end
            sender:set_session(stanza.attr.from, pr)
			local x = st.stanza("x", {xmlns = "http://jabber.org/protocol/muc#user";});
            room:publicise_occupant_status(sender, x);

            return true;
        end

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
                            if not msids[msid] then
                                msids[msid] = {}
                            end
                            msids[msid][description.attr.media] = "true"
                            module:log("debug", "msid %s content %s", msid, content.attr.name)
                        end
                    end

                    -- and also to subsequent offers (full elements)
                    sourcelist[#sourcelist+1] = source
                    module:log("debug", "source %s content %s", source.attr.ssrc, content.attr.name)
                end
                for group in description:childtags("ssrc-group", xmlns_jingle_rtp_ssma) do
                    module:log("debug", "group semantics %s", group.attr.semantics)
                    if group.attr.semantics == "FID" then
                        sourcelist[#sourcelist+1] = group 
                    end
                end
                sources[content.attr.name] = sourcelist 
            end
        end

        module:log("debug", "confid %s", tostring(confid))

        if action == "session-accept" or action == "source-add" or action == "source-remove" then
            -- update participant presence with a <media xmlns=...><source type=audio ssrc=... direction=sendrecv/>...</media>
            -- or the new plan to tell the MSID
            local pr = sender:get_presence()
            for msid, info in pairs(msids) do
                pr:tag("mediastream", { xmlns = xmlns_mmuc, msid = msid, audio = info.audio, video = info.video }):up()
            end
            --pr:tag("media", {xmlns = "http://.../ns/mjs"})
            --for name, source in pairs(sources) do
            --    pr:tag("source", { type = name, ssrc = source.attr.ssrc, direction = "sendrecv" }):up();
            --end
            sender:set_session(stanza.attr.from, pr)
			local x = st.stanza("x", {xmlns = "http://jabber.org/protocol/muc#user";});
            room:publicise_occupant_status(sender, x);

            participant2msids[room.jid][sender.nick] = msids

            -- FIXME handle updates and removals
            participant2sources[room.jid][sender.nick] = sources
            local sid = roomjid2conference[room.jid] -- uses the id from the bridge
            local sendaction = "source-add"
            if action == "source-remove" then
                sendaction = "source-remove"
            end
            local sourceadd = st.iq({ from = room.jid, type = "set" })
                :tag("jingle", { xmlns = xmlns_jingle, action = sendaction, initiator = room.jid, sid = sid })
            for name, sourcelist in pairs(sources) do
                sourceadd:tag("content", { creator = "initiator", name = name, senders = "both" })
                    :tag("description", { xmlns = xmlns_jingle_rtp, media = name })
                    for i, source in ipairs(sourcelist) do
                        sourceadd:add_child(source)
                    end

                    sourceadd:up() -- description
                :up() -- content
            end

            -- sent to everyone but the sender
            if sessions[room.jid] then
                for occupant_jid in iterators.keys(participant2sources[room.jid]) do
                    if occupant_jid ~= sender.nick and sessions[room.jid][occupant_jid] then
                        module:log("debug", "send %s to %s", sendaction, tostring(occupant_jid))
                        local occupant = room:get_occupant_by_nick(occupant_jid)
                        if (occupant) then -- FIXME: when does this not happen?
                            room:route_to_occupant(occupant, sourceadd)
                            --module:log("debug", "%s %s", sendaction, tostring(sourceadd))
                        else
                            module:log("debug", "not found %s", sendaction)
                        end
                    end
                end
            end
        end

        -- update the channels
        if jid2channels[room.jid] and jid2channels[room.jid][sender.nick] then
            local channels = jid2channels[room.jid][sender.nick]
            local confupdate = st.iq({ from = encode_roomjid(room.jid), to = bridge, type = "set" })
                :tag("conference", { xmlns = xmlns_colibri, id = confid })
            update_channels(confupdate, jingle:childtags("content", xmlns_jingle), channels, sender.nick)

            module:log("debug", "confupdate is %s", tostring(confupdate))
            module:send(confupdate);
        end

        session.send(st.reply(stanza))
        return true;
end, 2);
--
-- end Jingle functions
--

-- hook disco#info
module:hook("muc-disco#info", function(event)
    event.reply:tag("feature", {var = xmlns_jingle}):up();
    event.reply:tag("feature", {var = xmlns_jingle_ice}):up();
    event.reply:tag("feature", {var = xmlns_jingle_rtp}):up();
    event.reply:tag("feature", {var = xmlns_jingle_dtls}):up();

    event.reply:tag("feature", {var = xmlns_mmuc}):up();
    -- colibri doesn't matter to the client
    --event.reply:tag("feature", {var = xmlns_colibri}):up();
end);


-- pubsub stats collector -- see
-- https://github.com/jitsi/jitsi-videobridge/blob/master/doc/using_statistics.md
module:hook("message/host", function (event)
    -- process incoming pubsub stanzas from the pubsub node
    local origin, stanza = event.origin, event.stanza;
    if stanza.attr.from ~= focus_pubsub_service then return; end
    if stanza.attr.type ~= "headline" then return; end

    local ev = stanza:get_child("event", xmlns_pubsub_event)
    if ev == nil then return; end

    -- FIXME local items = ev:get_child("items", xmlns_pubsub_event)
    for items in ev:childtags("items") do
        if items.attr.node ~= focus_pubsub_node then return; end
        for item in items:childtags("item") do
            for stats in item:childtags("stats", xmlns_colibri) do
                local statstable = {}
                for stat in stats:childtags("stat", xmlns_colibri) do
                    statstable[stat.attr.name] = stat.attr.value
                end
                --module:log("debug", "%s stats: %s", item.attr.publisher, serialization.serialize(statstable))

                module:fire_event("jvb-stats", { stats = statstable, bridge = item.attr.publisher })
            end
        end
    end
    return true
end, 3)

-- process bridge statistics and determine most available bridge
module:hook("jvb-stats", function (event)
    local stats = {}
    for key, value in pairs(event.stats) do
        if key ~= "current_time" then
            stats[key] = tonumber(value)
        end
    end
    stats["timestamp"] = os_time()
    bridge_stats[event.bridge] = stats
    --module:log("debug", "all stats:\n%s", serialization.serialize(bridge_stats))
end, 3)

-- subscribe to the pubsub node
if focus_pubsub_service then
    -- wait until all hosts have been configured
    module:add_timer(5, function () 
        local sub = st.iq({ from = module:get_host(), to = focus_pubsub_service, type = "set" })
        sub:tag("pubsub", {xmlns = xmlns_pubsub})
          :tag("subscribe", {node = focus_pubsub_node, jid = module:get_host()}):up()
        :up()
        module:send(sub);
    end)
end

log("info", "mod_muc_focus loaded");
