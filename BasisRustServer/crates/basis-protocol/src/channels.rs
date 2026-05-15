pub const TOTAL_CHANNELS: u8 = 64;

pub const AUTH_IDENTITY: u8 = 0;
pub const META_DATA: u8 = 1;
pub const DISCONNECTION: u8 = 2;
pub const VOICE: u8 = 3;
pub const SHOUT_VOICE: u8 = 4;
pub const AUDIO_RECIPIENTS: u8 = 5;
pub const PLAYER_AVATAR_VERY_LOW: u8 = 6;
pub const PLAYER_AVATAR_VERY_LOW_ADDITIONAL: u8 = 7;
pub const PLAYER_AVATAR_LOW: u8 = 8;
pub const PLAYER_AVATAR_LOW_ADDITIONAL: u8 = 9;
pub const PLAYER_AVATAR_MEDIUM: u8 = 10;
pub const PLAYER_AVATAR_MEDIUM_ADDITIONAL: u8 = 11;
pub const PLAYER_AVATAR_HIGH: u8 = 12;
pub const PLAYER_AVATAR_HIGH_ADDITIONAL: u8 = 13;
pub const AVATAR_CHANGE_MESSAGE: u8 = 14;
pub const AVATAR: u8 = 15;
pub const CREATE_REMOTE_PLAYER: u8 = 16;
pub const CREATE_REMOTE_PLAYERS_FOR_NEW_PEER: u8 = 17;
pub const CHAT: u8 = 18;
pub const GET_CURRENT_OWNER_REQUEST: u8 = 19;
pub const CHANGE_CURRENT_OWNER_REQUEST: u8 = 20;
pub const REMOVE_CURRENT_OWNER_REQUEST: u8 = 21;
pub const NET_ID_ASSIGN: u8 = 22;
pub const NET_ID_ASSIGNS: u8 = 23;
pub const SCENE: u8 = 24;
pub const LOAD_RESOURCE: u8 = 25;
pub const UNLOAD_RESOURCE: u8 = 26;
pub const PRELOAD_READY: u8 = 27;
pub const SPAWN_PRELOADED: u8 = 28;
pub const CONTENT_SHARE: u8 = 29;
pub const CONTENT_SHARE_CLEANUP: u8 = 30;
pub const SERVER_BOUND: u8 = 31;
pub const STORE_DATABASE: u8 = 32;
pub const REQUEST_STORE_DATABASE: u8 = 33;
pub const ADMIN: u8 = 34;
pub const SERVER_STATISTICS: u8 = 35;
pub const CAMERA_PIP_STATE: u8 = 36;
pub const CAMERA_PIP_POSITION: u8 = 37;
pub const EVENTS: u8 = 38;
pub const AUDIO_RECIPIENTS_LARGE: u8 = 39;
pub const VOICE_LARGE: u8 = 40;
pub const PLAYER_AVATAR_VERY_LOW_LARGE: u8 = 41;
pub const PLAYER_AVATAR_VERY_LOW_ADDITIONAL_LARGE: u8 = 42;
pub const PLAYER_AVATAR_LOW_LARGE: u8 = 43;
pub const PLAYER_AVATAR_LOW_ADDITIONAL_LARGE: u8 = 44;
pub const PLAYER_AVATAR_MEDIUM_LARGE: u8 = 45;
pub const PLAYER_AVATAR_MEDIUM_ADDITIONAL_LARGE: u8 = 46;
pub const PLAYER_AVATAR_HIGH_LARGE: u8 = 47;
pub const PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE: u8 = 48;
pub const AUDIO_RECIPIENTS_INVERTED: u8 = 49;
pub const AUDIO_RECIPIENTS_INVERTED_LARGE: u8 = 50;
pub const AUDIO_RECIPIENTS_BITFIELD: u8 = 51;
pub const COMPRESSED_AVATAR_BUNDLE: u8 = 52;
pub const SERVER_LIBRARY: u8 = 53;
pub const P2P: u8 = 54;

pub const EVENT_TYPE_CAMERA_SHUTTER_SOUND: u8 = 0;
pub const EVENT_TYPE_CAMERA_COUNTDOWN: u8 = 1;
pub const EVENT_TYPE_PLAYER_TEMP_BLOCK: u8 = 2;
pub const EVENT_TYPE_AVATAR_RATE_CHANGE: u8 = 3;

pub const P2P_SUB_REQUEST: u8 = 0;
pub const P2P_SUB_ACCEPT: u8 = 1;
pub const P2P_SUB_DECLINE: u8 = 2;
pub const P2P_SUB_CANCEL: u8 = 3;
pub const P2P_SUB_LINK_LOST: u8 = 4;
pub const P2P_SUB_SERVER_ARMED: u8 = 5;
pub const P2P_SUB_LINK_UP: u8 = 6;

pub const PLAYER_AVATAR_QUALITY_CHANNELS: [u8; 16] = [
    PLAYER_AVATAR_VERY_LOW,
    PLAYER_AVATAR_VERY_LOW_ADDITIONAL,
    PLAYER_AVATAR_LOW,
    PLAYER_AVATAR_LOW_ADDITIONAL,
    PLAYER_AVATAR_MEDIUM,
    PLAYER_AVATAR_MEDIUM_ADDITIONAL,
    PLAYER_AVATAR_HIGH,
    PLAYER_AVATAR_HIGH_ADDITIONAL,
    PLAYER_AVATAR_VERY_LOW_LARGE,
    PLAYER_AVATAR_VERY_LOW_ADDITIONAL_LARGE,
    PLAYER_AVATAR_LOW_LARGE,
    PLAYER_AVATAR_LOW_ADDITIONAL_LARGE,
    PLAYER_AVATAR_MEDIUM_LARGE,
    PLAYER_AVATAR_MEDIUM_ADDITIONAL_LARGE,
    PLAYER_AVATAR_HIGH_LARGE,
    PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE,
];

pub fn player_avatar_channel_for_quality(quality_index: u8, has_additional_data: bool) -> u8 {
    PLAYER_AVATAR_VERY_LOW + quality_index * 2 + u8::from(has_additional_data)
}

pub fn player_avatar_large_channel_for_quality(quality_index: u8, has_additional_data: bool) -> u8 {
    PLAYER_AVATAR_VERY_LOW_LARGE + quality_index * 2 + u8::from(has_additional_data)
}

pub fn is_large_player_id_channel(channel: u8) -> bool {
    channel == VOICE_LARGE
        || (PLAYER_AVATAR_VERY_LOW_LARGE..=PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE).contains(&channel)
}

pub fn quality_from_channel(channel: u8) -> u8 {
    if channel >= PLAYER_AVATAR_VERY_LOW_LARGE {
        (channel - PLAYER_AVATAR_VERY_LOW_LARGE) / 2
    } else {
        (channel - PLAYER_AVATAR_VERY_LOW) / 2
    }
}

pub fn channel_has_additional_data(channel: u8) -> bool {
    if channel >= PLAYER_AVATAR_VERY_LOW_LARGE {
        ((channel - PLAYER_AVATAR_VERY_LOW_LARGE) & 1) == 1
    } else {
        ((channel - PLAYER_AVATAR_VERY_LOW) & 1) == 1
    }
}
