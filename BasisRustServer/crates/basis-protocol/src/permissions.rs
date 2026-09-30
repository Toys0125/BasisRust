pub mod nodes {
    pub const ALL: &str = "*";
    pub const HELP: &str = "basis.command.help";
    pub const SERVER_STATS: &str = "basis.server.stats";
    pub const RESOURCE_LOAD_WORLD: &str = "basis.resource.load.world";
    pub const RESOURCE_UNLOAD_WORLD: &str = "basis.resource.unload.world";
    pub const RESOURCE_LOAD_PROP: &str = "basis.resource.load.prop";
    pub const RESOURCE_UNLOAD_PROP: &str = "basis.resource.unload.prop";
    pub const RESOURCE_LOAD_AVATAR: &str = "basis.resource.load.avatar";
    pub const RESOURCE_UNLOAD_AVATAR: &str = "basis.resource.unload.avatar";
    pub const RESOURCE_LOCK_BYPASS_AVATAR: &str = "basis.resource.lockbypass.avatar";
    pub const RESOURCE_LOCK_BYPASS_PROP: &str = "basis.resource.lockbypass.prop";
    pub const RESOURCE_LOCK_BYPASS_WORLD: &str = "basis.resource.lockbypass.world";
    pub const RESOURCE_LOCK_BYPASS_SERVER: &str = "basis.resource.lockbypass.server";
    pub const CHAT_LOCK_BYPASS: &str = "basis.chat.lockbypass";
    pub const VOICE_LOCK_BYPASS: &str = "basis.voice.lockbypass";
    pub const OWNERSHIP_TRANSFER: &str = "basis.ownership.transfer";
    pub const OWNERSHIP_REMOVE: &str = "basis.ownership.remove";
    pub const OWNERSHIP_GET: &str = "basis.ownership.get";
    pub const CONTENT_SHARE_DELETE: &str = "basis.contentshare.delete";
    pub const CONTENT_SHARE_CREATE: &str = "basis.contentshare.create";
    pub const PROTECTION: &str = "basis.protection";
    pub const CONFIGURATION_EDITOR: &str = "basis.configuration";
    pub const PLAYER_MODERATION: &str = "basis.moderation";
    pub const MODERATION_BAN: &str = "basis.moderation.ban";
    pub const MODERATION_KICK: &str = "basis.moderation.kick";
    pub const MODERATION_IP_BAN: &str = "basis.moderation.ipban";
    pub const MODERATION_UNBAN: &str = "basis.moderation.unban";
    pub const MODERATION_UNBAN_IP: &str = "basis.moderation.unbanip";
    pub const MODERATION_MESSAGE: &str = "basis.moderation.message";
    pub const MODERATION_MESSAGE_ALL: &str = "basis.moderation.messageall";
    pub const MODERATION_TELEPORT: &str = "basis.moderation.teleport";
    pub const MODERATION_SHOUT: &str = "basis.moderation.shout";
    pub const MODERATION_GLOBAL_LOCK: &str = "basis.moderation.globallock";
    pub const MODERATION_FORCE_AVATAR: &str = "basis.moderation.forceavatar";
    pub const MODERATION_FULL_QUALITY_BROADCAST: &str = "basis.moderation.fullqualitybroadcast";
    pub const MODERATION_LOCOMOTION: &str = "basis.moderation.locomotion";
    pub const ADMIN_LOGS: &str = "basis.admin.logs";
    pub const MODERATION_HEADLESS_AUDIO: &str = "basis.moderation.headlessaudio";
    pub const MODERATION_OPUS_BITRATE: &str = "basis.moderation.opusbitrate";
    pub const MODERATION_WHITELIST: &str = "basis.moderation.whitelist";
    pub const PERMISSIONS_VIEW: &str = "basis.permissions.view";
    pub const PERMISSIONS_EDIT: &str = "basis.permissions.edit";

    pub const ALL_NODES: &[&str] = &[
        ALL,
        HELP,
        SERVER_STATS,
        RESOURCE_LOAD_WORLD,
        RESOURCE_UNLOAD_WORLD,
        RESOURCE_LOAD_PROP,
        RESOURCE_UNLOAD_PROP,
        RESOURCE_LOAD_AVATAR,
        RESOURCE_UNLOAD_AVATAR,
        RESOURCE_LOCK_BYPASS_AVATAR,
        RESOURCE_LOCK_BYPASS_PROP,
        RESOURCE_LOCK_BYPASS_WORLD,
        RESOURCE_LOCK_BYPASS_SERVER,
        CHAT_LOCK_BYPASS,
        VOICE_LOCK_BYPASS,
        OWNERSHIP_TRANSFER,
        OWNERSHIP_REMOVE,
        OWNERSHIP_GET,
        CONTENT_SHARE_DELETE,
        CONTENT_SHARE_CREATE,
        PROTECTION,
        CONFIGURATION_EDITOR,
        PLAYER_MODERATION,
        MODERATION_BAN,
        MODERATION_KICK,
        MODERATION_IP_BAN,
        MODERATION_UNBAN,
        MODERATION_UNBAN_IP,
        MODERATION_MESSAGE,
        MODERATION_MESSAGE_ALL,
        MODERATION_TELEPORT,
        MODERATION_SHOUT,
        MODERATION_GLOBAL_LOCK,
        MODERATION_FORCE_AVATAR,
        MODERATION_FULL_QUALITY_BROADCAST,
        MODERATION_LOCOMOTION,
        ADMIN_LOGS,
        MODERATION_HEADLESS_AUDIO,
        MODERATION_OPUS_BITRATE,
        MODERATION_WHITELIST,
        PERMISSIONS_VIEW,
        PERMISSIONS_EDIT,
    ];
}

pub const DEFAULT_GROUP_NODES: &[&str] = &[
    nodes::HELP,
    nodes::RESOURCE_LOAD_PROP,
    nodes::RESOURCE_UNLOAD_PROP,
    nodes::RESOURCE_LOAD_AVATAR,
    nodes::RESOURCE_UNLOAD_AVATAR,
    nodes::RESOURCE_LOAD_WORLD,
    nodes::RESOURCE_UNLOAD_WORLD,
    nodes::OWNERSHIP_TRANSFER,
    nodes::OWNERSHIP_REMOVE,
    nodes::OWNERSHIP_GET,
    nodes::CONTENT_SHARE_DELETE,
    nodes::CONTENT_SHARE_CREATE,
];
