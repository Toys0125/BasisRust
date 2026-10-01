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
    pub const MODERATION_ANNOUNCE: &str = "basis.moderation.announce";
    /// Legacy API name for the permission controlling announce mode.
    pub const MODERATION_SHOUT: &str = MODERATION_ANNOUNCE;
    pub const MODERATION_MUTE: &str = "basis.moderation.mute";
    pub const MODERATION_RENAME: &str = "basis.moderation.rename";
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
        MODERATION_ANNOUNCE,
        MODERATION_MUTE,
        MODERATION_RENAME,
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

/// BasisVR's moderator grants. Seed history allows upgrades without restoring removed grants.
pub const MODERATOR_GROUP_NODES: &[&str] = &[
    nodes::PLAYER_MODERATION,
    nodes::MODERATION_BAN,
    nodes::MODERATION_KICK,
    nodes::MODERATION_IP_BAN,
    nodes::MODERATION_UNBAN,
    nodes::MODERATION_UNBAN_IP,
    nodes::MODERATION_MESSAGE,
    nodes::MODERATION_MESSAGE_ALL,
    nodes::MODERATION_TELEPORT,
    nodes::MODERATION_ANNOUNCE,
    nodes::MODERATION_GLOBAL_LOCK,
    nodes::MODERATION_HEADLESS_AUDIO,
    nodes::MODERATION_OPUS_BITRATE,
    nodes::MODERATION_FULL_QUALITY_BROADCAST,
    nodes::MODERATION_FORCE_AVATAR,
    nodes::MODERATION_LOCOMOTION,
    nodes::MODERATION_MUTE,
    nodes::MODERATION_RENAME,
    nodes::PERMISSIONS_VIEW,
    nodes::RESOURCE_LOCK_BYPASS_AVATAR,
    nodes::RESOURCE_LOCK_BYPASS_PROP,
    nodes::RESOURCE_LOCK_BYPASS_WORLD,
    nodes::RESOURCE_LOCK_BYPASS_SERVER,
    nodes::CHAT_LOCK_BYPASS,
    nodes::VOICE_LOCK_BYPASS,
];

/// A stable key for .NET's OrdinalIgnoreCase equivalence classes.
/// Ordinal casing uses one-to-one uppercase mappings, not full case folding.
/// ICU .NET excludes dotless i and long s in pal_common.c's InitOrdinalCasingPage:
/// https://github.com/dotnet/runtime/blob/main/src/native/libs/System.Globalization.Native/pal_common.c
pub fn ordinal_ignore_case_key(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            let upper = match ch {
                '\u{0131}' | '\u{017f}' => ch,
                // UnicodeData simple uppercase differs from full casing for these Greek scalars.
                '\u{1f80}'..='\u{1f87}' | '\u{1f90}'..='\u{1f97}' | '\u{1fa0}'..='\u{1fa7}' => {
                    char::from_u32(ch as u32 + 8).unwrap()
                }
                '\u{1fb3}' => '\u{1fbc}',
                '\u{1fc3}' => '\u{1fcc}',
                '\u{1ff3}' => '\u{1ffc}',
                _ => {
                    let mut mapping = ch.to_uppercase();
                    let first = mapping.next().unwrap();
                    if mapping.next().is_none() {
                        first
                    } else {
                        ch
                    }
                }
            };
            // Keep ordinary permission nodes in their conventional lowercase spelling.
            upper.to_ascii_lowercase()
        })
        .collect()
}

pub fn ordinal_ignore_case_equal(left: &str, right: &str) -> bool {
    if left.is_ascii() && right.is_ascii() {
        left.eq_ignore_ascii_case(right)
    } else {
        ordinal_ignore_case_key(left) == ordinal_ignore_case_key(right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinal_casing_preserves_dotnet_equivalence_classes_without_expansions() {
        for (left, right) in [
            ("SIGMA.Σ", "sigma.ς"),
            ("Σ", "σ"),
            ("\u{1f80}", "\u{1f88}"),
            ("\u{1fb3}", "\u{1fbc}"),
            ("\u{10400}", "\u{10428}"),
            ("µ", "Μ"),
        ] {
            assert!(ordinal_ignore_case_equal(left, right), "{left} / {right}");
        }
        for (left, right) in [
            ("ı", "I"),
            ("ſ", "S"),
            ("İ", "i"),
            ("K", "K"),
            ("ß", "SS"),
            ("ß", "ẞ"),
            ("ﬀ", "ff"),
        ] {
            assert!(!ordinal_ignore_case_equal(left, right), "{left} / {right}");
        }
    }
}
