//! Built-in server plugins share one discovery catalog.
use basis_protocol::{
    avatar_host, channels,
    messages::{BasisMessageDescriptor, BasisMessageSupply},
    screen_share,
};

pub(crate) fn add_descriptors(supply: &mut BasisMessageSupply) {
    for (id, name) in [
        (avatar_host::MESSAGE_ID, avatar_host::MESSAGE_NAME),
        (screen_share::CONTROL_ID, screen_share::CONTROL_MESSAGE),
        (screen_share::MEDIA_ID, screen_share::MEDIA_MESSAGE),
    ] {
        supply.descriptors.push(BasisMessageDescriptor {
            id,
            version: 1,
            channel: channels::PLUGIN_RELIABLE,
            flags: 1,
            name: name.into(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn plugins_have_unique_manifest_ids_and_reliable_multiplexing() {
        let mut supply = basis_protocol::messages::core_message_supply();
        add_descriptors(&mut supply);
        let mut ids = std::collections::HashSet::new();
        assert!(supply.descriptors.iter().all(|d| ids.insert(d.id)));
        for descriptor in supply.descriptors.iter().filter(|d| d.id >= 64) {
            assert_eq!(descriptor.channel, channels::PLUGIN_RELIABLE);
            assert_eq!(descriptor.flags, 1);
        }
    }
}
