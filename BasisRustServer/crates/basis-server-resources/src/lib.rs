mod default_library;

use basis_protocol::messages::{
    CameraPipPositionMessage, CameraPipStateMessage, ClientCameraPipPositionMessage,
    ClientCameraPipStateMessage, ContentShareCleanupMessage, ContentShareMessage,
    LocalLoadResource, ModifyResource, OwnershipTransferMessage, PreloadReadyMessage,
    ResourceManagementMessage, ServerContentShareCleanupMessage, ServerContentShareMessage,
    SpawnPreloadedMessage, UnloadResource,
};
pub use default_library::{DefaultLibrary, DefaultLibraryEntry};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default)]
pub struct ResourceState {
    resources: Arc<RwLock<HashMap<String, ResourceManagementMessage>>>,
    creator_counts: Arc<RwLock<HashMap<String, usize>>>,
    preload_sessions: Arc<RwLock<HashMap<String, PreloadSession>>>,
}

impl ResourceState {
    pub fn load_resource(&self, message: ResourceManagementMessage) -> bool {
        self.load_resource_with_limit(message, usize::MAX)
    }

    pub fn load_resource_with_limit(
        &self,
        message: ResourceManagementMessage,
        max_per_creator: usize,
    ) -> bool {
        self.insert_resource(&mut self.resources.write(), message, max_per_creator)
    }

    fn insert_resource(
        &self,
        resources: &mut HashMap<String, ResourceManagementMessage>,
        message: ResourceManagementMessage,
        max_per_creator: usize,
    ) -> bool {
        let id = message.loaded_net_id.clone();
        if id.is_empty() {
            return false;
        }
        let creator = message.uuid_of_creator.clone();
        if resources.contains_key(&id) {
            return false;
        }
        if !creator.is_empty() {
            let mut counts = self.creator_counts.write();
            let count = counts.get(&creator).copied().unwrap_or(0);
            if count >= max_per_creator {
                return false;
            }
            counts.insert(creator.clone(), count + 1);
        }
        resources.insert(id, message);
        true
    }

    pub fn unload_resource(&self, id: &str) -> Option<ResourceManagementMessage> {
        self.unload_resource_if(id, |_| true)
    }

    /// Authorize the actual resource under the same lock as removal.
    pub fn unload_resource_if(
        &self,
        id: &str,
        allowed: impl FnOnce(&ResourceManagementMessage) -> bool,
    ) -> Option<ResourceManagementMessage> {
        let mut resources = self.resources.write();
        if !allowed(resources.get(id)?) {
            return None;
        }
        let removed = resources.remove(id)?;
        // Keep the resource lock until cleanup completes so a replacement with
        // the same ID cannot lose its preload session or creator accounting.
        self.preload_sessions.write().remove(id);
        self.note_resource_removed(&removed.uuid_of_creator);
        Some(removed)
    }

    fn note_resource_removed(&self, creator: &str) {
        if creator.is_empty() {
            return;
        }
        let mut counts = self.creator_counts.write();
        match counts.get(creator).copied() {
            Some(0) | None => {}
            Some(1) => {
                counts.remove(creator);
            }
            Some(count) => {
                counts.insert(creator.to_string(), count - 1);
            }
        }
    }

    pub fn modify_resource(&self, message: &ModifyResource) -> bool {
        self.modify_resource_if(message, |resource| resource.mode == message.mode)
            .is_some()
    }

    /// Authorize and update the current resource under one write lock. Returns
    /// the committed message with server-owned mode and normalized static flags.
    pub fn modify_resource_if(
        &self,
        message: &ModifyResource,
        allowed: impl FnOnce(&ResourceManagementMessage) -> bool,
    ) -> Option<ModifyResource> {
        let mut resources = self.resources.write();
        let resource = resources.get_mut(&message.loaded_net_id)?;
        if !allowed(resource) {
            return None;
        }
        let committed = ModifyResource {
            mode: resource.mode,
            loaded_net_id: message.loaded_net_id.clone(),
            static_resource: message.static_resource || message.static_admin_locked,
            static_admin_locked: message.static_admin_locked,
        };
        resource.static_admin_locked = committed.static_admin_locked;
        resource.static_resource = committed.static_resource;
        Some(committed)
    }

    pub fn all_scene_unloads(&self) -> Vec<UnloadResource> {
        let scene_ids: Vec<String> = self
            .resources
            .read()
            .iter()
            .filter_map(|(id, resource)| (resource.mode == 1).then_some(id.clone()))
            .collect();
        let mut resources = self.resources.write();
        scene_ids
            .into_iter()
            .filter_map(|id| {
                resources.remove(&id).map(|resource| {
                    self.note_resource_removed(&resource.uuid_of_creator);
                    UnloadResource {
                        mode: resource.mode,
                        loaded_net_id: id,
                    }
                })
            })
            .collect()
    }

    pub fn reset_non_persistent(&self) -> Vec<UnloadResource> {
        let ids: Vec<String> = self
            .resources
            .read()
            .iter()
            .filter_map(|(id, resource)| (!resource.persist).then_some(id.clone()))
            .collect();
        let mut resources = self.resources.write();
        ids.into_iter()
            .filter_map(|id| {
                resources.remove(&id).map(|resource| {
                    self.note_resource_removed(&resource.uuid_of_creator);
                    UnloadResource {
                        mode: resource.mode,
                        loaded_net_id: id,
                    }
                })
            })
            .collect()
    }

    pub fn reset(&self) {
        self.resources.write().clear();
        self.creator_counts.write().clear();
        self.preload_sessions.write().clear();
    }

    pub fn all_resources(&self) -> Vec<ResourceManagementMessage> {
        self.resources.read().values().cloned().collect()
    }

    pub fn remove_creator_non_persistent(&self, creator: &str) -> Vec<UnloadResource> {
        if creator.is_empty() {
            return Vec::new();
        }
        let ids: Vec<String> = self
            .resources
            .read()
            .iter()
            .filter_map(|(id, resource)| {
                (!resource.persist && resource.uuid_of_creator == creator).then_some(id.clone())
            })
            .collect();
        let mut resources = self.resources.write();
        ids.into_iter()
            .filter_map(|id| {
                resources.remove(&id).map(|resource| {
                    self.preload_sessions.write().remove(&id);
                    self.note_resource_removed(&resource.uuid_of_creator);
                    UnloadResource {
                        mode: resource.mode,
                        loaded_net_id: id,
                    }
                })
            })
            .collect()
    }

    pub fn start_preload(
        &self,
        resource: LocalLoadResource,
        peers: &[u16],
        max_per_creator: usize,
    ) -> bool {
        let mut resources = self.resources.write();
        if !self.insert_resource(&mut resources, resource.clone(), max_per_creator) {
            return false;
        }
        self.preload_sessions.write().insert(
            resource.loaded_net_id.clone(),
            PreloadSession {
                ready_peers: HashSet::new(),
                failed_peers: HashSet::new(),
                total_peer_count: peers.len(),
                started_at: Instant::now(),
            },
        );
        true
    }

    pub fn mark_preload_ready(
        &self,
        peer_id: u16,
        message: PreloadReadyMessage,
    ) -> Option<SpawnPreloadedMessage> {
        let mut sessions = self.preload_sessions.write();
        let session = sessions.get_mut(&message.loaded_net_id)?;
        session.ready_peers.remove(&peer_id);
        session.failed_peers.remove(&peer_id);
        if message.is_ready {
            session.ready_peers.insert(peer_id);
        } else {
            session.failed_peers.insert(peer_id);
        }
        if session.is_complete() {
            sessions.remove(&message.loaded_net_id);
            Some(SpawnPreloadedMessage {
                loaded_net_id: message.loaded_net_id,
            })
        } else {
            None
        }
    }

    pub fn remove_preload_peer(&self, peer_id: u16) -> Vec<SpawnPreloadedMessage> {
        let mut sessions = self.preload_sessions.write();
        let mut completed = Vec::new();
        for (id, session) in sessions.iter_mut() {
            session.ready_peers.remove(&peer_id);
            session.failed_peers.remove(&peer_id);
            session.total_peer_count = session.total_peer_count.saturating_sub(1);
            if session.total_peer_count == 0 || session.is_complete() {
                completed.push(id.clone());
            }
        }
        completed
            .into_iter()
            .filter_map(|id| {
                sessions
                    .remove(&id)
                    .map(|_| SpawnPreloadedMessage { loaded_net_id: id })
            })
            .collect()
    }

    pub fn timed_out_preloads(&self) -> Vec<SpawnPreloadedMessage> {
        const TIMEOUT: Duration = Duration::from_secs(5 * 60);
        let mut sessions = self.preload_sessions.write();
        let ids: Vec<String> = sessions
            .iter()
            .filter_map(|(id, session)| {
                (session.started_at.elapsed() >= TIMEOUT).then_some(id.clone())
            })
            .collect();
        ids.into_iter()
            .filter_map(|id| {
                sessions
                    .remove(&id)
                    .map(|_| SpawnPreloadedMessage { loaded_net_id: id })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct PreloadSession {
    ready_peers: HashSet<u16>,
    failed_peers: HashSet<u16>,
    total_peer_count: usize,
    started_at: Instant,
}

impl PreloadSession {
    fn is_complete(&self) -> bool {
        self.ready_peers.len() + self.failed_peers.len() >= self.total_peer_count
    }
}

#[derive(Debug, Clone, Default)]
pub struct NetIdState {
    by_name: Arc<RwLock<HashMap<String, u16>>>,
    next_id: Arc<RwLock<u32>>,
    per_peer_assigned: Arc<RwLock<HashMap<u16, usize>>>,
}

impl NetIdState {
    pub fn add_or_find_for_peer(
        &self,
        name: &str,
        peer_id: u16,
        max_per_peer: usize,
    ) -> Option<(u16, bool)> {
        if let Some(id) = self.by_name.read().get(name).copied() {
            return Some((id, true));
        }
        let mut by_name = self.by_name.write();
        if let Some(id) = by_name.get(name).copied() {
            return Some((id, true));
        }
        let mut counts = self.per_peer_assigned.write();
        let assigned = counts.get(&peer_id).copied().unwrap_or(0);
        if assigned >= max_per_peer {
            return None;
        }
        let mut next = self.next_id.write();
        if *next > u16::MAX as u32 {
            return None;
        }
        let id = *next as u16;
        *next += 1;
        by_name.insert(name.to_string(), id);
        counts.insert(peer_id, assigned + 1);
        Some((id, false))
    }

    pub fn remove_peer(&self, peer_id: u16) {
        self.per_peer_assigned.write().remove(&peer_id);
    }

    pub fn find(&self, name: &str) -> Option<u16> {
        self.by_name.read().get(name).copied()
    }

    pub fn all(&self) -> Vec<(String, u16)> {
        self.by_name
            .read()
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect()
    }

    pub fn reset(&self) {
        self.by_name.write().clear();
        *self.next_id.write() = 0;
        self.per_peer_assigned.write().clear();
    }
}

#[derive(Debug, Clone, Default)]
pub struct OwnershipState {
    by_object: Arc<RwLock<HashMap<String, u16>>>,
}

impl OwnershipState {
    pub fn request_new_or_existing(&self, ownership_id: &str, requester: u16) -> u16 {
        let mut by_object = self.by_object.write();
        *by_object
            .entry(ownership_id.to_string())
            .or_insert(requester)
    }

    pub fn switch_ownership(&self, ownership_id: &str, new_owner: u16) -> u16 {
        self.by_object
            .write()
            .insert(ownership_id.to_string(), new_owner);
        new_owner
    }

    pub fn remove_if_owner(&self, ownership_id: &str, owner: u16) -> bool {
        let mut by_object = self.by_object.write();
        if by_object.get(ownership_id).copied() == Some(owner) {
            by_object.remove(ownership_id);
            true
        } else {
            false
        }
    }

    pub fn remove_player(&self, player_id: u16) -> Vec<OwnershipTransferMessage> {
        let mut by_object = self.by_object.write();
        let keys: Vec<_> = by_object
            .iter()
            .filter_map(|(object, owner)| (*owner == player_id).then_some(object.clone()))
            .collect();
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            by_object.remove(&key);
            removed.push(OwnershipTransferMessage {
                player_id,
                ownership_id: key,
            });
        }
        removed
    }

    pub fn all(&self) -> Vec<OwnershipTransferMessage> {
        self.by_object
            .read()
            .iter()
            .map(|(ownership_id, player_id)| OwnershipTransferMessage {
                player_id: *player_id,
                ownership_id: ownership_id.clone(),
            })
            .collect()
    }

    pub fn reset(&self) {
        self.by_object.write().clear();
    }
}

#[derive(Debug, Clone)]
struct SharedSphere {
    message: ServerContentShareMessage,
    dropped_at: Instant,
    left_at: Option<Instant>,
}

#[derive(Debug, Clone, Default)]
pub struct ContentShareState {
    spheres: Arc<RwLock<HashMap<String, SharedSphere>>>,
}

impl ContentShareState {
    pub fn add(
        &self,
        player_id: u16,
        sharer_uuid: String,
        sharer_display_name: String,
        message: ContentShareMessage,
    ) -> Option<ServerContentShareMessage> {
        self.add_with_limit(
            player_id,
            sharer_uuid,
            sharer_display_name,
            message,
            usize::MAX,
        )
    }

    pub fn add_with_limit(
        &self,
        player_id: u16,
        sharer_uuid: String,
        sharer_display_name: String,
        message: ContentShareMessage,
        max_per_player: usize,
    ) -> Option<ServerContentShareMessage> {
        self.add_with_limit_and_protection(
            player_id,
            sharer_uuid,
            sharer_display_name,
            message,
            max_per_player,
            false,
        )
    }

    pub fn add_with_limit_and_protection(
        &self,
        player_id: u16,
        sharer_uuid: String,
        sharer_display_name: String,
        message: ContentShareMessage,
        max_per_player: usize,
        sharer_protected: bool,
    ) -> Option<ServerContentShareMessage> {
        let server = ServerContentShareMessage {
            player_id,
            sharer_uuid,
            sharer_display_name,
            content_share_message: message,
            sharer_protected,
        };
        let mut spheres = self.spheres.write();
        if spheres.contains_key(&server.content_share_message.sphere_net_id)
            || spheres
                .values()
                .filter(|sphere| sphere.message.player_id == player_id && sphere.left_at.is_none())
                .count()
                >= max_per_player
        {
            return None;
        }
        spheres.insert(
            server.content_share_message.sphere_net_id.clone(),
            SharedSphere {
                message: server.clone(),
                dropped_at: Instant::now(),
                left_at: None,
            },
        );
        Some(server)
    }

    /// Check ownership and protection under the same lock as removal, so concurrent
    /// requests cannot bypass the content-removal policy.
    pub fn remove_authorized(
        &self,
        player_id: u16,
        message: ContentShareCleanupMessage,
        is_moderator: bool,
        content_removal_locked: bool,
    ) -> Option<ServerContentShareCleanupMessage> {
        let mut spheres = self.spheres.write();
        let existing = spheres.get(&message.sphere_net_id)?;
        let is_owner = existing.left_at.is_none() && existing.message.player_id == player_id;
        if !is_owner
            && !is_moderator
            && (existing.message.sharer_protected || content_removal_locked)
        {
            return None;
        }
        spheres.remove(&message.sphere_net_id)?;
        Some(ServerContentShareCleanupMessage {
            player_id,
            content_share_cleanup_message: message,
        })
    }

    pub fn remove_player(&self, player_id: u16) -> Vec<ServerContentShareCleanupMessage> {
        self.remove_player_with_timeout(player_id, 0)
    }

    pub fn remove_player_with_timeout(
        &self,
        player_id: u16,
        leave_timeout_seconds: i32,
    ) -> Vec<ServerContentShareCleanupMessage> {
        let mut spheres = self.spheres.write();
        let now = Instant::now();
        let keys: Vec<_> = spheres
            .iter()
            .filter_map(|(key, value)| {
                (value.message.player_id == player_id && value.left_at.is_none())
                    .then_some(key.clone())
            })
            .collect();
        let mut removed = Vec::new();
        for key in keys {
            if leave_timeout_seconds > 0 {
                if let Some(sphere) = spheres.get_mut(&key) {
                    sphere.left_at = Some(now);
                }
            } else {
                spheres.remove(&key);
                removed.push(ServerContentShareCleanupMessage {
                    player_id,
                    content_share_cleanup_message: ContentShareCleanupMessage {
                        sphere_net_id: key,
                    },
                });
            }
        }
        removed
    }

    pub fn expire_due(
        &self,
        now: Instant,
        leave_timeout_seconds: i32,
        deletion_timer_seconds: i32,
    ) -> Vec<ServerContentShareCleanupMessage> {
        let mut spheres = self.spheres.write();
        let leave_after = Duration::from_secs(leave_timeout_seconds.max(0) as u64);
        let delete_after = Duration::from_secs(deletion_timer_seconds.max(0) as u64);
        let due: Vec<_> = spheres
            .iter()
            .filter_map(|(key, sphere)| {
                let left_expired = sphere
                    .left_at
                    .is_some_and(|left| now.saturating_duration_since(left) >= leave_after);
                let lifetime_expired = deletion_timer_seconds > 0
                    && now.saturating_duration_since(sphere.dropped_at) >= delete_after;
                (left_expired || lifetime_expired)
                    .then_some((key.clone(), sphere.message.player_id))
            })
            .collect();
        due.into_iter()
            .map(|(key, player_id)| {
                spheres.remove(&key);
                ServerContentShareCleanupMessage {
                    player_id,
                    content_share_cleanup_message: ContentShareCleanupMessage {
                        sphere_net_id: key,
                    },
                }
            })
            .collect()
    }

    pub fn all(&self) -> Vec<ServerContentShareMessage> {
        self.spheres
            .read()
            .values()
            .map(|s| s.message.clone())
            .collect()
    }

    pub fn reset(&self) {
        self.spheres.write().clear();
    }
}

#[derive(Debug, Clone)]
pub struct PipCameraState {
    pub state: CameraPipStateMessage,
    pub has_new_data: bool,
    pub last_sent_times: HashMap<u16, Instant>,
}

#[derive(Debug, Clone, Default)]
pub struct PipState {
    states: Arc<RwLock<HashMap<u16, PipCameraState>>>,
}

impl PipState {
    pub fn state_change(
        &self,
        player_id: u16,
        message: ClientCameraPipStateMessage,
    ) -> CameraPipStateMessage {
        let state = CameraPipStateMessage {
            player_id,
            is_active: message.is_active,
            position_x: message.position_x,
            position_y: message.position_y,
            position_z: message.position_z,
            rotation_x: message.rotation_x,
            rotation_y: message.rotation_y,
            rotation_z: message.rotation_z,
            rotation_w: message.rotation_w,
        };
        if message.is_active {
            self.states.write().insert(
                player_id,
                PipCameraState {
                    state: state.clone(),
                    has_new_data: true,
                    last_sent_times: HashMap::new(),
                },
            );
        } else if let Some(existing) = self.states.write().get_mut(&player_id) {
            existing.state.is_active = false;
            existing.has_new_data = false;
            existing.last_sent_times.clear();
        }
        state
    }

    pub fn position_update(
        &self,
        player_id: u16,
        message: ClientCameraPipPositionMessage,
    ) -> Option<CameraPipPositionMessage> {
        let mut states = self.states.write();
        let state = states.get_mut(&player_id)?;
        if !state.state.is_active {
            return None;
        }
        state.state.position_x = message.position_x;
        state.state.position_y = message.position_y;
        state.state.position_z = message.position_z;
        state.state.rotation_x = message.rotation_x;
        state.state.rotation_y = message.rotation_y;
        state.state.rotation_z = message.rotation_z;
        state.state.rotation_w = message.rotation_w;
        state.has_new_data = true;
        Some(CameraPipPositionMessage {
            player_id,
            position_x: message.position_x,
            position_y: message.position_y,
            position_z: message.position_z,
            rotation_x: message.rotation_x,
            rotation_y: message.rotation_y,
            rotation_z: message.rotation_z,
            rotation_w: message.rotation_w,
        })
    }

    pub fn remove_player(&self, player_id: u16) -> Option<CameraPipStateMessage> {
        self.states.write().remove(&player_id).and_then(|state| {
            state.state.is_active.then_some(CameraPipStateMessage {
                is_active: false,
                ..state.state
            })
        })
    }

    pub fn all_active(&self) -> Vec<CameraPipStateMessage> {
        self.states
            .read()
            .values()
            .filter_map(|state| state.state.is_active.then_some(state.state.clone()))
            .collect()
    }

    pub fn reset(&self) {
        self.states.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource(id: &str, creator: &str) -> ResourceManagementMessage {
        ResourceManagementMessage {
            mode: 0,
            loaded_net_id: id.into(),
            unlock_password: String::new(),
            combined_url: "https://example.com/resource".into(),
            uuid_of_creator: creator.into(),
            is_admin_locked: true,
            position_x: 0.0,
            position_y: 0.0,
            position_z: 0.0,
            quaternion_x: 0.0,
            quaternion_y: 0.0,
            quaternion_z: 0.0,
            quaternion_w: 1.0,
            scale_x: 1.0,
            scale_y: 1.0,
            scale_z: 1.0,
            persist: false,
            static_resource: false,
            static_admin_locked: false,
            modify_scale: false,
            load_strategy: 0,
        }
    }

    #[test]
    fn authorized_modification_checks_the_current_resource_under_the_write_lock() {
        let state = ResourceState::default();
        assert!(state.load_resource(resource("one", "alice")));
        let request = ModifyResource {
            mode: 99,
            loaded_net_id: "one".into(),
            static_resource: false,
            static_admin_locked: true,
        };
        let committed = state
            .modify_resource_if(&request, |current| {
                assert!(
                    state.resources.try_write().is_none(),
                    "authorization must hold the mutation lock"
                );
                current.uuid_of_creator == "alice"
            })
            .unwrap();
        assert_eq!(committed.mode, 0);
        assert!(committed.static_resource && committed.static_admin_locked);
        state.unload_resource("one").unwrap();
        assert!(state.load_resource(resource("one", "bob")));
        assert!(state
            .modify_resource_if(&request, |current| current.uuid_of_creator == "alice")
            .is_none());
        let current = state.resources.read()["one"].clone();
        assert!(!current.static_resource && !current.static_admin_locked);
        assert!(state
            .modify_resource_if(
                &ModifyResource {
                    loaded_net_id: "missing".into(),
                    ..request.clone()
                },
                |_| panic!("missing IDs must not scan resources")
            )
            .is_none());
        assert!(
            !state.modify_resource(&request),
            "legacy API must still reject mismatched mode"
        );
    }

    #[test]
    fn authorized_unload_preserves_denied_preloads_and_cleans_up_once() {
        let state = ResourceState::default();
        assert!(state.start_preload(resource("one", "alice"), &[1], 1));
        assert!(state
            .unload_resource_if("missing", |_| panic!("missing ID must not scan resources"))
            .is_none());
        assert!(state
            .unload_resource_if("one", |resource| !resource.is_admin_locked)
            .is_none());
        assert!(state.preload_sessions.read().contains_key("one"));
        assert!(!state.load_resource_with_limit(resource("two", "alice"), 1));
        assert!(state
            .unload_resource_if("one", |resource| resource.uuid_of_creator == "alice")
            .is_some());
        assert!(!state.preload_sessions.read().contains_key("one"));
        assert!(state.unload_resource("one").is_none());
        assert!(state.load_resource_with_limit(resource("one", "bob"), 1));
        assert!(state
            .unload_resource_if("one", |resource| resource.uuid_of_creator == "alice")
            .is_none());
        assert!(state.load_resource_with_limit(resource("two", "alice"), 1));
        assert_eq!(state.creator_counts.read().get("bob"), Some(&1));
    }

    #[test]
    fn net_id_namespace_exhausts_gracefully_and_preserves_live_names_until_reset() {
        let state = NetIdState::default();
        for id in 0..=u16::MAX {
            assert_eq!(
                state.add_or_find_for_peer(&format!("name-{id}"), 1, 65536),
                Some((id, false))
            );
        }
        assert_eq!(state.add_or_find_for_peer("overflow", 2, 1), None);
        assert_eq!(
            state.add_or_find_for_peer("name-65535", 2, 0),
            Some((u16::MAX, true))
        );
        state.remove_peer(1);
        assert_eq!(state.find("name-0"), Some(0));
        assert_eq!(state.add_or_find_for_peer("still-overflow", 2, 1), None);
        state.reset();
        assert_eq!(state.find("name-0"), None);
        assert_eq!(
            state.add_or_find_for_peer("new-session", 2, 1),
            Some((0, false))
        );
        assert_eq!(state.add_or_find_for_peer("peer-quota", 2, 1), None);
    }

    #[test]
    fn ownership_removes_player_owned_objects() {
        let state = OwnershipState::default();
        state.switch_ownership("a", 7);
        state.switch_ownership("b", 8);
        let removed = state.remove_player(7);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].ownership_id, "a");
    }

    #[test]
    fn protected_content_cleanup_requires_owner_or_moderator() {
        let state = ContentShareState::default();
        let msg = ContentShareMessage {
            sphere_net_id: "protected".into(),
            content_url: "u".into(),
            unlock_password: String::new(),
            content_type: basis_protocol::messages::ContentShareType::Avatar,
            position_x: 0.0,
            position_y: 0.0,
            position_z: 0.0,
        };
        state.add_with_limit_and_protection(1, "owner".into(), "name".into(), msg, 10, true);
        let cleanup = || ContentShareCleanupMessage {
            sphere_net_id: "protected".into(),
        };
        assert!(state
            .remove_authorized(2, cleanup(), false, false)
            .is_none());
        assert_eq!(state.all().len(), 1);
        assert!(state.remove_authorized(1, cleanup(), false, true).is_some());
        assert!(state.all().is_empty());
    }

    #[test]
    fn locked_content_cleanup_requires_owner_or_moderator() {
        let state = ContentShareState::default();
        let msg = ContentShareMessage {
            sphere_net_id: "locked".into(),
            content_url: "u".into(),
            unlock_password: String::new(),
            content_type: basis_protocol::messages::ContentShareType::Avatar,
            position_x: 0.0,
            position_y: 0.0,
            position_z: 0.0,
        };
        state.add(1, "owner".into(), "name".into(), msg);
        let cleanup = || ContentShareCleanupMessage {
            sphere_net_id: "locked".into(),
        };
        assert!(state.remove_authorized(2, cleanup(), false, true).is_none());
        assert!(state.remove_authorized(2, cleanup(), true, true).is_some());
    }

    #[test]
    fn spheres_expire_after_disconnect_or_configured_lifetime() {
        let state = ContentShareState::default();
        let msg = ContentShareMessage {
            sphere_net_id: "later".into(),
            content_url: "u".into(),
            unlock_password: String::new(),
            content_type: basis_protocol::messages::ContentShareType::Avatar,
            position_x: 0.0,
            position_y: 0.0,
            position_z: 0.0,
        };
        state.add(8, "owner".into(), "name".into(), msg);
        assert!(state.remove_player_with_timeout(8, 60).is_empty());
        assert_eq!(state.all().len(), 1);
        assert!(state.expire_due(Instant::now(), 60, 0).is_empty());
        assert_eq!(
            state
                .expire_due(Instant::now() + Duration::from_secs(61), 60, 0)
                .len(),
            1
        );
        assert!(state.all().is_empty());
    }

    #[test]
    fn content_share_rejects_duplicate_sphere_id() {
        let state = ContentShareState::default();
        let msg = ContentShareMessage {
            sphere_net_id: "s".to_string(),
            content_url: "u".to_string(),
            unlock_password: String::new(),
            content_type: basis_protocol::messages::ContentShareType::Avatar,
            position_x: 0.0,
            position_y: 0.0,
            position_z: 0.0,
        };
        assert!(state
            .add(1, "uuid".to_string(), "name".to_string(), msg.clone())
            .is_some());
        assert!(state
            .add(1, "uuid".to_string(), "name".to_string(), msg)
            .is_none());
    }
}
