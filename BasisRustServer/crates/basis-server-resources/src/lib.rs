use anyhow::Result;
use basis_protocol::messages::{
    CameraPipPositionMessage, CameraPipStateMessage, ClientCameraPipPositionMessage,
    ClientCameraPipStateMessage, ContentShareCleanupMessage, ContentShareMessage,
    LocalLoadResource, OwnershipTransferMessage, PreloadReadyMessage, ResourceManagementMessage,
    ServerContentShareCleanupMessage, ServerContentShareMessage, SpawnPreloadedMessage,
    UnloadResource,
};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default)]
pub struct ResourceState {
    resources: Arc<RwLock<HashMap<String, ResourceManagementMessage>>>,
    preload_sessions: Arc<RwLock<HashMap<String, PreloadSession>>>,
}

impl ResourceState {
    pub fn load_resource(&self, message: ResourceManagementMessage) -> bool {
        let id = message.loaded_net_id.clone();
        if id.is_empty() {
            return false;
        }
        let mut resources = self.resources.write();
        if resources.contains_key(&id) {
            return false;
        }
        resources.insert(id, message);
        true
    }

    pub fn unload_resource(&self, id: &str) -> Option<ResourceManagementMessage> {
        self.preload_sessions.write().remove(id);
        self.resources.write().remove(id)
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
                resources.remove(&id).map(|resource| UnloadResource {
                    mode: resource.mode,
                    loaded_net_id: id,
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
                resources.remove(&id).map(|resource| UnloadResource {
                    mode: resource.mode,
                    loaded_net_id: id,
                })
            })
            .collect()
    }

    pub fn reset(&self) {
        self.resources.write().clear();
        self.preload_sessions.write().clear();
    }

    pub fn all_resources(&self) -> Vec<ResourceManagementMessage> {
        self.resources.read().values().cloned().collect()
    }

    pub fn start_preload(&self, resource: LocalLoadResource, peers: &[u16]) -> bool {
        if !self.load_resource(resource.clone()) {
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
    next_id: Arc<RwLock<u16>>,
}

impl NetIdState {
    pub fn add_or_find(&self, name: &str) -> u16 {
        if let Some(id) = self.by_name.read().get(name).copied() {
            return id;
        }
        let mut by_name = self.by_name.write();
        if let Some(id) = by_name.get(name).copied() {
            return id;
        }
        let mut next = self.next_id.write();
        let id = *next;
        *next = next.wrapping_add(1);
        by_name.insert(name.to_string(), id);
        id
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

#[derive(Debug, Clone, Default)]
pub struct ContentShareState {
    spheres: Arc<RwLock<HashMap<String, ServerContentShareMessage>>>,
}

impl ContentShareState {
    pub fn add(
        &self,
        player_id: u16,
        sharer_uuid: String,
        sharer_display_name: String,
        message: ContentShareMessage,
    ) -> Option<ServerContentShareMessage> {
        let server = ServerContentShareMessage {
            player_id,
            sharer_uuid,
            sharer_display_name,
            content_share_message: message,
        };
        let mut spheres = self.spheres.write();
        if spheres.contains_key(&server.content_share_message.sphere_net_id) {
            return None;
        }
        spheres.insert(
            server.content_share_message.sphere_net_id.clone(),
            server.clone(),
        );
        Some(server)
    }

    pub fn remove(
        &self,
        player_id: u16,
        message: ContentShareCleanupMessage,
    ) -> Option<ServerContentShareCleanupMessage> {
        if self
            .spheres
            .write()
            .remove(&message.sphere_net_id)
            .is_some()
        {
            Some(ServerContentShareCleanupMessage {
                player_id,
                content_share_cleanup_message: message,
            })
        } else {
            None
        }
    }

    pub fn remove_player(&self, player_id: u16) -> Vec<ServerContentShareCleanupMessage> {
        let mut spheres = self.spheres.write();
        let keys: Vec<_> = spheres
            .iter()
            .filter_map(|(key, value)| (value.player_id == player_id).then_some(key.clone()))
            .collect();
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            spheres.remove(&key);
            removed.push(ServerContentShareCleanupMessage {
                player_id,
                content_share_cleanup_message: ContentShareCleanupMessage { sphere_net_id: key },
            });
        }
        removed
    }

    pub fn all(&self) -> Vec<ServerContentShareMessage> {
        self.spheres.read().values().cloned().collect()
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

#[derive(Debug, Clone, Default)]
pub struct DefaultLibrary {
    pub entries: Vec<ResourceManagementMessage>,
}

impl DefaultLibrary {
    pub fn load_xml_dir(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let mut entries = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.path().extension().and_then(|e| e.to_str()) == Some("xml") {
                let combined_url = fs::read_to_string(entry.path()).unwrap_or_default();
                entries.push(ResourceManagementMessage {
                    mode: 0,
                    loaded_net_id: String::new(),
                    unlock_password: String::new(),
                    load_strategy: 0,
                    combined_url,
                    uuid_of_creator: String::new(),
                    is_admin_locked: false,
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
                    modify_scale: false,
                });
            }
        }
        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
