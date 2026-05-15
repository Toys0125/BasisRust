use anyhow::{Context, Result};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

pub use basis_protocol::permissions::nodes;

#[derive(Debug, Clone, Default)]
pub struct PermissionUser {
    pub uuid: String,
    pub nodes: HashSet<String>,
    pub groups: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PermissionGroup {
    pub name: String,
    pub nodes: HashSet<String>,
    pub parents: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PermissionStore {
    pub users: HashMap<String, PermissionUser>,
    pub groups: HashMap<String, PermissionGroup>,
}

#[derive(Debug, Clone)]
pub struct PermissionManager {
    path: Arc<RwLock<PathBuf>>,
    store: Arc<RwLock<PermissionStore>>,
}

impl Default for PermissionManager {
    fn default() -> Self {
        Self {
            path: Arc::new(RwLock::new(PathBuf::from("permissions.xml"))),
            store: Arc::new(RwLock::new(PermissionStore::default())),
        }
    }
}

impl PermissionManager {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let manager = Self::default();
        *manager.path.write() = path.into();
        manager
    }

    pub fn get_xml_path(&self) -> PathBuf {
        self.path.read().clone()
    }

    pub fn set_xml_path(&self, path: impl Into<PathBuf>) {
        *self.path.write() = path.into();
    }

    pub fn load_from_xml(&self) -> Result<()> {
        let path = self.get_xml_path();
        let store = load_permissions(&path)?;
        *self.store.write() = store;
        Ok(())
    }

    pub fn save_to_xml(&self) -> Result<()> {
        let path = self.get_xml_path();
        save_permissions(&path, &self.snapshot())
    }

    pub fn ensure_defaults(&self) {
        let mut store = self.store.write();
        store
            .groups
            .entry("default".to_string())
            .or_insert_with(|| {
                let mut group = PermissionGroup {
                    name: "default".to_string(),
                    ..Default::default()
                };
                for node in basis_protocol::permissions::DEFAULT_GROUP_NODES {
                    group.nodes.insert((*node).to_string());
                }
                group
            });
        store
            .groups
            .entry("moderator".to_string())
            .or_insert_with(|| {
                let mut group = PermissionGroup {
                    name: "moderator".to_string(),
                    ..Default::default()
                };
                group.parents.insert("default".to_string());
                for node in [
                    nodes::MODERATION_BAN,
                    nodes::MODERATION_KICK,
                    nodes::MODERATION_IP_BAN,
                    nodes::MODERATION_UNBAN,
                    nodes::MODERATION_UNBAN_IP,
                    nodes::MODERATION_MESSAGE,
                    nodes::MODERATION_MESSAGE_ALL,
                    nodes::MODERATION_TELEPORT,
                    nodes::MODERATION_SHOUT,
                    nodes::MODERATION_GLOBAL_LOCK,
                    nodes::MODERATION_HEADLESS_AUDIO,
                    nodes::MODERATION_OPUS_BITRATE,
                    nodes::PERMISSIONS_VIEW,
                    nodes::RESOURCE_LOCK_BYPASS_AVATAR,
                    nodes::RESOURCE_LOCK_BYPASS_PROP,
                    nodes::RESOURCE_LOCK_BYPASS_WORLD,
                    nodes::RESOURCE_LOCK_BYPASS_SERVER,
                ] {
                    group.nodes.insert(node.to_string());
                }
                group
            });
        store.groups.entry("admin".to_string()).or_insert_with(|| {
            let mut group = PermissionGroup {
                name: "admin".to_string(),
                ..Default::default()
            };
            group.nodes.insert(nodes::ALL.to_string());
            group.parents.insert("moderator".to_string());
            group
        });
    }

    pub fn snapshot(&self) -> PermissionStore {
        self.store.read().clone()
    }

    pub fn get_or_create_user(&self, uuid: &str) {
        let mut store = self.store.write();
        store.users.entry(uuid.to_string()).or_insert_with(|| {
            let mut user = PermissionUser {
                uuid: uuid.to_string(),
                ..Default::default()
            };
            user.groups.insert("default".to_string());
            user
        });
    }

    pub fn add_user_node(&self, uuid: &str, node: &str) {
        self.get_or_create_user(uuid);
        if let Some(user) = self.store.write().users.get_mut(uuid) {
            user.nodes.insert(node.trim().to_string());
        }
    }

    pub fn add_user_to_group(&self, uuid: &str, group: &str) {
        self.get_or_create_user(uuid);
        if let Some(user) = self.store.write().users.get_mut(uuid) {
            user.groups.insert(group.trim().to_string());
        }
    }

    pub fn get_or_create_group(&self, group: &str) {
        let group = group.trim();
        if group.is_empty() {
            return;
        }
        self.store
            .write()
            .groups
            .entry(group.to_string())
            .or_insert_with(|| PermissionGroup {
                name: group.to_string(),
                ..Default::default()
            });
    }

    pub fn add_group_node(&self, group: &str, node: &str) {
        self.get_or_create_group(group);
        if let Some(group) = self.store.write().groups.get_mut(group.trim()) {
            group.nodes.insert(node.trim().to_string());
        }
    }

    pub fn add_group_parent(&self, group: &str, parent: &str) {
        self.get_or_create_group(group);
        self.get_or_create_group(parent);
        if let Some(group) = self.store.write().groups.get_mut(group.trim()) {
            group.parents.insert(parent.trim().to_string());
        }
    }

    pub fn delete_group(&self, group: &str) {
        let group = group.trim();
        if group == "default" || group.is_empty() {
            return;
        }
        let mut store = self.store.write();
        store.groups.remove(group);
        for user in store.users.values_mut() {
            user.groups.remove(group);
        }
        for existing in store.groups.values_mut() {
            existing.parents.remove(group);
        }
    }

    pub fn has(&self, uuid: &str, node: &str) -> bool {
        let decisions = self.effective_decisions(uuid);
        check_node(&decisions, node)
    }

    pub fn allowed_rules(&self, uuid: &str) -> Vec<String> {
        self.effective_decisions(uuid)
            .into_iter()
            .filter_map(|(node, allow)| allow.then_some(node))
            .collect()
    }

    pub fn denied_rules(&self, uuid: &str) -> Vec<String> {
        self.effective_decisions(uuid)
            .into_iter()
            .filter_map(|(node, allow)| (!allow).then_some(node))
            .collect()
    }

    fn effective_decisions(&self, uuid: &str) -> HashMap<String, bool> {
        let store = self.store.read();
        let Some(user) = store.users.get(uuid) else {
            return HashMap::new();
        };
        let mut decisions = HashMap::new();
        let mut visited = HashSet::new();
        for group in &user.groups {
            apply_group(group, &store, &mut visited, &mut decisions);
        }
        apply_raw_nodes(&user.nodes, &mut decisions);
        decisions
    }
}

fn apply_group(
    group_name: &str,
    store: &PermissionStore,
    visited: &mut HashSet<String>,
    decisions: &mut HashMap<String, bool>,
) {
    if !visited.insert(group_name.to_string()) {
        return;
    }
    let Some(group) = store.groups.get(group_name) else {
        return;
    };
    for parent in &group.parents {
        apply_group(parent, store, visited, decisions);
    }
    apply_raw_nodes(&group.nodes, decisions);
}

fn apply_raw_nodes(raw_nodes: &HashSet<String>, decisions: &mut HashMap<String, bool>) {
    for raw in raw_nodes {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let (node, allow) = raw
            .strip_prefix('-')
            .map(|node| (node.trim(), false))
            .unwrap_or((raw, true));
        if node.is_empty() {
            continue;
        }
        if matches!(decisions.get(node), Some(false)) {
            continue;
        }
        decisions.insert(node.to_string(), allow);
    }
}

fn check_node(decisions: &HashMap<String, bool>, node: &str) -> bool {
    let node = node.trim();
    if node.is_empty() {
        return false;
    }
    if let Some(value) = decisions.get(node) {
        return *value;
    }
    let mut current = node;
    while let Some((prefix, _)) = current.rsplit_once('.') {
        let wildcard = format!("{prefix}.*");
        if let Some(value) = decisions.get(&wildcard) {
            return *value;
        }
        current = prefix;
    }
    decisions.get(nodes::ALL).copied().unwrap_or(false)
}

fn load_permissions(path: &Path) -> Result<PermissionStore> {
    if !path.exists() {
        return Ok(PermissionStore::default());
    }
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading permissions {}", path.display()))?;
    let mut store = PermissionStore::default();
    let mut current_group: Option<String> = None;
    let mut current_user: Option<String> = None;
    let mut in_groups = false;
    let mut in_users = false;

    for line in text.lines() {
        let trimmed = line.trim();
        match trimmed {
            "<Groups>" => {
                in_groups = true;
                in_users = false;
            }
            "</Groups>" => {
                in_groups = false;
                current_group = None;
            }
            "<Users>" => {
                in_users = true;
                in_groups = false;
            }
            "</Users>" => {
                in_users = false;
                current_user = None;
            }
            "</Group>" => {
                if in_groups {
                    current_group = None;
                }
            }
            "</User>" => current_user = None,
            _ => {
                if trimmed.starts_with("<Group ") {
                    if let Some(name) = attr(trimmed, "name") {
                        if in_groups {
                            current_group = Some(name.clone());
                            store.groups.entry(name.clone()).or_insert(PermissionGroup {
                                name,
                                ..Default::default()
                            });
                        } else if in_users {
                            if let Some(user) =
                                current_user.as_ref().and_then(|u| store.users.get_mut(u))
                            {
                                user.groups.insert(name);
                            }
                        }
                    }
                } else if trimmed.starts_with("<User ") {
                    if let Some(uuid) = attr(trimmed, "uuid") {
                        current_user = Some(uuid.clone());
                        store.users.entry(uuid.clone()).or_insert(PermissionUser {
                            uuid,
                            ..Default::default()
                        });
                    }
                } else if trimmed.starts_with("<Parent ") {
                    if let (Some(group), Some(parent)) =
                        (current_group.as_ref(), attr(trimmed, "name"))
                    {
                        if let Some(group) = store.groups.get_mut(group) {
                            group.parents.insert(parent);
                        }
                    }
                } else if trimmed.starts_with("<Node ") {
                    if let Some(node) = attr(trimmed, "value") {
                        if let Some(group) =
                            current_group.as_ref().and_then(|g| store.groups.get_mut(g))
                        {
                            group.nodes.insert(node.clone());
                        } else if let Some(user) =
                            current_user.as_ref().and_then(|u| store.users.get_mut(u))
                        {
                            user.nodes.insert(node);
                        }
                    }
                }
            }
        }
    }
    Ok(store)
}

fn save_permissions(path: &Path, store: &PermissionStore) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut out = String::from("<?xml version=\"1.0\"?>\n<Permissions>\n  <Groups>\n");
    for group in store.groups.values() {
        out.push_str(&format!("    <Group name=\"{}\">\n", escape(&group.name)));
        for parent in &group.parents {
            out.push_str(&format!("      <Parent name=\"{}\" />\n", escape(parent)));
        }
        for node in &group.nodes {
            out.push_str(&format!("      <Node value=\"{}\" />\n", escape(node)));
        }
        out.push_str("    </Group>\n");
    }
    out.push_str("  </Groups>\n  <Users>\n");
    for user in store.users.values() {
        out.push_str(&format!("    <User uuid=\"{}\">\n", escape(&user.uuid)));
        for group in &user.groups {
            out.push_str(&format!("      <Group name=\"{}\" />\n", escape(group)));
        }
        for node in &user.nodes {
            out.push_str(&format!("      <Node value=\"{}\" />\n", escape(node)));
        }
        out.push_str("    </User>\n");
    }
    out.push_str("  </Users>\n</Permissions>\n");
    fs::write(path, out)?;
    Ok(())
}

fn attr(line: &str, name: &str) -> Option<String> {
    let needle = format!("{name}=\"");
    let start = line.find(&needle)? + needle.len();
    let end = line[start..].find('"')? + start;
    Some(unescape(&line[start..end]))
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn unescape(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deny_wins_and_wildcards_work() {
        let manager = PermissionManager::default();
        manager.ensure_defaults();
        manager.get_or_create_user("u");
        manager.add_user_node("u", "basis.test.*");
        manager.add_user_node("u", "-basis.test.blocked");
        assert!(manager.has("u", "basis.test.allowed"));
        assert!(!manager.has("u", "basis.test.blocked"));
    }
}
