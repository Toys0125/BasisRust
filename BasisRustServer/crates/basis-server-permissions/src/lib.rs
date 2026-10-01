use anyhow::{bail, ensure, Context, Result};
use parking_lot::{Mutex, RwLock};
use quick_xml::{
    events::{BytesStart, Event},
    Reader,
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

pub use basis_protocol::permissions::nodes;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionUser {
    pub uuid: String,
    pub nodes: HashSet<String>,
    pub groups: HashSet<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionGroup {
    pub name: String,
    pub nodes: HashSet<String>,
    pub parents: HashSet<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionStore {
    pub users: HashMap<String, PermissionUser>,
    pub groups: HashMap<String, PermissionGroup>,
    pub seeded_defaults: HashMap<String, HashSet<String>>,
}

#[derive(Debug)]
struct Pending {
    file_support: bool,
    dirty_since: Option<Instant>,
    changes: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct PermissionManager {
    path: Arc<RwLock<PathBuf>>,
    store: Arc<RwLock<PermissionStore>>,
    pending: Arc<Mutex<Pending>>,
}

impl Default for PermissionManager {
    fn default() -> Self {
        Self {
            path: Arc::new(RwLock::new(PathBuf::from("permissions.xml"))),
            store: Arc::new(RwLock::new(PermissionStore::default())),
            pending: Arc::new(Mutex::new(Pending {
                file_support: true,
                dirty_since: None,
                changes: Vec::new(),
            })),
        }
    }
}

impl PermissionManager {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let manager = Self::default();
        manager.set_xml_path(path);
        manager
    }

    pub fn get_xml_path(&self) -> PathBuf {
        self.path.read().clone()
    }
    pub fn set_xml_path(&self, path: impl Into<PathBuf>) {
        *self.path.write() = path.into();
    }

    /// Memory-only operation also skips imports, matching BasisVR's disk-disabled startup.
    pub fn set_file_support(&self, enabled: bool) {
        self.pending.lock().file_support = enabled;
    }

    pub fn load_from_xml(&self) -> Result<()> {
        self.load_from_xml_path(self.get_xml_path())
    }

    pub fn load_from_xml_path(&self, path: impl Into<PathBuf>) -> Result<()> {
        if !self.pending.lock().file_support {
            return Ok(());
        }
        let path = path.into();
        let loaded = load_permissions(&path)?;
        let mut store = self.store.write();
        let changed = *store != loaded;
        *store = loaded;
        let mut pending = self.pending.lock();
        pending.dirty_since = None;
        if changed {
            enqueue(&mut pending.changes, None);
        }
        self.set_xml_path(path);
        Ok(())
    }

    pub fn save_to_xml(&self) -> Result<()> {
        self.save_to_xml_path(self.get_xml_path())
    }

    pub fn save_to_xml_path(&self, path: impl AsRef<Path>) -> Result<()> {
        // Holding the store read lock prevents a concurrent edit being marked clean by this save.
        let store = self.store.read();
        let mut pending = self.pending.lock();
        if !pending.file_support {
            return Ok(());
        }
        save_permissions(path.as_ref(), &store)?;
        pending.dirty_since = None;
        Ok(())
    }

    pub fn flush_pending_save(&self) -> Result<()> {
        self.save_pending(false)
    }

    /// The server tick drives the same 750 ms trailing-edge debounce as BasisVR.
    pub fn save_if_due(&self) -> Result<()> {
        self.save_pending(true)
    }

    fn save_pending(&self, require_due: bool) -> Result<()> {
        let store = self.store.read();
        let mut pending = self.pending.lock();
        let Some(since) = pending.dirty_since else {
            return Ok(());
        };
        if !pending.file_support || (require_due && since.elapsed() < Duration::from_millis(750)) {
            return Ok(());
        }
        save_permissions(&self.get_xml_path(), &store)?;
        pending.dirty_since = None;
        Ok(())
    }

    /// None means refresh all players; Some(uuid) means refresh that player.
    pub fn take_changes(&self) -> Vec<Option<String>> {
        std::mem::take(&mut self.pending.lock().changes)
    }

    fn mutate(&self, uuid: Option<&str>, f: impl FnOnce(&mut PermissionStore) -> bool) {
        let mut store = self.store.write();
        if f(&mut store) {
            let mut pending = self.pending.lock();
            pending.dirty_since = Some(Instant::now());
            enqueue(&mut pending.changes, uuid.map(str::to_string));
        }
    }

    pub fn ensure_defaults(&self) {
        self.mutate(None, |store| {
            let mut changed = seed_legacy_group(
                store,
                "default",
                None,
                basis_protocol::permissions::DEFAULT_GROUP_NODES,
                LEGACY_DEFAULT_GROUP_NODES,
            );
            changed |= seed_legacy_group(
                store,
                "moderator",
                Some("default"),
                basis_protocol::permissions::MODERATOR_GROUP_NODES,
                LEGACY_MODERATOR_GROUP_NODES,
            );
            changed |= seed_legacy_group(
                store,
                "admin",
                Some("moderator"),
                &[nodes::ALL],
                &[nodes::ALL],
            );
            changed
        });
    }

    pub fn snapshot(&self) -> PermissionStore {
        self.store.read().clone()
    }

    pub fn get_or_create_user(&self, uuid: &str) {
        if uuid.trim().is_empty() {
            return;
        }
        self.mutate(Some(uuid), |store| {
            let created = key(&store.users, uuid).is_none();
            user_mut(store, uuid);
            created
        });
    }

    pub fn add_user_node(&self, uuid: &str, node: &str) {
        if uuid.trim().is_empty() || node.trim().is_empty() {
            return;
        }
        self.mutate(Some(uuid), |store| {
            let created = key(&store.users, uuid).is_none();
            insert(&mut user_mut(store, uuid).nodes, node.trim()) | created
        });
    }

    pub fn remove_user_node(&self, uuid: &str, node: &str) {
        self.mutate(Some(uuid), |store| {
            let Some(k) = key(&store.users, uuid) else {
                return false;
            };
            remove(&mut store.users.get_mut(&k).unwrap().nodes, node.trim())
        });
    }

    pub fn add_user_to_group(&self, uuid: &str, group: &str) {
        if uuid.trim().is_empty() || group.trim().is_empty() {
            return;
        }
        self.mutate(Some(uuid), |store| {
            let created = key(&store.users, uuid).is_none();
            insert(&mut user_mut(store, uuid).groups, group.trim()) | created
        });
    }

    pub fn remove_user_from_group(&self, uuid: &str, group: &str) {
        self.mutate(Some(uuid), |store| {
            let Some(k) = key(&store.users, uuid) else {
                return false;
            };
            remove(&mut store.users.get_mut(&k).unwrap().groups, group.trim())
        });
    }

    pub fn get_or_create_group(&self, group: &str) {
        if group.trim().is_empty() {
            return;
        }
        self.mutate(None, |store| {
            let created = key(&store.groups, group.trim()).is_none();
            group_mut(store, group.trim());
            created
        });
    }

    pub fn add_group_node(&self, group: &str, node: &str) {
        if group.trim().is_empty() || node.trim().is_empty() {
            return;
        }
        self.mutate(None, |store| {
            let created = key(&store.groups, group.trim()).is_none();
            insert(&mut group_mut(store, group.trim()).nodes, node.trim()) | created
        });
    }

    pub fn remove_group_node(&self, group: &str, node: &str) {
        self.mutate(None, |store| {
            let Some(k) = key(&store.groups, group.trim()) else {
                return false;
            };
            remove(&mut store.groups.get_mut(&k).unwrap().nodes, node.trim())
        });
    }

    pub fn add_group_parent(&self, group: &str, parent: &str) {
        if group.trim().is_empty() || parent.trim().is_empty() {
            return;
        }
        self.mutate(None, |store| {
            let created = key(&store.groups, group.trim()).is_none();
            insert(&mut group_mut(store, group.trim()).parents, parent.trim()) | created
        });
    }

    pub fn remove_group_parent(&self, group: &str, parent: &str) {
        self.mutate(None, |store| {
            let Some(k) = key(&store.groups, group.trim()) else {
                return false;
            };
            remove(
                &mut store.groups.get_mut(&k).unwrap().parents,
                parent.trim(),
            )
        });
    }

    pub fn delete_group(&self, group: &str) {
        self.mutate(None, |store| {
            let Some(k) = key(&store.groups, group.trim()) else {
                return false;
            };
            store.groups.remove(&k);
            for user in store.users.values_mut() {
                remove(&mut user.groups, &k);
            }
            for group in store.groups.values_mut() {
                remove(&mut group.parents, &k);
            }
            true
        });
    }

    pub fn has(&self, uuid: &str, node: &str) -> bool {
        check_node(&self.effective_decisions(uuid), node)
    }

    pub fn is_in_group(&self, uuid: &str, group: &str) -> bool {
        if uuid.trim().is_empty() || group.trim().is_empty() {
            return false;
        }
        let store = self.store.read();
        let mut visited = HashSet::new();
        if let Some(k) = key(&store.users, uuid) {
            store.users[&k]
                .groups
                .iter()
                .any(|g| inherits_group(g, group.trim(), &store, &mut visited))
        } else {
            inherits_group("default", group.trim(), &store, &mut visited)
        }
    }

    pub fn allowed_rules(&self, uuid: &str) -> Vec<String> {
        self.effective_decisions(uuid)
            .into_iter()
            .filter_map(|(n, allow)| allow.then_some(n))
            .collect()
    }

    pub fn denied_rules(&self, uuid: &str) -> Vec<String> {
        self.effective_decisions(uuid)
            .into_iter()
            .filter_map(|(n, allow)| (!allow).then_some(n))
            .collect()
    }

    fn effective_decisions(&self, uuid: &str) -> HashMap<String, bool> {
        let store = self.store.read();
        let mut decisions = HashMap::new();
        let mut visited = HashSet::new();
        if let Some(k) = key(&store.users, uuid) {
            let user = &store.users[&k];
            for group in &user.groups {
                apply_group(group, &store, &mut visited, &mut decisions);
            }
            apply_raw_nodes(&user.nodes, &mut decisions);
        } else {
            apply_group("default", &store, &mut visited, &mut decisions);
        }
        decisions
    }
}

// Keep operator spellings in public snapshots and XML; compare identities independently.
fn same(a: &str, b: &str) -> bool {
    basis_protocol::permissions::ordinal_ignore_case_equal(a, b)
}
fn key<T>(map: &HashMap<String, T>, wanted: &str) -> Option<String> {
    if map.contains_key(wanted) {
        return Some(wanted.to_string());
    }
    map.keys().find(|k| same(k, wanted)).cloned()
}
fn contains(set: &HashSet<String>, value: &str) -> bool {
    set.contains(value) || set.iter().any(|v| same(v, value))
}
fn insert(set: &mut HashSet<String>, value: &str) -> bool {
    if contains(set, value) {
        false
    } else {
        set.insert(value.to_string())
    }
}
fn remove(set: &mut HashSet<String>, value: &str) -> bool {
    if set.remove(value) {
        return true;
    }
    let Some(k) = set.iter().find(|v| same(v, value)).cloned() else {
        return false;
    };
    set.remove(&k)
}
fn enqueue(changes: &mut Vec<Option<String>>, change: Option<String>) {
    if changes.iter().any(|old| match (old, &change) {
        (None, None) => true,
        (Some(a), Some(b)) => same(a, b),
        _ => false,
    }) {
        return;
    }
    changes.push(change);
}
fn user_mut<'a>(store: &'a mut PermissionStore, uuid: &str) -> &'a mut PermissionUser {
    let k = key(&store.users, uuid).unwrap_or_else(|| uuid.to_string());
    store.users.entry(k).or_insert_with(|| PermissionUser {
        uuid: uuid.to_string(),
        groups: HashSet::from(["default".to_string()]),
        ..Default::default()
    })
}
fn group_mut<'a>(store: &'a mut PermissionStore, name: &str) -> &'a mut PermissionGroup {
    let k = key(&store.groups, name).unwrap_or_else(|| name.to_string());
    store.groups.entry(k).or_insert_with(|| PermissionGroup {
        name: name.to_string(),
        ..Default::default()
    })
}
fn seed_group(
    store: &mut PermissionStore,
    name: &str,
    parent: Option<&str>,
    nodes: &[&str],
) -> bool {
    let seeded_key = key(&store.seeded_defaults, name).unwrap_or_else(|| name.to_string());
    let mut changed = !store.seeded_defaults.contains_key(&seeded_key);
    let seeded = store.seeded_defaults.entry(seeded_key).or_default();
    let group_key = key(&store.groups, name).unwrap_or_else(|| name.to_string());
    if !store.groups.contains_key(&group_key) {
        let mut group = PermissionGroup {
            name: name.to_string(),
            ..Default::default()
        };
        if let Some(parent) = parent {
            group.parents.insert(parent.to_string());
        }
        store.groups.insert(group_key.clone(), group);
        seeded.clear();
        changed = true;
    }
    let group = store.groups.get_mut(&group_key).unwrap();
    for node in nodes {
        if insert(seeded, node) {
            changed = true;
            if !contains(&group.nodes, &format!("-{node}")) {
                insert(&mut group.nodes, node);
            }
        }
    }
    changed
}

const LEGACY_DEFAULT_GROUP_NODES: &[&str] = &[
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
const LEGACY_MODERATOR_GROUP_NODES: &[&str] = &[
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
    nodes::PERMISSIONS_VIEW,
    nodes::RESOURCE_LOCK_BYPASS_AVATAR,
    nodes::RESOURCE_LOCK_BYPASS_PROP,
    nodes::RESOURCE_LOCK_BYPASS_WORLD,
    nodes::RESOURCE_LOCK_BYPASS_SERVER,
];

fn seed_legacy_group(
    store: &mut PermissionStore,
    name: &str,
    parent: Option<&str>,
    nodes: &[&str],
    legacy_nodes: &[&str],
) -> bool {
    if key(&store.groups, name).is_none() {
        return seed_group(store, name, parent, nodes);
    }
    let mut history_added = false;
    if key(&store.seeded_defaults, name).is_none() {
        let seeded = store.seeded_defaults.entry(name.to_string()).or_default();
        for node in legacy_nodes {
            history_added |= insert(seeded, node);
        }
    }
    seed_group(store, name, parent, nodes) | history_added
}
fn inherits_group(
    name: &str,
    target: &str,
    store: &PermissionStore,
    visited: &mut HashSet<String>,
) -> bool {
    if same(name, target) {
        return true;
    }
    if !visited.insert(basis_protocol::permissions::ordinal_ignore_case_key(name)) {
        return false;
    }
    key(&store.groups, name).is_some_and(|k| {
        store.groups[&k]
            .parents
            .iter()
            .any(|p| inherits_group(p, target, store, visited))
    })
}
fn apply_group(
    name: &str,
    store: &PermissionStore,
    visited: &mut HashSet<String>,
    decisions: &mut HashMap<String, bool>,
) {
    if !visited.insert(basis_protocol::permissions::ordinal_ignore_case_key(name)) {
        return;
    }
    let Some(k) = key(&store.groups, name) else {
        return;
    };
    let group = &store.groups[&k];
    for parent in &group.parents {
        apply_group(parent, store, visited, decisions);
    }
    apply_raw_nodes(&group.nodes, decisions);
}
fn apply_raw_nodes(raw_nodes: &HashSet<String>, decisions: &mut HashMap<String, bool>) {
    for raw in raw_nodes {
        let raw = raw.trim();
        let (node, allow) = raw
            .strip_prefix('-')
            .map(|n| (n.trim(), false))
            .unwrap_or((raw, true));
        if node.is_empty() {
            continue;
        }
        let node = basis_protocol::permissions::ordinal_ignore_case_key(node);
        if matches!(decisions.get(&node), Some(false)) {
            continue;
        }
        decisions.insert(node, allow);
    }
}
fn check_node(decisions: &HashMap<String, bool>, node: &str) -> bool {
    let node = basis_protocol::permissions::ordinal_ignore_case_key(node.trim());
    if node.is_empty() {
        return false;
    }
    if let Some(value) = decisions.get(&node) {
        return *value;
    }
    let mut current = node.as_str();
    while let Some((prefix, _)) = current.rsplit_once('.') {
        if let Some(value) = decisions.get(&format!("{prefix}.*")) {
            return *value;
        }
        current = prefix;
    }
    decisions.get(nodes::ALL).copied().unwrap_or(false)
}

fn migrate(node: &str) -> String {
    if same(node, "basis.moderation.shout") {
        nodes::MODERATION_ANNOUNCE.to_string()
    } else if same(node, "-basis.moderation.shout") {
        format!("-{}", nodes::MODERATION_ANNOUNCE)
    } else {
        node.to_string()
    }
}

#[derive(Debug)]
struct Frame {
    tag: String,
    identity: Option<String>,
}

fn load_permissions(path: &Path) -> Result<PermissionStore> {
    if !path.exists() {
        return Ok(PermissionStore::default());
    }
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading permissions {}", path.display()))?;
    parse_permissions(text.trim_start_matches('\u{feff}'))
        .with_context(|| format!("parsing permissions {}", path.display()))
}
fn parse_permissions(text: &str) -> Result<PermissionStore> {
    ensure!(text.chars().all(|c| matches!(c as u32, 0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF)), "invalid XML character");
    let mut reader = Reader::from_str(text);
    reader.config_mut().check_comments = true;
    let mut seen_declaration = false;
    let mut store = PermissionStore::default();
    let mut stack: Vec<Frame> = Vec::new();
    let mut seen_root = false;
    loop {
        match reader.read_event()? {
            Event::Start(element) => {
                let frame = start_element(&reader, &element, &stack, &mut store, &mut seen_root)?;
                stack.push(frame);
            }
            Event::Empty(element) => {
                start_element(&reader, &element, &stack, &mut store, &mut seen_root)?;
            }
            Event::End(element) => {
                let frame = stack.pop().context("unexpected closing element")?;
                ensure!(
                    element.name().as_ref() == frame.tag.as_bytes(),
                    "mismatched closing element"
                );
            }
            Event::Text(text) => ensure!(
                text.xml_content()?.trim().is_empty(),
                "unexpected text in permissions XML"
            ),
            Event::DocType(_) => bail!("DTD is prohibited in permissions XML"),
            Event::CData(_) | Event::GeneralRef(_) => {
                bail!("unexpected content in permissions XML")
            }
            Event::Decl(_) => {
                ensure!(
                    !seen_root && !seen_declaration && stack.is_empty(),
                    "unexpected XML declaration"
                );
                seen_declaration = true;
            }
            Event::Comment(_) | Event::PI(_) => {}
            Event::Eof => break,
        }
    }
    ensure!(
        seen_root && stack.is_empty(),
        "missing or incomplete Permissions root"
    );
    Ok(store)
}
fn start_element(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    stack: &[Frame],
    store: &mut PermissionStore,
    seen_root: &mut bool,
) -> Result<Frame> {
    let tag = std::str::from_utf8(element.name().as_ref())?.to_string();
    let mut attributes = HashMap::new();
    for attribute in element.attributes() {
        let attribute = attribute?;
        ensure!(
            !attribute.value.contains(&b'<'),
            "unescaped < in XML attribute"
        );
        attributes.insert(
            std::str::from_utf8(attribute.key.as_ref())?.to_string(),
            attribute
                .decode_and_unescape_value(reader.decoder())?
                .into_owned(),
        );
    }
    let attr = |name: &str| -> Result<String> {
        let value = attributes
            .get(name)
            .with_context(|| format!("{tag} is missing {name}"))?;
        ensure!(!value.trim().is_empty(), "{tag} has empty {name}");
        Ok(value.clone())
    };
    let parent = stack.last().map(|f| f.tag.as_str()).unwrap_or("");
    let section = stack.get(1).map(|f| f.tag.as_str()).unwrap_or("");
    let mut identity = None;
    match (parent, tag.as_str()) {
        ("", "Permissions") => {
            ensure!(!*seen_root, "multiple XML roots");
            *seen_root = true;
        }
        ("Permissions", "Groups" | "Users" | "SeededDefaults") => {}
        ("Groups", "Group") => {
            let name = attr("name")?;
            if let Some(old) = key(&store.groups, &name) {
                store.groups.remove(&old);
            }
            store.groups.insert(
                name.clone(),
                PermissionGroup {
                    name: name.clone(),
                    ..Default::default()
                },
            );
            identity = Some(name);
        }
        ("Users", "User") => {
            let uuid = attr("uuid")?;
            if let Some(old) = key(&store.users, &uuid) {
                store.users.remove(&old);
            }
            store.users.insert(
                uuid.clone(),
                PermissionUser {
                    uuid: uuid.clone(),
                    ..Default::default()
                },
            );
            identity = Some(uuid);
        }
        ("SeededDefaults", "Group") => {
            let name = attr("name")?;
            let k = key(&store.seeded_defaults, &name).unwrap_or(name);
            store.seeded_defaults.entry(k.clone()).or_default();
            identity = Some(k);
        }
        ("User", "Group") if section == "Users" => {
            let user = stack
                .last()
                .and_then(|f| f.identity.as_ref())
                .context("missing user identity")?;
            insert(
                &mut store.users.get_mut(user).unwrap().groups,
                attr("name")?.trim(),
            );
        }
        ("Group", "Parent") if section == "Groups" => {
            let group = stack
                .last()
                .and_then(|f| f.identity.as_ref())
                .context("missing group identity")?;
            insert(
                &mut store.groups.get_mut(group).unwrap().parents,
                attr("name")?.trim(),
            );
        }
        ("Group" | "User", "Node") => {
            let k = stack
                .last()
                .and_then(|f| f.identity.as_ref())
                .context("missing node owner")?;
            let node = migrate(attr("value")?.trim());
            let set = match section {
                "Groups" => &mut store.groups.get_mut(k).context("missing group")?.nodes,
                "Users" => &mut store.users.get_mut(k).context("missing user")?.nodes,
                "SeededDefaults" => store
                    .seeded_defaults
                    .get_mut(k)
                    .context("missing seed group")?,
                _ => bail!("Node outside permission group/user"),
            };
            insert(set, &node);
        }
        _ => bail!("unexpected {tag} inside {parent}"),
    }
    Ok(Frame { tag, identity })
}

fn save_permissions(path: &Path, store: &PermissionStore) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let mut out = String::from("<?xml version=\"1.0\"?>\n<Permissions>\n  <Groups>\n");
    let mut groups: Vec<_> = store.groups.values().collect();
    groups.sort_by(|a, b| a.name.cmp(&b.name));
    for group in groups {
        out.push_str(&format!("    <Group name=\"{}\">\n", escape(&group.name)));
        write_set(&mut out, "Parent", "name", &group.parents);
        write_set(&mut out, "Node", "value", &group.nodes);
        out.push_str("    </Group>\n");
    }
    out.push_str("  </Groups>\n  <Users>\n");
    let mut users: Vec<_> = store.users.values().collect();
    users.sort_by(|a, b| a.uuid.cmp(&b.uuid));
    for user in users {
        out.push_str(&format!("    <User uuid=\"{}\">\n", escape(&user.uuid)));
        write_set(&mut out, "Group", "name", &user.groups);
        write_set(&mut out, "Node", "value", &user.nodes);
        out.push_str("    </User>\n");
    }
    out.push_str("  </Users>\n  <SeededDefaults>\n");
    let mut seeded: Vec<_> = store.seeded_defaults.iter().collect();
    seeded.sort_by_key(|(name, _)| *name);
    for (name, nodes) in seeded {
        out.push_str(&format!("    <Group name=\"{}\">\n", escape(name)));
        write_set(&mut out, "Node", "value", nodes);
        out.push_str("    </Group>\n");
    }
    out.push_str("  </SeededDefaults>\n</Permissions>\n");
    // Keep the previous complete file if writing fails or the process stops before replacement.
    let temporary = path.with_extension("xml.tmp");
    fs::write(&temporary, out)
        .with_context(|| format!("writing permissions {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("replacing permissions {}", path.display()))?;
    Ok(())
}
fn write_set(out: &mut String, tag: &str, attribute: &str, set: &HashSet<String>) {
    let mut values: Vec<_> = set.iter().collect();
    values.sort();
    for value in values {
        out.push_str(&format!(
            "      <{tag} {attribute}=\"{}\" />\n",
            escape(value)
        ));
    }
}
fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\r', "&#13;")
        .replace('\n', "&#10;")
        .replace('\t', "&#9;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "basis-permissions-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn path(&self) -> PathBuf {
            self.0.join("permissions.xml")
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn unknown_user_uses_implicit_default_without_materializing_entry() {
        let manager = PermissionManager::default();
        manager.ensure_defaults();
        assert!(manager.has(
            "unknown",
            basis_protocol::permissions::DEFAULT_GROUP_NODES[0]
        ));
        assert!(manager.is_in_group("unknown", "DEFAULT"));
        assert!(!manager.snapshot().users.contains_key("unknown"));
        assert!(!manager.is_in_group("", "default"));
    }

    #[test]
    fn xml_roundtrip_preserves_identifiers_seed_history_and_migrates_legacy_denies() {
        let fixture = Fixture::new();
        fs::write(fixture.path(), "\u{feff}<?xml version='1.0'?><Permissions><Groups><Group name='MiXeD &amp; &#x26;'><Parent name='Base'/><Node value='Basis.Test.*'/><Node value='-BASIS.TEST.BLOCKED'/></Group><Group name='Base'><Node value='basis.moderation.shout'/></Group></Groups><Users><User uuid='UUID'><Group name='mixed &amp; &#38;'/><Node value='-BASIS.MODERATION.SHOUT'/></User></Users><SeededDefaults><Group name='moderator'><Node value='basis.moderation.shout'/></Group></SeededDefaults></Permissions>").unwrap();
        let manager = PermissionManager::new(fixture.path());
        manager.load_from_xml().unwrap();
        assert!(manager.has("uuid", "BASIS.TEST.ALLOWED"));
        assert!(!manager.has("uuid", "basis.test.blocked"));
        assert!(!manager.has("uuid", nodes::MODERATION_ANNOUNCE));
        assert!(manager.is_in_group("uuid", "BASE"));
        assert_eq!(manager.snapshot().groups["MiXeD & &"].name, "MiXeD & &");
        assert!(
            manager.snapshot().seeded_defaults["moderator"].contains(nodes::MODERATION_ANNOUNCE)
        );
        manager.save_to_xml().unwrap();
        let other = PermissionManager::new(fixture.path());
        other.load_from_xml().unwrap();
        assert_eq!(manager.snapshot(), other.snapshot());
        assert!(!fs::read_to_string(fixture.path())
            .unwrap()
            .contains("moderation.shout"));
    }

    #[test]
    fn malformed_reloads_retain_the_previous_store_and_path() {
        let fixture = Fixture::new();
        let manager = PermissionManager::new(fixture.path());
        manager.ensure_defaults();
        manager.add_user_to_group("UUID", "admin");
        let snapshot = manager.snapshot();
        for malformed in [
            "",
            "<Wrong/>",
            "<Permissions><Groups>",
            "<Permissions><Groups></Permissions>",
            "<Permissions/><Permissions/>",
            "<?xml version='1.0'?><?xml version='1.0'?><Permissions/>",
            "<Permissions bad='<'/>",
            "<!-- bad -- comment --><Permissions/>",
            "<Permissions>\0</Permissions>",
            "<!DOCTYPE Permissions><Permissions/>",
            "<Permissions><Users><User><Node value='*'/></User></Users></Permissions>",
            "<Permissions><Groups><Group name='a' name='b'/></Groups></Permissions>",
            "<Permissions><Groups><Group name='&unknown;'/></Groups></Permissions>",
        ] {
            let broken_path = fixture.0.join("broken.xml");
            fs::write(&broken_path, malformed).unwrap();
            assert!(
                manager.load_from_xml_path(&broken_path).is_err(),
                "accepted {malformed}"
            );
            assert_eq!(manager.snapshot(), snapshot);
            assert_eq!(manager.get_xml_path(), fixture.path());
            assert!(manager.has("uuid", nodes::MODERATION_BAN));
        }
    }

    #[test]
    fn seeds_only_new_defaults_and_retains_deliberate_removals_and_denies() {
        let fixture = Fixture::new();
        fs::write(fixture.path(), "<Permissions><Groups><Group name='MODERATOR'><Node value='-BASIS.MODERATION.MUTE'/></Group></Groups><Users/><SeededDefaults><Group name='Moderator'><Node value='basis.moderation.ban'/></Group></SeededDefaults></Permissions>").unwrap();
        let manager = PermissionManager::new(fixture.path());
        manager.load_from_xml().unwrap();
        manager.ensure_defaults();
        manager.add_user_to_group("u", "moderator");
        assert!(!manager.has("u", nodes::MODERATION_BAN));
        assert!(!manager.has("u", nodes::MODERATION_MUTE));
        assert!(manager.has("u", nodes::MODERATION_RENAME));
        manager.remove_group_node("moderator", nodes::MODERATION_RENAME);
        manager.ensure_defaults();
        assert!(!manager.has("u", nodes::MODERATION_RENAME));
        manager.save_to_xml().unwrap();
        manager.load_from_xml().unwrap();
        manager.ensure_defaults();
        assert!(!manager.has("u", nodes::MODERATION_RENAME));
        assert_eq!(manager.snapshot().seeded_defaults["Moderator"].len(), 25);
        let fresh = PermissionManager::default();
        fresh.ensure_defaults();
        assert_eq!(fresh.snapshot().groups["default"].nodes.len(), 12);
        assert_eq!(fresh.snapshot().groups["moderator"].nodes.len(), 25);
        assert!(fresh.is_in_group("unknown", "default"));
    }

    #[test]
    fn upgrades_legacy_xml_without_restoring_removed_grants() {
        let fixture = Fixture::new();
        fs::write(
            fixture.path(),
            format!(
                "<Permissions><Groups><Group name='default'><Node value='{}'/></Group><Group name='moderator'><Node value='{}'/></Group></Groups><Users/></Permissions>",
                nodes::RESOURCE_LOAD_PROP,
                nodes::MODERATION_KICK,
            ),
        )
        .unwrap();
        let manager = PermissionManager::new(fixture.path());
        manager.load_from_xml().unwrap();
        manager.ensure_defaults();
        manager.add_user_to_group("u", "moderator");

        assert!(!manager.has("unknown", nodes::HELP));
        assert!(!manager.has("u", nodes::MODERATION_BAN));
        assert!(manager.has("u", nodes::MODERATION_KICK));
        assert!(manager.has("u", nodes::MODERATION_MUTE));
        assert!(manager.snapshot().seeded_defaults["moderator"].contains(nodes::MODERATION_BAN));

        manager.remove_group_node("moderator", nodes::MODERATION_MUTE);
        manager.ensure_defaults();
        assert!(!manager.has("u", nodes::MODERATION_MUTE));
        manager.save_to_xml().unwrap();

        let reloaded = PermissionManager::new(fixture.path());
        reloaded.load_from_xml().unwrap();
        reloaded.ensure_defaults();
        reloaded.add_user_to_group("u", "moderator");
        assert!(!reloaded.has("unknown", nodes::HELP));
        assert!(!reloaded.has("u", nodes::MODERATION_BAN));
        assert!(!reloaded.has("u", nodes::MODERATION_MUTE));
    }

    #[test]
    fn history_only_legacy_migration_is_dirty_and_persisted() {
        let fixture = Fixture::new();
        let node_elements = |nodes: &[&str]| {
            nodes
                .iter()
                .map(|node| format!("<Node value='{node}'/>"))
                .collect::<String>()
        };
        let xml = format!(
            "<Permissions><Groups><Group name='default'>{}</Group><Group name='moderator'>{}</Group><Group name='admin'><Node value='{}'/></Group></Groups><Users/><SeededDefaults><Group name='moderator'>{}</Group></SeededDefaults></Permissions>",
            node_elements(basis_protocol::permissions::DEFAULT_GROUP_NODES),
            node_elements(basis_protocol::permissions::MODERATOR_GROUP_NODES),
            nodes::ALL,
            node_elements(basis_protocol::permissions::MODERATOR_GROUP_NODES),
        );
        fs::write(fixture.path(), xml).unwrap();
        let manager = PermissionManager::new(fixture.path());
        manager.load_from_xml().unwrap();
        manager.ensure_defaults();

        assert_eq!(manager.take_changes(), vec![None]);
        assert!(manager.pending.lock().dirty_since.is_some());
        manager.flush_pending_save().unwrap();

        let reloaded = PermissionManager::new(fixture.path());
        reloaded.load_from_xml().unwrap();
        assert_eq!(reloaded.snapshot().seeded_defaults["default"].len(), 12);
        assert!(reloaded.snapshot().seeded_defaults["admin"].contains(nodes::ALL));
    }

    #[test]
    fn case_insensitive_mutations_and_group_queries_observe_inheritance_and_cycles() {
        let manager = PermissionManager::default();
        manager.ensure_defaults();
        manager.add_user_to_group("UUID", "ADMIN");
        manager.add_group_parent("moderator", "Admin");
        assert!(manager.is_in_group("uuid", "DEFAULT"));
        manager.add_user_node("uuid", "-BASIS.TEST.BLOCKED");
        manager.add_user_node("UUID", "basis.test.blocked");
        assert!(!manager.has("uUiD", "Basis.Test.Blocked"));
        assert_eq!(manager.snapshot().users.len(), 1);
        assert_eq!(manager.snapshot().users["UUID"].uuid, "UUID");
        manager.remove_user_node("Uuid", "-basis.test.blocked");
        assert!(manager.has("uuid", "BASIS.TEST.BLOCKED"));
        manager.add_user_to_group("uuid", "Undefined");
        assert!(manager.is_in_group("uuid", "undefined"));
        assert!(!manager.snapshot().groups.contains_key("Undefined"));
        manager.remove_user_from_group("UUID", "undefined");
        assert!(!manager.is_in_group("uuid", "Undefined"));
        manager.delete_group("DEFAULT");
        assert!(!manager.has("unknown", nodes::HELP));
        assert!(!manager.is_in_group("uuid", "default"));
        assert!(!manager.snapshot().groups["moderator"]
            .parents
            .contains("default"));
    }

    #[test]
    fn unicode_permission_names_use_ordinal_casing_for_memberships_and_denies() {
        let manager = PermissionManager::new("unused.xml");
        manager.set_file_support(false);
        manager.add_group_node("Σ", "CUSTOM.\u{1f80}");
        manager.add_user_to_group("user", "ς");
        assert!(manager.is_in_group("USER", "σ"));
        assert!(manager.has("USER", "custom.\u{1f88}"));
        manager.add_user_node("USER", "-custom.\u{1f88}");
        assert!(!manager.has("user", "custom.\u{1f80}"));
        manager.add_user_node("user", "custom.K");
        assert!(!manager.has("user", "custom.k"));
    }

    #[test]
    fn genuine_changes_drive_refresh_debounce_and_memory_only_persistence() {
        let fixture = Fixture::new();
        let manager = PermissionManager::new(fixture.path());
        manager.ensure_defaults();
        assert_eq!(manager.take_changes(), vec![None]);
        manager.ensure_defaults();
        manager.remove_user_node("absent", "node");
        manager.add_user_node("", "node");
        assert!(manager.take_changes().is_empty());
        manager.add_user_node("UUID", "basis.test");
        manager.add_user_node("uuid", "BASIS.TEST");
        assert_eq!(manager.take_changes(), vec![Some("UUID".into())]);
        manager.save_if_due().unwrap();
        assert!(!fixture.path().exists());
        // Advance the logical deadline without making this test sleep.
        manager.pending.lock().dirty_since = Some(Instant::now() - Duration::from_millis(751));
        manager.save_if_due().unwrap();
        assert!(fixture.path().exists());
        let persisted = fs::read_to_string(fixture.path()).unwrap();
        manager.set_file_support(false);
        manager.add_user_node("uuid", "basis.extra");
        manager.flush_pending_save().unwrap();
        manager.save_to_xml().unwrap();
        manager.load_from_xml().unwrap();
        assert!(manager.has("uuid", "basis.extra"));
        assert_eq!(fs::read_to_string(fixture.path()).unwrap(), persisted);
        manager.set_file_support(true);
        manager.flush_pending_save().unwrap();
        assert_ne!(fs::read_to_string(fixture.path()).unwrap(), persisted);
    }

    #[test]
    fn failed_save_remains_pending_for_retry() {
        let fixture = Fixture::new();
        let blocked = fixture.0.join("not-a-directory");
        fs::write(&blocked, "file").unwrap();
        let manager = PermissionManager::new(blocked.join("permissions.xml"));
        manager.add_user_node("u", "basis.test");
        assert!(manager.flush_pending_save().is_err());
        assert!(manager.pending.lock().dirty_since.is_some());
        manager.set_xml_path(fixture.path());
        manager.flush_pending_save().unwrap();
        assert!(manager.pending.lock().dirty_since.is_none());
    }
}
