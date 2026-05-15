use anyhow::Result;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BasisData {
    pub name: String,
    pub json_payload: Value,
}

#[derive(Debug, Clone, Default)]
pub struct PersistentDatabase {
    path: Option<PathBuf>,
    data: Arc<RwLock<HashMap<String, BasisData>>>,
}

impl PersistentDatabase {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn file_backed(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn load(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if !path.exists() {
            return Ok(());
        }
        let text = fs::read_to_string(path)?;
        let values: Vec<BasisData> = serde_json::from_str(&text)?;
        let mut data = self.data.write();
        data.clear();
        for value in values {
            data.insert(value.name.clone(), value);
        }
        Ok(())
    }

    pub fn add_or_update(&self, value: BasisData) {
        self.data.write().insert(value.name.clone(), value);
    }

    pub fn get(&self, name: &str) -> Option<BasisData> {
        self.data.read().get(name).cloned()
    }

    pub fn shutdown(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let values: Vec<_> = self.data.read().values().cloned().collect();
        fs::write(path, serde_json::to_string_pretty(&values)?)?;
        Ok(())
    }
}
