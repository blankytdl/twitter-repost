//! Host-native seen-post store: an in-memory working set backed by an atomic local JSON file.
//! Mount the configured data directory as persistent storage on the hosting server.
//! Single-writer assumed; this module intentionally has no GitHub/runtime API dependency.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    io,
    path::{Path, PathBuf},
};
use tokio::{io::AsyncWriteExt, time::Instant};
use tracing::{info, warn};

#[derive(Serialize, Deserialize, Default)]
struct State {
    #[serde(default)]
    ids: Vec<String>,
    #[serde(default)]
    subs: Vec<String>,
}

pub struct SeenStore {
    order: VecDeque<String>,
    set: HashSet<String>,
    subs: HashSet<String>,
    dirty: bool,
    path: PathBuf,
    cap: usize,
    last_flush: Option<Instant>,
}

impl SeenStore {
    pub fn new(path: PathBuf, cap: usize) -> Self {
        Self {
            order: VecDeque::new(),
            set: HashSet::new(),
            subs: HashSet::new(),
            dirty: false,
            path,
            cap,
            last_flush: None,
        }
    }

    /// Opens the host-local state file. Existing files are validated strictly:
    /// a corrupt or unreadable state must stop startup rather than silently
    /// starting with an empty seen set and flooding the destination with duplicates.
    pub async fn open(path: PathBuf, cap: usize) -> Result<Self> {
        let mut store = Self::new(path, cap);

        if let Some(parent) = store.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("cannot create state directory {}", parent.display()))?;
        }

        match tokio::fs::read(&store.path).await {
            Ok(bytes) => {
                let state: State = serde_json::from_slice(&bytes)
                    .with_context(|| format!("state file {} is corrupt; refusing to start with empty history", store.path.display()))?;
                store.apply(state);
                info!(ids = store.order.len(), path = %store.path.display(), "state restored from host storage");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Create the initial file now, so an invalid/unwritable mount is detected
                // before the polling loop starts and before any post can be sent.
                let bytes = store.snapshot()?;
                write_file(&store.path, &bytes)
                    .await
                    .with_context(|| format!("cannot initialize host state file {}", store.path.display()))?;
                info!(path = %store.path.display(), "initialized empty state on host storage");
            }
            Err(e) => {
                return Err(e).with_context(|| format!("cannot read host state file {}", store.path.display()));
            }
        }

        Ok(store)
    }

    fn apply(&mut self, state: State) {
        for id in state.ids {
            self.insert_inner(id);
        }
        self.subs.extend(state.subs.into_iter().map(|s| s.to_ascii_lowercase()));
    }

    /// Kept for unit tests and explicit local reloads.
    pub async fn load_local(&mut self) -> bool {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => match serde_json::from_slice::<State>(&bytes) {
                Ok(state) => {
                    self.apply(state);
                    info!(ids = self.order.len(), path = %self.path.display(), "state loaded from local file");
                    !self.order.is_empty()
                }
                Err(e) => {
                    warn!("local state file unreadable: {e}");
                    false
                }
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => {
                warn!("local state file could not be read: {e}");
                false
            }
        }
    }

    fn insert_inner(&mut self, id: String) -> bool {
        if self.set.contains(&id) {
            return false;
        }
        self.set.insert(id.clone());
        self.order.push_back(id);
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }

    pub fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }

    pub fn insert(&mut self, id: &str) {
        if self.insert_inner(id.to_string()) {
            self.dirty = true;
        }
    }

    #[allow(dead_code)]
    pub fn sub_initialized(&self, sub: &str) -> bool {
        self.subs.contains(&sub.to_ascii_lowercase())
    }

    /// True if this account was already seeded/running for this channel.
    pub fn sub_known(&self, chat: &str, sub: &str) -> bool {
        self.subs.contains(&format!("{chat}|{sub}").to_ascii_lowercase())
    }

    pub fn mark_sub(&mut self, sub: &str) {
        if self.subs.insert(sub.to_ascii_lowercase()) {
            self.dirty = true;
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        let mut subs: Vec<&str> = self.subs.iter().map(String::as_str).collect();
        subs.sort_unstable();
        Ok(serde_json::to_vec(&State {
            ids: self.order.iter().cloned().collect(),
            subs: subs.into_iter().map(String::from).collect(),
        })?)
    }

    /// Flush only if dirty AND the last successful flush is older than the configured minimum.
    pub async fn flush_if_due(&mut self, min: std::time::Duration) {
        if !self.dirty {
            return;
        }
        if self.last_flush.is_some_and(|t| t.elapsed() < min) {
            return;
        }
        self.flush().await;
    }

    /// Writes to the host-mounted state file using a temporary file + atomic rename.
    /// On failure the state remains dirty and will be retried on the next flush.
    pub async fn flush(&mut self) {
        if !self.dirty {
            return;
        }
        let bytes = match self.snapshot() {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!("cannot serialize state: {e}");
                return;
            }
        };
        match write_file(&self.path, &bytes).await {
            Ok(()) => {
                self.dirty = false;
                self.last_flush = Some(Instant::now());
                info!(ids = self.order.len(), path = %self.path.display(), "state saved to host storage");
            }
            Err(e) => {
                warn!(path = %self.path.display(), "host state save failed (will retry): {e:#}");
            }
        }
    }
}

async fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(dir) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("cannot create {}", dir.display()))?;
    }

    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = tokio::fs::File::create(&tmp)
        .await
        .with_context(|| format!("cannot create temporary state file {}", tmp.display()))?;
    file.write_all(bytes).await.context("cannot write temporary state file")?;
    file.sync_all().await.context("cannot sync temporary state file")?;
    drop(file);

    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("cannot atomically replace state file {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("xtb-test-{}-{}.json", std::process::id(), name))
    }

    #[test]
    fn sub_known_is_case_insensitive_for_channel_and_sub() {
        let mut s = SeenStore::new(tmp("case"), 10);
        assert!(!s.sub_known("@AnimeChannel", "Anime"));
        s.mark_sub(&crate::scheduler::sub_key("@AnimeChannel", "Anime"));
        assert!(s.sub_known("@AnimeChannel", "Anime"));
        assert!(s.sub_known("@animechannel", "ANIME"));
        assert!(!s.sub_known("@AnimeChannel", "manga"));
    }

    #[tokio::test]
    async fn dedup_and_cap() {
        let mut s = SeenStore::new(tmp("cap"), 3);
        for id in ["a", "b", "c", "a", "d"] {
            s.insert(id);
        }
        assert!(!s.contains("a"));
        assert!(s.contains("b") && s.contains("c") && s.contains("d"));
    }

    #[tokio::test]
    async fn local_roundtrip_simulates_restart() {
        let p = tmp("rt");
        let _ = std::fs::remove_file(&p);
        let mut s = SeenStore::new(p.clone(), 100);
        s.insert("1700000000000000001");
        s.mark_sub("Foo");
        s.flush().await;

        let mut s2 = SeenStore::open(p.clone(), 100).await.unwrap();
        assert!(s2.contains("1700000000000000001"));
        assert!(s2.sub_initialized("foo"));
        assert!(!s2.contains("t3_y"));
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn corrupt_host_state_refuses_to_start() {
        let p = tmp("corrupt");
        tokio::fs::write(&p, b"{not-json").await.unwrap();
        let err = SeenStore::open(p.clone(), 100).await.err().unwrap();
        assert!(err.to_string().contains("corrupt"));
        let _ = std::fs::remove_file(&p);
    }

    #[tokio::test]
    async fn open_creates_missing_directory_and_state_file() {
        let p = std::env::temp_dir()
            .join(format!("xtb-test-{}-nested", std::process::id()))
            .join("seen.json");
        let _ = tokio::fs::remove_dir_all(p.parent().unwrap()).await;
        let _store = SeenStore::open(p.clone(), 100).await.unwrap();
        assert!(tokio::fs::metadata(&p).await.is_ok());
        let _ = tokio::fs::remove_dir_all(p.parent().unwrap()).await;
    }
}