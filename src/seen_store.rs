//! Seen-post store: in-memory working set, local JSON file, and a durable
//! backup in a dedicated GitHub branch (Contents API). Single-writer assumed.
use crate::config::GithubSettings;
use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::{Client, Method, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashSet, VecDeque},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::sleep;
use tracing::{info, warn};

const FILE_PATH: &str = "seen_posts.json";

#[derive(Serialize)]
struct StateRef<'a> {
    ids: &'a VecDeque<String>,
    subs: Vec<&'a str>,
}

#[derive(Serialize, Deserialize, Default)]
struct State {
    #[serde(default)]
    ids: Vec<String>,
    #[serde(default)]
    subs: Vec<String>,
}

enum PutErr {
    Conflict,
    Other(anyhow::Error),
}

struct GitHub {
    client: Client,
    s: GithubSettings,
    branch_ready: bool,
}

impl GitHub {
    fn rb(&self, m: Method, path: &str) -> RequestBuilder {
        self.rb_accept(m, path, "application/vnd.github+json")
    }

    fn rb_accept(&self, m: Method, path: &str, accept: &str) -> RequestBuilder {
        self.client
            .request(m, format!("{}{}", self.s.api_base, path))
            .bearer_auth(&self.s.token)
            .header("Accept", accept)
            .header("X-GitHub-Api-Version", "2022-11-28")
            .timeout(Duration::from_secs(30))
    }

    async fn get_file(&self) -> Result<Option<(String, Vec<u8>)>> {
        let r = self
            .rb(Method::GET, &format!("/repos/{}/contents/{}", self.s.repo, FILE_PATH))
            .query(&[("ref", self.s.branch.as_str())])
            .send()
            .await
            .map_err(|e| anyhow!(e.without_url()))?;
        match r.status().as_u16() {
            200 => {
                let v: Value = r.json().await?;
                let sha = v["sha"].as_str().ok_or_else(|| anyhow!("GitHub response lacks sha"))?.to_string();
                // Files over 1 MB come back with an empty body and encoding "none": fetch the raw bytes instead
                // (otherwise a big state file would be read as empty and the bot would re-send old posts).
                let content_str = v["content"].as_str();
                if v["encoding"].as_str() == Some("none") || (content_str == Some("") && v["size"].as_u64().unwrap_or(0) > 0) {
                    let raw = self
                        .rb_accept(Method::GET, &format!("/repos/{}/contents/{}", self.s.repo, FILE_PATH), "application/vnd.github.raw+json")
                        .query(&[("ref", self.s.branch.as_str())])
                        .timeout(Duration::from_secs(120))
                        .send()
                        .await
                        .map_err(|e| anyhow!(e.without_url()))?;
                    if !raw.status().is_success() {
                        bail!("GitHub raw download failed: HTTP {}", raw.status().as_u16());
                    }
                    let bytes = raw.bytes().await.map_err(|e| anyhow!(e.without_url()))?;
                    return Ok(Some((sha, bytes.to_vec())));
                }
                let content = content_str.ok_or_else(|| anyhow!("GitHub response lacks content"))?;
                let clean: String = content.chars().filter(|c| !c.is_whitespace()).collect();
                Ok(Some((sha, STANDARD.decode(clean)?)))
            }
            404 => Ok(None),
            s => bail!("GitHub GET contents failed: HTTP {s}"),
        }
    }

    async fn ensure_branch(&mut self) -> Result<()> {
        if self.branch_ready {
            return Ok(());
        }
        let repo = self.s.repo.clone();
        let r = self
            .rb(Method::GET, &format!("/repos/{repo}/git/ref/heads/{}", self.s.branch))
            .send()
            .await
            .map_err(|e| anyhow!(e.without_url()))?;
        match r.status().as_u16() {
            200 => {}
            404 => {
                let v: Value = self
                    .rb(Method::GET, &format!("/repos/{repo}"))
                    .send()
                    .await
                    .map_err(|e| anyhow!(e.without_url()))?
                    .json()
                    .await?;
                let default = v["default_branch"].as_str().ok_or_else(|| anyhow!("no default_branch"))?.to_string();
                let v: Value = self
                    .rb(Method::GET, &format!("/repos/{repo}/git/ref/heads/{default}"))
                    .send()
                    .await
                    .map_err(|e| anyhow!(e.without_url()))?
                    .json()
                    .await?;
                let sha = v["object"]["sha"].as_str().ok_or_else(|| anyhow!("no base sha"))?.to_string();
                let r = self
                    .rb(Method::POST, &format!("/repos/{repo}/git/refs"))
                    .json(&json!({ "ref": format!("refs/heads/{}", self.s.branch), "sha": sha }))
                    .send()
                    .await
                    .map_err(|e| anyhow!(e.without_url()))?;
                let st = r.status().as_u16();
                if st != 201 && st != 422 {
                    bail!("creating state branch failed: HTTP {st}");
                }
                info!(branch = %self.s.branch, "created state branch");
            }
            s => bail!("GitHub branch check failed: HTTP {s}"),
        }
        self.branch_ready = true;
        Ok(())
    }

    async fn put(&self, bytes: &[u8], sha: Option<&str>) -> std::result::Result<String, PutErr> {
        let mut body = json!({
            "message": "seen: update state [skip render]",
            "content": STANDARD.encode(bytes),
            "branch": self.s.branch,
        });
        if let Some(s) = sha {
            body["sha"] = json!(s);
        }
        let r = self
            .rb(Method::PUT, &format!("/repos/{}/contents/{}", self.s.repo, FILE_PATH))
            .json(&body)
            .send()
            .await
            .map_err(|e| PutErr::Other(anyhow!(e.without_url())))?;
        match r.status().as_u16() {
            200 | 201 => {
                let v: Value = r.json().await.map_err(|e| PutErr::Other(e.into()))?;
                v["content"]["sha"]
                    .as_str()
                    .map(String::from)
                    .ok_or_else(|| PutErr::Other(anyhow!("PUT response lacks sha")))
            }
            409 | 422 => Err(PutErr::Conflict),
            s @ (403 | 429) => Err(PutErr::Other(anyhow!("GitHub rate limited or forbidden (HTTP {s})"))),
            s => Err(PutErr::Other(anyhow!("GitHub PUT failed: HTTP {s}"))),
        }
    }
}

async fn push(gh: &mut GitHub, sha: &mut Option<String>, bytes: &[u8]) -> Result<()> {
    gh.ensure_branch().await?;
    if sha.is_none() {
        *sha = gh.get_file().await?.map(|(s, _)| s);
    }
    for attempt in 0..2 {
        match gh.put(bytes, sha.as_deref()).await {
            Ok(n) => {
                *sha = Some(n);
                return Ok(());
            }
            Err(PutErr::Conflict) if attempt == 0 => {
                *sha = gh.get_file().await?.map(|(s, _)| s);
            }
            Err(PutErr::Conflict) => bail!("GitHub sha conflict persisted"),
            Err(PutErr::Other(e)) => return Err(e),
        }
    }
    bail!("unreachable")
}

async fn fetch_remote(gh: &GitHub) -> Result<Option<(String, Vec<u8>)>> {
    let mut delay = 2u64;
    for attempt in 1..=5 {
        match gh.get_file().await {
            Ok(r) => return Ok(r),
            Err(e) if attempt == 5 => return Err(e.context("could not restore state from GitHub")),
            Err(e) => {
                warn!("state restore attempt {attempt} failed: {e:#}");
                sleep(Duration::from_secs(delay)).await;
                delay *= 2;
            }
        }
    }
    unreachable!()
}

pub struct SeenStore {
    order: VecDeque<String>,
    set: HashSet<String>,
    subs: HashSet<String>,
    dirty: bool,
    path: PathBuf,
    cap: usize,
    gh: Option<GitHub>,
    sha: Option<String>,
    last_flush: Option<tokio::time::Instant>,
}

impl SeenStore {
    pub fn new(path: PathBuf, cap: usize) -> Self {
        Self { order: VecDeque::new(), set: HashSet::new(), subs: HashSet::new(), dirty: false, path, cap, gh: None, sha: None, last_flush: None }
    }

    /// Restores state. With GitHub configured, a *failed* restore is fatal so a
    /// transient outage can never cause a re-send flood from an empty history.
    pub async fn open(path: PathBuf, cap: usize, gh: Option<(Client, GithubSettings)>) -> Result<Self> {
        let mut st = Self::new(path, cap);
        st.gh = gh.map(|(client, s)| GitHub { client, s, branch_ready: false });
        let remote = match &st.gh {
            Some(gh) => fetch_remote(gh).await?,
            None => None,
        };
        match remote {
            Some((sha, bytes)) => {
                let state: State = serde_json::from_slice(&bytes).context("remote state file is corrupt; refusing to overwrite")?;
                st.apply(state);
                st.sha = Some(sha);
                info!(ids = st.order.len(), "state restored from GitHub");
                st.write_local().await;
            }
            None => {
                let had_local = st.load_local().await;
                if st.gh.is_some() {
                    info!("no remote state yet");
                    st.dirty = had_local; // push a surviving local file to the new state branch
                } else {
                    warn!("GitHub backup not configured: state is lost when the instance restarts");
                }
            }
        }
        Ok(st)
    }

    fn apply(&mut self, s: State) {
        for id in s.ids {
            self.insert_inner(id);
        }
        self.subs.extend(s.subs);
    }

    pub async fn load_local(&mut self) -> bool {
        match tokio::fs::read(&self.path).await {
            Ok(b) => match serde_json::from_slice::<State>(&b) {
                Ok(s) => {
                    self.apply(s);
                    info!(ids = self.order.len(), "state loaded from local file");
                    !self.order.is_empty()
                }
                Err(e) => {
                    warn!("local state file unreadable: {e}");
                    false
                }
            },
            Err(_) => false,
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
        Ok(serde_json::to_vec(&StateRef { ids: &self.order, subs })?)
    }

    async fn write_local(&self) {
        let Ok(bytes) = self.snapshot() else { return };
        write_file(&self.path, &bytes).await;
    }

    /// Flush only if dirty AND the last flush is older than `min` (limits GitHub API traffic).
    pub async fn flush_if_due(&mut self, min: Duration) {
        if !self.dirty {
            return;
        }
        if let Some(t) = self.last_flush {
            if t.elapsed() < min {
                return;
            }
        }
        self.flush().await;
    }

    /// Writes local file and pushes to GitHub (if configured). On failure the
    /// store stays dirty and is retried on the next flush.
    pub async fn flush(&mut self) {
        if !self.dirty {
            return;
        }
        let bytes = match self.snapshot() {
            Ok(b) => b,
            Err(e) => {
                warn!("cannot serialize state: {e}");
                return;
            }
        };
        write_file(&self.path, &bytes).await;
        self.last_flush = Some(tokio::time::Instant::now());
        match self.gh.as_mut() {
            None => self.dirty = false,
            Some(gh) => match push(gh, &mut self.sha, &bytes).await {
                Ok(()) => {
                    self.dirty = false;
                    info!(ids = self.order.len(), "state flushed to GitHub");
                }
                Err(e) => warn!("GitHub state flush failed (will retry): {e:#}"),
            },
        }
    }
}

async fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(dir) = path.parent() {
        let _ = tokio::fs::create_dir_all(dir).await;
    }
    let tmp = path.with_extension("tmp");
    if let Err(e) = tokio::fs::write(&tmp, bytes).await {
        warn!("local state write failed: {e}");
        return;
    }
    if let Err(e) = tokio::fs::rename(&tmp, path).await {
        warn!("local state rename failed: {e}");
    }
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
        assert!(!s.contains("a")); // evicted: oldest of 3 cap after d arrives
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

        let mut s2 = SeenStore::new(p.clone(), 100);
        assert!(s2.load_local().await);
        assert!(s2.contains("1700000000000000001"));
        assert!(s2.sub_initialized("foo"));
        assert!(!s2.contains("t3_y"));
        let _ = std::fs::remove_file(&p);
    }
}
