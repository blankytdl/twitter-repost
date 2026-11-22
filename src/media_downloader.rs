//! Bounded, streaming downloads to temp files. Temp files are removed on Drop (success or failure).
use anyhow::{anyhow, bail, Result};
use futures_util::StreamExt;
use reqwest::Client;
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct TempFile {
    path: PathBuf,
}

impl TempFile {
    pub fn new(ext: &str) -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("xtb-{}-{}.{}", std::process::id(), n, ext));
        Self { path }
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub async fn size(&self) -> u64 {
        tokio::fs::metadata(&self.path).await.map(|m| m.len()).unwrap_or(0)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug)]
pub struct TooLarge(pub u64);
impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exceeds size limit (>= {} bytes)", self.0)
    }
}
impl std::error::Error for TooLarge {}

pub fn is_too_large(e: &anyhow::Error) -> bool {
    e.downcast_ref::<TooLarge>().is_some()
}

pub fn ext_of(url: &str) -> String {
    let p = url.split(['?', '#']).next().unwrap_or("");
    match p.rsplit_once('.') {
        Some((_, e)) if e.len() <= 5 && e.chars().all(|c| c.is_ascii_alphanumeric()) => e.to_ascii_lowercase(),
        _ => "bin".into(),
    }
}

/// Streams `url` into a temp file; aborts as soon as `max` bytes are exceeded.
pub async fn download(client: &Client, url: &str, max: u64, ext: &str) -> Result<TempFile> {
    let resp = client
        .get(url)
        .timeout(Duration::from_secs(300))
        .send()
        .await
        .map_err(|e| anyhow!(e.without_url()))?;
    if !resp.status().is_success() {
        bail!("download failed: HTTP {}", resp.status().as_u16());
    }
    if let Some(len) = resp.content_length() {
        if len > max {
            return Err(TooLarge(len).into());
        }
    }
    let tf = TempFile::new(ext);
    let mut f = tokio::fs::File::create(tf.path()).await?;
    let mut total: u64 = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| anyhow!(e.without_url()))?;
        total += chunk.len() as u64;
        if total > max {
            return Err(TooLarge(total).into());
        }
        f.write_all(&chunk).await?;
    }
    f.flush().await?;
    Ok(tf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_of_handles_queries_and_garbage() {
        assert_eq!(ext_of("https://pbs.twimg.com/media/AbC.jpg?name=large"), "jpg");
        assert_eq!(ext_of("https://video.twimg.com/x/vid.MP4"), "mp4");
        assert_eq!(ext_of("https://pbs.twimg.com/media/AbC?format=jpg&name=large"), "bin");
        assert_eq!(ext_of("https://example.com/a.verylongext"), "bin");
    }

    #[test]
    fn temp_file_is_removed_on_drop() {
        let p = {
            let t = TempFile::new("bin");
            std::fs::write(t.path(), b"x").unwrap();
            assert!(t.path().exists());
            t.path().to_path_buf()
        };
        assert!(!p.exists());
    }
}
