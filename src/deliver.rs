//! Turns a `Post` into Telegram messages: native photos / albums / videos / GIFs, with the text
//! as caption (or, if it does not fit, as follow-up messages). Everything is streamed through
//! temp files; nothing large is held in RAM.
use crate::{
    config::Config,
    media_downloader as md,
    telegram::{FileKind, Telegram, TgError},
    x_source::Post,
};
use anyhow::Result;
use reqwest::Client;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::time::{sleep, Instant};
use tracing::warn;

/// Telegram's caption limit is 1024 characters after parsing; we stay safely below.
const CAPTION_MAX: usize = 1000;
/// Follow-up / text-only message chunk (escaped characters). Telegram's hard limit is 4096.
const TEXT_CHUNK: usize = 3000;
/// Per-image download cap (an album holds up to 10 temp files at once).
const PHOTO_DL_MAX: u64 = 25 * 1024 * 1024;

pub fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn esc_len(c: char) -> usize {
    match c {
        '&' => 5,
        '<' | '>' => 4,
        _ => 1,
    }
}

/// Splits `text` into chunks whose HTML-escaped length is <= `limit`, preferring to break at whitespace.
pub fn split_text(text: &str, limit: usize) -> Vec<String> {
    let chars: Vec<char> = text.trim().chars().collect();
    let limit = limit.max(10);
    let mut out = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        while start < chars.len() && chars[start].is_whitespace() {
            start += 1;
        }
        if start >= chars.len() {
            break;
        }
        let (mut end, mut used) = (start, 0usize);
        while end < chars.len() {
            let l = esc_len(chars[end]);
            if used + l > limit {
                break;
            }
            used += l;
            end += 1;
        }
        if end < chars.len() {
            let mid = start + (end - start) / 2;
            let brk = (mid..end)
                .rev()
                .find(|&i| chars[i] == '\n')
                .or_else(|| (mid..end).rev().find(|&i| chars[i].is_whitespace()));
            if let Some(p) = brk.filter(|&p| p > start) {
                end = p;
            }
        }
        let chunk: String = chars[start..end].iter().collect();
        let chunk = chunk.trim();
        if !chunk.is_empty() {
            out.push(chunk.to_string());
        }
        start = end;
    }
    out
}

/// Display names come from a third party: keep the header short whatever they send.
fn clip(s: &str, max: usize) -> String {
    let t: String = s.chars().take(max).collect();
    if t.chars().count() < s.chars().count() {
        format!("{}…", t.trim_end())
    } else {
        t
    }
}

fn header(p: &Post) -> String {
    let mut h = format!("<b>{}</b> (@{})", esc(&clip(&p.name, 80)), esc(&clip(&p.handle, 15)));
    if let Some(by) = &p.reposter {
        h.push_str(&format!(" · 🔁 {}", esc(&clip(by, 60))));
    }
    h
}

fn footer(p: &Post) -> String {
    format!("❤️ {}  |  <a href=\"{}\">Open on X</a>", p.likes, p.url().replace('"', "%22"))
}

/// Caption for a post with media, plus follow-up messages carrying the full text when it won't fit.
pub fn render_media(p: &Post) -> (String, Vec<String>) {
    let (h, f) = (header(p), footer(p));
    let text = p.text.trim();
    let all = if text.is_empty() { format!("{h}\n\n{f}") } else { format!("{h}\n\n{}\n\n{f}", esc(text)) };
    if all.chars().count() <= CAPTION_MAX {
        return (all, vec![]);
    }
    (format!("{h}\n\n{f}"), split_text(text, TEXT_CHUNK))
}

/// Messages for a text-only post: header on the first, footer on the last.
pub fn render_text(p: &Post) -> Vec<String> {
    let (h, f) = (header(p), footer(p));
    let pieces = split_text(&p.text, TEXT_CHUNK);
    if pieces.is_empty() {
        return vec![format!("{h}\n\n{f}")];
    }
    let n = pieces.len();
    pieces
        .iter()
        .enumerate()
        .map(|(i, piece)| {
            let mut s = esc(piece);
            if i == 0 {
                s = format!("{h}\n\n{s}");
            }
            if i == n - 1 {
                s = format!("{s}\n\n{f}");
            }
            s
        })
        .collect()
}

fn host_of(url: &str) -> Option<&str> {
    let rest = url.strip_prefix("https://").or_else(|| url.strip_prefix("http://"))?;
    let authority = rest.split(['/', '?', '#']).next()?;
    if authority.contains('@') {
        return None;
    }
    authority.split(':').next().filter(|h| !h.is_empty())
}

/// Media is only downloaded from the configured hosts (default: *.twimg.com) over https.
pub fn media_url_allowed(cfg: &Config, url: &str) -> bool {
    if !(url.starts_with("https://") || (cfg.allow_http_media && url.starts_with("http://"))) {
        return false;
    }
    let Some(host) = host_of(url) else { return false };
    let host = host.to_ascii_lowercase();
    cfg.media_hosts.iter().any(|h| host == *h || host.ends_with(&format!(".{h}")))
}

/// X serves photos in several sizes; ask for the large one.
fn photo_url(u: &str) -> String {
    if host_of(u) == Some("pbs.twimg.com") && !u.contains('?') {
        format!("{u}?name=large")
    } else {
        u.to_string()
    }
}

fn photo_ext(u: &str) -> String {
    let e = md::ext_of(u);
    if e != "bin" {
        return e;
    }
    u.split(['?', '&']).find_map(|kv| kv.strip_prefix("format=")).map(|f| f.to_ascii_lowercase()).unwrap_or(e)
}

fn retry_as_document(e: &TgError, kind: FileKind) -> bool {
    if e.code != 400 || kind == FileKind::Document {
        return false;
    }
    let d = e.desc.to_ascii_lowercase();
    !["chat", "rights", "forbidden", "caption", "blocked", "kicked", "entities", "parse"].iter().any(|w| d.contains(w))
}

pub struct Deliverer {
    cfg: Arc<Config>,
    /// Client for media downloads only: redirects are followed only to allowed hosts (https), at most 3.
    media_http: Client,
    tg: Telegram,
    last_send: Mutex<HashMap<String, Instant>>,
}

impl Deliverer {
    pub fn new(cfg: Arc<Config>, _http: Client, tg: Telegram) -> Self {
        let policy_cfg = cfg.clone();
        let media_http = Client::builder()
            .user_agent(cfg.user_agent.clone())
            .connect_timeout(std::time::Duration::from_secs(15))
            .pool_max_idle_per_host(1)
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= 3 {
                    attempt.error("too many redirects")
                } else if media_url_allowed(&policy_cfg, attempt.url().as_str()) {
                    attempt.follow()
                } else {
                    attempt.stop() // the 3xx answer is then treated as a failed download
                }
            }))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { cfg, media_http, tg, last_send: Mutex::new(HashMap::new()) }
    }

    /// Keeps us under Telegram's per-chat rate limits (~20 msgs/min to a channel).
    async fn pace_n(&self, chat: &str, n: u32) {
        let wait = {
            let mut m = self.last_send.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let start = match m.get(chat) {
                Some(t) => (*t).max(now),
                None => now,
            };
            m.insert(chat.to_string(), start + self.cfg.send_gap * n);
            start - now
        };
        if !wait.is_zero() {
            sleep(wait).await;
        }
    }

    async fn msg(&self, chat: &str, text: &str) -> Result<()> {
        self.pace_n(chat, 1).await;
        self.tg.send_message(chat, text).await?;
        Ok(())
    }

    async fn file(&self, chat: &str, kind: FileKind, path: &Path, cap: &str) -> Result<(), TgError> {
        self.pace_n(chat, 1).await;
        self.tg.send_file(chat, kind, path, cap, false).await
    }

    /// Sends one post. An `Err` means nothing (or nothing important) was sent, so it is safe to retry.
    pub async fn deliver(&self, chat: &str, p: &Post) -> Result<()> {
        if !p.has_media() {
            let msgs = render_text(p);
            for (i, m) in msgs.iter().enumerate() {
                match self.msg(chat, m).await {
                    Ok(()) => {}
                    Err(e) if i == 0 => return Err(e),
                    Err(e) => {
                        warn!(post = %p.id, "later part of a long post failed after the first was sent: {e:#}");
                        break; // don't make the scheduler re-send the first part
                    }
                }
            }
            return Ok(());
        }

        let (cap, follow) = if self.cfg.captions { render_media(p) } else { (String::new(), vec![]) };
        if p.photos.is_empty() {
            self.video(chat, p, &cap).await?;
        } else {
            self.photos(chat, p, &cap).await?;
        }
        for t in follow {
            // The media is out already: a failure here must never trigger a re-send.
            if let Err(e) = self.msg(chat, &t).await {
                warn!(post = %p.id, "follow-up text failed: {e:#}");
                break;
            }
        }
        Ok(())
    }

    async fn photos(&self, chat: &str, p: &Post, cap: &str) -> Result<()> {
        let mut files: Vec<(md::TempFile, u64)> = Vec::new();
        for raw in p.photos.iter().take(10) {
            let url = photo_url(raw);
            if !media_url_allowed(&self.cfg, &url) {
                warn!(post = %p.id, "photo host not allowed; skipped");
                continue;
            }
            match md::download(&self.media_http, &url, PHOTO_DL_MAX, &photo_ext(&url)).await {
                Ok(tf) => {
                    let size = tf.size().await;
                    files.push((tf, size));
                }
                Err(e) => warn!(post = %p.id, reason = %e, "photo skipped"),
            }
        }
        match files.len() {
            0 => self.link(chat, p, cap).await, // link preview still shows the images
            1 => self.upload_photo(chat, files[0].0.path(), files[0].1, cap).await,
            _ => self.album(chat, &files, cap).await,
        }
    }

    async fn upload_photo(&self, chat: &str, path: &Path, size: u64, cap: &str) -> Result<()> {
        if size <= self.cfg.max_photo_bytes {
            match self.file(chat, FileKind::Photo, path, cap).await {
                Ok(()) => return Ok(()),
                Err(e) if !retry_as_document(&e, FileKind::Photo) => return Err(e.into()),
                Err(e) => warn!("telegram rejected photo ({}); sending as document", e.desc),
            }
        }
        self.file(chat, FileKind::Document, path, cap).await?; // too big / odd dimensions: last resort
        Ok(())
    }

    async fn album(&self, chat: &str, files: &[(md::TempFile, u64)], cap: &str) -> Result<()> {
        let paths: Vec<&Path> = files.iter().map(|(t, _)| t.path()).collect();
        self.pace_n(chat, paths.len() as u32).await;
        match self.tg.send_media_group(chat, &paths, cap, false).await {
            Ok(()) => Ok(()),
            Err(e) if retry_as_document(&e, FileKind::Photo) => {
                warn!("telegram rejected album ({}); sending images one by one", e.desc);
                for (i, (t, s)) in files.iter().enumerate() {
                    self.upload_photo(chat, t.path(), *s, if i == 0 { cap } else { "" }).await?;
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn video(&self, chat: &str, p: &Post, cap: &str) -> Result<()> {
        let v = &p.videos[0];
        if !media_url_allowed(&self.cfg, &v.url) {
            warn!(post = %p.id, "video host not allowed; sending a link instead");
            return self.link(chat, p, cap).await;
        }
        let kind = if v.gif { FileKind::Animation } else { FileKind::Video };
        match md::download(&self.media_http, &v.url, self.cfg.max_upload_bytes, "mp4").await {
            Ok(tf) => match self.file(chat, kind, tf.path(), cap).await {
                Ok(()) => Ok(()),
                Err(e) if retry_as_document(&e, kind) => {
                    warn!("telegram rejected video ({}); sending as document", e.desc);
                    self.file(chat, FileKind::Document, tf.path(), cap).await?;
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
            Err(e) if md::is_too_large(&e) => {
                warn!(post = %p.id, "video over the upload limit; sending a link instead");
                self.link(chat, p, cap).await
            }
            Err(e) => Err(e), // temporary: the scheduler retries, then skips after 3 cycles
        }
    }

    /// Link message (Telegram shows a preview with the media).
    async fn link(&self, chat: &str, p: &Post, cap: &str) -> Result<()> {
        let text = if cap.trim().is_empty() { p.url() } else { cap.to_string() };
        self.msg(chat, &text).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::x_source::Video;

    fn post(text: &str) -> Post {
        Post {
            id: "5".into(),
            handle: "nasa".into(),
            name: "NASA <&>".into(),
            text: text.into(),
            likes: 12,
            reply: false,
            reposter: None,
            photos: vec![],
            videos: vec![],
        }
    }

    fn cfg() -> Config {
        let get = |k: &str| match k {
            "TELEGRAM_BOT_TOKEN" => Some("t".to_string()),
            _ => None,
        };
        Config::from_lookup(&get, false).unwrap()
    }


    /// Tiny deterministic PRNG so the fuzz tests need no extra dependency.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
            &items[(self.next() % items.len() as u64) as usize]
        }
        fn text(&mut self, tokens: &[&str], max: usize) -> String {
            let n = (self.next() % max as u64) as usize;
            let mut s = String::new();
            for _ in 0..n {
                match self.next() % 5 {
                    0 => s.push(char::from_u32((self.next() % 0x2FFF) as u32 + 0x20).unwrap_or('x')),
                    1 => s.push(*self.pick(&['é', '🎉', '\u{0}', '\n', '\r', ' ', '<', '>', '&', '"', '\'', '\\', '/', '#', '?'])),
                    _ => s.push_str(*self.pick(tokens)),
                }
            }
            s
        }
    }

    #[test]
    fn fuzz_text_and_url_helpers_never_panic_and_keep_their_promises() {
        let mut r = Rng(1234);
        let tokens = ["word ", "a", "&", "<b>", ">", "\n\n", "  ", "🎉", "héllo", "https://pbs.twimg.com/media/x.jpg", "?name=large", "@", ":", "//"];
        let c = cfg();
        for _ in 0..30_000 {
            let s = r.text(&tokens, 60);
            let limit = 10 + (r.next() % 200) as usize;
            let parts = split_text(&s, limit);
            assert!(parts.iter().all(|p| esc(p).chars().count() <= limit && !p.is_empty()));
            let _ = host_of(&s);
            let _ = media_url_allowed(&c, &s);
            let _ = photo_url(&s);
            let _ = photo_ext(&s);
            let mut p = post(&s);
            p.reposter = Some(s.clone());
            p.name = s.clone();
            let (cap, follow) = render_media(&p);
            assert!(cap.chars().count() <= 1024, "caption must fit Telegram's limit even with absurd names: {}", cap.chars().count());
            for f in &follow {
                assert!(esc(f).chars().count() <= TEXT_CHUNK);
            }
            for m in render_text(&p) {
                assert!(m.chars().count() <= 4096, "text message too long: {}", m.chars().count());
            }
        }
    }

    #[test]
    fn absurd_display_names_are_clipped_so_telegram_never_rejects_the_message() {
        let mut p = post("hello");
        p.name = "N".repeat(3000);
        p.reposter = Some("R".repeat(3000));
        p.photos = vec!["https://pbs.twimg.com/media/a.jpg".into()];
        let (cap, follow) = render_media(&p);
        assert!(follow.is_empty() && cap.chars().count() <= CAPTION_MAX, "{}", cap.chars().count());
        assert!(cap.contains("hello") && cap.contains("…"));
        p.text = "word ".repeat(2000);
        let (cap, _) = render_media(&p);
        assert!(cap.chars().count() <= 1024);
        for m in render_text(&p) {
            assert!(m.chars().count() <= 4096, "{}", m.chars().count());
        }
        p.name = "&".repeat(3000); // escaping inflates every character 5x
        assert!(render_text(&p).iter().all(|m| m.chars().count() <= 4096));
    }

    #[test]
    fn caption_fits_or_overflows_to_followups() {
        let (cap, follow) = render_media(&post("short & sweet"));
        assert!(follow.is_empty());
        assert!(cap.contains("<b>NASA &lt;&amp;&gt;</b> (@nasa)") && cap.contains("short &amp; sweet") && cap.contains("❤️ 12"));
        assert!(cap.ends_with("Open on X</a>"));

        let long = "word ".repeat(400);
        let (cap, follow) = render_media(&post(&long));
        assert!(cap.chars().count() <= CAPTION_MAX && !cap.contains("word"));
        assert!(!follow.is_empty() && follow.iter().all(|f| esc(f).chars().count() <= TEXT_CHUNK + 400));

        let mut p = post("hi");
        p.reposter = Some("@bob".into());
        assert!(render_media(&p).0.contains("🔁 @bob"));
    }

    #[test]
    fn text_messages_have_header_first_footer_last_and_stay_under_limit() {
        let msgs = render_text(&post(&"&".repeat(7000)));
        assert!(msgs.len() > 1);
        assert!(msgs[0].starts_with("<b>") && !msgs[0].contains("Open on X"));
        assert!(msgs.last().unwrap().ends_with("</a>") && !msgs.last().unwrap().starts_with("<b>"));
        assert!(msgs.iter().all(|m| m.chars().count() <= 4096), "{:?}", msgs.iter().map(|m| m.chars().count()).collect::<Vec<_>>());
        let one = render_text(&post(""));
        assert_eq!(one.len(), 1);
        assert!(one[0].contains("Open on X"));
    }

    #[test]
    fn split_text_is_lossless_and_bounded() {
        for txt in ["&".repeat(500), "<a> ".repeat(300), "word ".repeat(1000), "x".repeat(2000), "line one\n\nline two ".repeat(50)] {
            let parts = split_text(&txt, 100);
            assert!(parts.iter().all(|p| esc(p).chars().count() <= 100));
            let squash = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
            assert_eq!(squash(&parts.join(" ")), squash(&txt));
        }
        assert!(split_text("   ", 50).is_empty());
        assert_eq!(split_text("héllo wörld 🎉", 100), vec!["héllo wörld 🎉"]);
    }

    #[test]
    fn media_host_allowlist() {
        let c = cfg();
        assert!(media_url_allowed(&c, "https://pbs.twimg.com/media/a.jpg"));
        assert!(media_url_allowed(&c, "https://video.twimg.com/x.mp4?tag=1"));
        assert!(media_url_allowed(&c, "https://twimg.com/x"));
        assert!(!media_url_allowed(&c, "http://pbs.twimg.com/media/a.jpg")); // http refused
        assert!(!media_url_allowed(&c, "https://eviltwimg.com/a.jpg"));
        assert!(!media_url_allowed(&c, "https://twimg.com.evil.io/a.jpg"));
        assert!(!media_url_allowed(&c, "https://user@pbs.twimg.com/a.jpg"));
        assert!(!media_url_allowed(&c, "https://169.254.169.254/latest/meta-data"));
        assert!(!media_url_allowed(&c, "ftp://pbs.twimg.com/a.jpg"));
    }

    #[test]
    fn photo_helpers() {
        assert_eq!(photo_url("https://pbs.twimg.com/media/a.jpg"), "https://pbs.twimg.com/media/a.jpg?name=large");
        assert_eq!(photo_url("https://pbs.twimg.com/media/a?format=png&name=orig"), "https://pbs.twimg.com/media/a?format=png&name=orig");
        assert_eq!(photo_url("https://other.example/a.jpg"), "https://other.example/a.jpg");
        assert_eq!(photo_ext("https://pbs.twimg.com/media/a?format=png&name=orig"), "png");
        assert_eq!(photo_ext("https://pbs.twimg.com/media/a.jpg?name=large"), "jpg");
        let _ = Video { url: String::new(), gif: false };
    }

    #[test]
    fn document_fallback_rules() {
        let e = |c: u16, d: &str| TgError { code: c, desc: d.into(), retry_after: None };
        assert!(retry_as_document(&e(400, "Bad Request: PHOTO_INVALID_DIMENSIONS"), FileKind::Photo));
        assert!(!retry_as_document(&e(400, "Bad Request: chat not found"), FileKind::Photo));
        assert!(!retry_as_document(&e(400, "can't parse entities"), FileKind::Photo));
        assert!(!retry_as_document(&e(500, "boom"), FileKind::Photo));
        assert!(!retry_as_document(&e(400, "PHOTO_INVALID_DIMENSIONS"), FileKind::Document));
    }
}
