//! Free X data sources (no X API, no keys):
//!   * discovery: configurable RSS URL templates (FxTwitter `/{user}/feed.xml` or Nitter `/{user}/rss`);
//!   * details:   the FxEmbed JSON API (`api.fxtwitter.com/<user>/status/<id>`).
//! Both are unofficial third-party services and can stop working at any time.
use anyhow::{anyhow, bail, Result};
use reqwest::Client;
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_FEED_BYTES: usize = 5_000_000;

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Post ids are snowflakes: the creation time is encoded in them.
pub fn id_age_secs(id: &str, now_ms: u64) -> Option<u64> {
    let n: u64 = id.parse().ok()?;
    let created = (n >> 22) + 1_288_834_974_657;
    Some(now_ms.saturating_sub(created) / 1000)
}

pub fn valid_handle(h: &str) -> bool {
    !h.is_empty() && h.len() <= 15 && h.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// ---------------------------------------------------------------- RSS

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedItem {
    pub handle: String,
    pub id: String,
    /// Who reposted it, as a display label ("@handle" from RSS, a name from the For you page).
    pub reposter: Option<String>,
}

fn unescape(s: &str) -> String {
    let s = s.trim();
    let s = s.strip_prefix("<![CDATA[").and_then(|x| x.strip_suffix("]]>")).unwrap_or(s);
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Inner text of the first `<name>...</name>` element in `block`.
fn tag<'a>(block: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}");
    let close = format!("</{name}>");
    let mut from = 0;
    while let Some(i) = block[from..].find(&open) {
        let start = from + i + open.len();
        match block[start..].chars().next() {
            Some('>') | Some(' ') | Some('\n') | Some('/') => {
                let gt = block[start..].find('>')? + start;
                if block[..gt].ends_with('/') {
                    return None; // self-closing
                }
                let end = block[gt + 1..].find(&close)? + gt + 1;
                return Some(&block[gt + 1..end]);
            }
            _ => from = start,
        }
    }
    None
}

fn status_of(link: &str) -> Option<(String, String)> {
    let i = link.find("/status/")?;
    let id: String = link[i + 8..].chars().take_while(|c| c.is_ascii_digit()).collect();
    if id.is_empty() {
        return None;
    }
    let handle = link[..i].rsplit('/').next()?;
    valid_handle(handle).then(|| (handle.to_string(), id))
}

fn repost_of(title: &str) -> Option<String> {
    let rest = title.trim_start().strip_prefix("RT by @")?;
    let h: String = rest.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
    valid_handle(&h).then(|| format!("@{h}"))
}

/// Parses a Nitter-style RSS 2.0 feed. Errors on anything else (block pages, HTML, other formats).
pub fn parse_rss(xml: &str) -> Result<Vec<FeedItem>> {
    if xml.len() > MAX_FEED_BYTES {
        bail!("feed too large");
    }
    if xml.contains("<!DOCTYPE") || xml.contains("<!ENTITY") {
        bail!("not an RSS feed (DOCTYPE found: probably a block page)");
    }
    if !(xml.contains("<rss") && xml.contains("<channel")) {
        bail!("not an RSS 2.0 feed");
    }
    let mut out: Vec<FeedItem> = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find("<item") {
        let after = &rest[i + 5..];
        if !matches!(after.chars().next(), Some('>') | Some(' ') | Some('\n')) {
            rest = after; // e.g. "<items"
            continue;
        }
        let Some(end) = after.find("</item>") else { break };
        let block = &after[..end];
        rest = &after[end + 7..];
        let Some(link) = tag(block, "link") else { continue };
        let Some((handle, id)) = status_of(&unescape(link)) else { continue };
        let reposter = tag(block, "title").and_then(|t| repost_of(&unescape(t)));
        if !out.iter().any(|x| x.id == id) {
            out.push(FeedItem { handle, id, reposter });
        }
    }
    Ok(out)
}

/// Resolve an RSS source. Explicit {handle}/{user} templates support different feed URL
/// layouts; a bare host keeps the historical Nitter /<handle>/rss behaviour.
fn feed_url(source: &str, account: &str) -> String {
    if source.contains("{handle}") {
        source.replace("{handle}", account)
    } else if source.contains("{user}") {
        source.replace("{user}", account)
    } else {
        format!("{}/{}/rss", source.trim_end_matches('/'), account)
    }
}

/// RSS sources in priority order; the one that worked last is tried first.
pub struct Sources {
    client: Client,
    bases: Vec<String>,
}

impl Sources {
    pub fn new(client: Client, bases: Vec<String>) -> Self {
        Self { client, bases }
    }

    pub fn primary(&self) -> &str {
        self.bases.first().map(String::as_str).unwrap_or("")
    }

    async fn try_one(&self, url: &str) -> Result<Vec<FeedItem>> {
        let resp = self.client.get(url).timeout(Duration::from_secs(30)).send().await.map_err(|e| anyhow!(e.without_url()))?;
        if !resp.status().is_success() {
            bail!("HTTP {}", resp.status().as_u16());
        }
        if resp.content_length().map(|l| l as usize > MAX_FEED_BYTES).unwrap_or(false) {
            bail!("feed too large");
        }
        let text = resp.text().await.map_err(|e| anyhow!(e.without_url()))?;
        parse_rss(&text)
    }

    pub async fn fetch_feed(&mut self, account: &str) -> Result<Vec<FeedItem>> {
        let mut last = String::from("no sources");
        let mut empty_ok = false; // someone answered with a valid but empty feed
        for pos in 0..self.bases.len() {
            let url = feed_url(&self.bases[pos], account);
            match self.try_one(&url).await {
                // Blocked or rate-limited instances often answer with an empty feed: ask the next one too.
                Ok(items) if items.is_empty() => empty_ok = true,
                Ok(items) => {
                    if pos != 0 {
                        let b = self.bases.remove(pos); // remember what works
                        self.bases.insert(0, b);
                    }
                    return Ok(items);
                }
                Err(e) => last = format!("{url}: {e:#}"),
            }
        }
        if empty_ok {
            return Ok(Vec::new()); // every instance that answered says the account has no posts
        }
        bail!("all RSS sources failed for @{account} ({last})")
    }
}

// ---------------------------------------------------------------- FxEmbed

#[derive(Debug, Clone, PartialEq)]
pub struct Video {
    pub url: String,
    pub gif: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Post {
    pub id: String,
    pub handle: String,
    pub name: String,
    pub text: String,
    pub likes: u64,
    pub reply: bool,
    pub reposter: Option<String>,
    pub photos: Vec<String>,
    pub videos: Vec<Video>,
}

impl Post {
    pub fn has_media(&self) -> bool {
        !self.photos.is_empty() || !self.videos.is_empty()
    }
    pub fn url(&self) -> String {
        format!("https://x.com/{}/status/{}", self.handle, self.id)
    }
}

fn json_truthy(v: &Value) -> bool {
    match v {
        Value::Null | Value::Bool(false) => false,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        _ => true,
    }
}

pub fn parse_fx(v: &Value, handle: &str, id: &str, reposter: Option<String>) -> Option<Post> {
    let tw = v.get("tweet").filter(|t| t.is_object())?;
    let author = &tw["author"];
    let shown_handle = author["screen_name"].as_str().filter(|h| valid_handle(h)).unwrap_or(handle);
    let urls = |key: &str| -> Vec<&Value> {
        tw["media"][key].as_array().map(|a| a.iter().collect()).unwrap_or_default()
    };
    Some(Post {
        id: id.to_string(),
        handle: shown_handle.to_string(),
        name: author["name"].as_str().filter(|n| !n.is_empty()).unwrap_or(shown_handle).to_string(),
        text: tw["text"].as_str().unwrap_or("").trim().to_string(),
        likes: tw["likes"].as_u64().unwrap_or(0),
        reply: json_truthy(&tw["replying_to"]),
        reposter,
        photos: urls("photos").iter().filter_map(|p| p["url"].as_str()).map(String::from).collect(),
        videos: urls("videos")
            .iter()
            .filter_map(|m| m["url"].as_str().map(|u| Video { url: u.to_string(), gif: m["type"].as_str() == Some("gif") }))
            .collect(),
    })
}

/// Post details from FxEmbed. `Ok(None)` = the post is gone or private; `Err` = temporary problem.
pub async fn fetch_post(client: &Client, fx_base: &str, handle: &str, id: &str, reposter: Option<String>) -> Result<Option<Post>> {
    let url = format!("{fx_base}/{handle}/status/{id}");
    let resp = client.get(&url).timeout(Duration::from_secs(30)).send().await.map_err(|e| anyhow!(e.without_url()))?;
    match resp.status().as_u16() {
        401 | 403 | 404 | 410 => return Ok(None),
        s if !(200..300).contains(&s) => bail!("FxEmbed HTTP {s}"),
        _ => {}
    }
    let v: Value = resp.json().await.map_err(|e| anyhow!("FxEmbed returned invalid JSON: {}", e.without_url()))?;
    Ok(parse_fx(&v, handle, id, reposter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rss(items: &str) -> String {
        format!(r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>x</title>{items}</channel></rss>"#)
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
    fn fuzz_feed_parsers_never_panic() {
        let tokens = ["<item>", "</item>", "<item", "<link>", "</link>", "<link/>", "<title>", "</title>", "<rss", "<channel>", "https://x/", "/status/", "/status/123", "RT by @", "RT by @a_b: ", "<![CDATA[", "]]>", "&amp;", "&#39;", "<!DOCTYPE", "<!ENTITY", "</", "<", ">", "a", "NASA"];
        let mut r = Rng(0x9E3779B97F4A7C15);
        for _ in 0..60_000 {
            let s = r.text(&tokens, 40);
            let _ = parse_rss(&s);
            let _ = parse_rss(&format!("<rss><channel>{s}</channel></rss>"));
            let _ = status_of(&s);
            let _ = repost_of(&s);
            let _ = tag(&s, "link");
            let _ = unescape(&s);
            let _ = id_age_secs(&s, now_ms());
        }
    }

    #[test]
    fn fuzz_fx_parser_never_panics() {
        let mut r = Rng(42);
        let strs = ["", "x", "nasa", "bad name!", "https://pbs.twimg.com/a.jpg", "gif", "video", "🎉"];
        for _ in 0..30_000 {
            let val = |r: &mut Rng| -> Value {
                match r.next() % 7 {
                    0 => Value::Null,
                    1 => json!(r.next() % 1000),
                    2 => json!(-(r.next() as i64 % 5)),
                    3 => json!(r.next() % 2 == 0),
                    4 => json!([]),
                    5 => json!({}),
                    _ => json!(*r.pick(&strs)),
                }
            };
            let tw = json!({"text": val(&mut r), "likes": val(&mut r), "replying_to": val(&mut r),
                "author": val(&mut r),
                "media": {"photos": [{"url": val(&mut r)}, val(&mut r)], "videos": [{"url": val(&mut r), "type": val(&mut r)}, val(&mut r)]}});
            let v = if r.next() % 4 == 0 { val(&mut r) } else { json!({"tweet": tw}) };
            let _ = parse_fx(&v, "h", "1", None);
        }
    }

    #[test]
    fn parses_posts_reposts_cdata_and_entities() {
        let x = rss(
            r#"<item><title>hello &amp; bye</title><link>https://xcancel.com/NASA/status/111#m</link></item>
               <item><title><![CDATA[RT by @NASA: great]]></title><link>https://xcancel.com/esa/status/222#m</link></item>
               <item><title>no link</title></item>
               <item><title>bad</title><link>https://xcancel.com/NASA/likes</link></item>
               <item><title>dup</title><link>https://xcancel.com/NASA/status/111#m</link></item>
               <item><title>x</title><link>https://xcancel.com/web/status/333</link></item>
               <items><link>https://xcancel.com/NASA/status/999</link></items>"#,
        );
        let items = parse_rss(&x).unwrap();
        assert_eq!(
            items,
            vec![
                FeedItem { handle: "NASA".into(), id: "111".into(), reposter: None },
                FeedItem { handle: "esa".into(), id: "222".into(), reposter: Some("@NASA".into()) },
                FeedItem { handle: "web".into(), id: "333".into(), reposter: None },
            ]
        );
    }

    #[test]
    fn resolves_fxtwitter_and_nitter_source_templates() {
        assert_eq!(
            feed_url("https://fxtwitter.com/{handle}/feed.xml", "NASA"),
            "https://fxtwitter.com/NASA/feed.xml"
        );
        assert_eq!(
            feed_url("https://nitter.example/{user}/rss", "SpaceX"),
            "https://nitter.example/SpaceX/rss"
        );
        // Existing deployments that specify only a Nitter host remain compatible.
        assert_eq!(feed_url("https://xcancel.com", "esa"), "https://xcancel.com/esa/rss");
    }

    #[test]
    fn rejects_non_rss() {
        assert!(parse_rss("<html><body>Just a moment...</body></html>").is_err());
        assert!(parse_rss("Access denied").is_err());
        assert!(parse_rss("<!DOCTYPE x [<!ENTITY a \"b\">]><rss><channel></channel></rss>").is_err());
        assert!(parse_rss("<feed><entry/></feed>").is_err());
        assert!(parse_rss(&"x".repeat(MAX_FEED_BYTES + 1)).is_err());
        assert!(parse_rss(&rss("")).unwrap().is_empty()); // a valid empty feed is fine
    }

    #[test]
    fn tag_helper_edges() {
        assert_eq!(tag("<link>a</link>", "link"), Some("a"));
        assert_eq!(tag("<linkx>a</linkx><link>b</link>", "link"), Some("b"));
        assert_eq!(tag("<link/>", "link"), None);
        assert_eq!(tag("<link>unterminated", "link"), None);
    }

    #[test]
    fn snowflake_age() {
        let now = 1_800_000_000_000u64;
        let id = (((now - 120_000) - 1_288_834_974_657) << 22) | 7;
        assert_eq!(id_age_secs(&id.to_string(), now), Some(120));
        assert_eq!(id_age_secs("abc", now), None);
        assert_eq!(id_age_secs("1", now).map(|a| a > 0), Some(true));
    }

    #[test]
    fn fx_parsing() {
        let v = json!({"code":200,"tweet":{"text":" hi ","likes":7,"replying_to":null,
            "author":{"screen_name":"nasa","name":"NASA"},
            "media":{"photos":[{"url":"https://pbs.twimg.com/media/a.jpg"}],
                     "videos":[{"url":"https://video.twimg.com/v.mp4","type":"gif"},{"url":"https://video.twimg.com/w.mp4","type":"video"},{"type":"video"}]}}});
        let p = parse_fx(&v, "fallback", "5", Some("bob".into())).unwrap();
        assert_eq!((p.handle.as_str(), p.name.as_str(), p.text.as_str(), p.likes), ("nasa", "NASA", "hi", 7));
        assert!(!p.reply && p.has_media() && p.reposter.as_deref() == Some("bob"));
        assert_eq!(p.photos, vec!["https://pbs.twimg.com/media/a.jpg"]);
        assert_eq!(p.videos.len(), 2);
        assert!(p.videos[0].gif && !p.videos[1].gif);
        assert_eq!(p.url(), "https://x.com/nasa/status/5");

        let r = json!({"tweet":{"text":"x","replying_to":"bob","author":{"screen_name":"bad name!","name":""}}});
        let p = parse_fx(&r, "fb", "6", None).unwrap();
        assert!(p.reply && !p.has_media());
        assert_eq!((p.handle.as_str(), p.name.as_str(), p.likes), ("fb", "fb", 0)); // invalid handle/name -> safe fallbacks
        assert!(parse_fx(&json!({"code":404}), "a", "1", None).is_none());
        assert!(parse_fx(&json!({"tweet":null}), "a", "1", None).is_none());
        assert!(parse_fx(&json!({"tweet":{}}), "a", "1", None).is_some());
        assert!(!json_truthy(&json!(false)) && !json_truthy(&json!("")) && json_truthy(&json!({"x":1})));
    }
}
