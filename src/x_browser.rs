//! Reads the logged-in "For you" feed through a headless browser: one persistent Lightpanda
//! process (or any Chrome-DevTools-Protocol browser) and a minimal CDP client over WebSocket.
//!
//! The browser only DISCOVERS posts (id + author + who reposted); each post's text and media come
//! from FxEmbed exactly like in account mode, so the fragile page-scraping part stays tiny.
//! One page at a time; the page (target) is closed after every fetch.
use crate::{
    config::{BrowserKind, BrowserSettings},
    x_source::{valid_handle, FeedItem},
};
use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{process::Stdio, time::Duration};
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout, Instant},
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

/// Two complementary scripts injected into every page the bot opens (each is an idempotent IIFE):
/// AgebypassX patches X's feature switches / initial state / JSON so the age gate and sensitive-media
/// interstitials never switch on; InsensitiveX strips sensitive flags and tombstones from API answers.
const INSENSITIVE_JS: &str = concat!(include_str!("agebypass_x.js"), "\n;\n", include_str!("insensitive_x.js"));

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Runs inside the page. Waits for the timeline (or a login wall / error page), optionally selects
/// the "For you" tab, scrolls a few times collecting posts as it goes (X removes off-screen posts
/// from the DOM), and returns a JSON string. Placeholders are filled in by `script()`.
const EXTRACT_JS: &str = r#"(async () => {
  const WAIT = __WAIT_MS__, SCROLLS = __SCROLLS__, PICK_TAB = __PICK_TAB__;
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const q = (s, r) => (r || document).querySelector(s);
  const qa = (s, r) => Array.from((r || document).querySelectorAll(s));
  const found = new Map();
  const collect = () => {
    qa('article[data-testid="tweet"]').forEach(a => {
      const t = q('a[href*="/status/"] time', a);
      const link = t && t.closest('a');
      if (!link) return;
      const m = (link.getAttribute('href') || '').match(/^\/([A-Za-z0-9_]{1,15})\/status\/(\d+)/);
      if (!m || found.has(m[2])) return;
      const ctx = q('[data-testid="socialContext"]', a);
      const promoted = qa('span', a).some(s => s.children.length === 0 && (s.textContent || '').trim() === 'Ad');
      found.set(m[2], { id: m[2], handle: m[1], context: ctx ? (ctx.textContent || '').trim().slice(0, 120) : '', promoted: promoted });
    });
  };
  const state = () => {
    const href = location.href || '';
    if (/\/i\/flow\/login|\/login(\?|$|\/)/.test(href) || q('[data-testid="loginButton"]') || q('a[href="/login"]') || q('[data-testid="login"]')) return 'login';
    if (q('article[data-testid="tweet"]')) return 'ok';
    if (q('[data-testid="error-detail"]')) return 'error';
    if (/Rate limit exceeded/i.test((document.body && document.body.textContent) || '')) return 'ratelimit';
    return 'loading';
  };
  let st = 'loading';
  for (let waited = 0; waited < WAIT; waited += 500) {
    st = state();
    if (st !== 'loading') break;
    await sleep(500);
  }
  if (st === 'ok') {
    if (PICK_TAB) {
      const fy = qa('[role="tablist"] [role="tab"]').find(t => /^for you$/i.test((t.textContent || '').trim()));
      if (fy && fy.getAttribute('aria-selected') !== 'true') { fy.click(); await sleep(2500); }
    }
    collect();
    for (let i = 0; i < SCROLLS; i++) {
      window.scrollTo(0, (document.body && document.body.scrollHeight) || 100000);
      await sleep(1500);
      collect();
    }
  }
  return JSON.stringify({ status: st, url: location.href || '', title: document.title || '', posts: Array.from(found.values()) });
})()"#;

fn script(s: &BrowserSettings) -> String {
    EXTRACT_JS
        .replace("__WAIT_MS__", &s.wait.as_millis().to_string())
        .replace("__SCROLLS__", &s.scrolls.to_string())
        .replace("__PICK_TAB__", if s.pick_tab { "true" } else { "false" })
}

/// "Jane Doe reposted" -> "Jane Doe". Anything else (liked, follows, pinned...) -> None.
pub fn repost_label(context: &str) -> Option<String> {
    let c = context.trim();
    let idx = c.to_ascii_lowercase().find(" reposted")?;
    let name = c[..idx].trim();
    (!name.is_empty() && name.chars().count() <= 60).then(|| name.to_string())
}

/// Turns the JSON produced by `EXTRACT_JS` into feed items, or a clear error for login walls etc.
pub fn parse_page(json: &str) -> Result<Vec<FeedItem>> {
    let v: Value = serde_json::from_str(json).map_err(|e| anyhow!("page script returned invalid JSON: {e}"))?;
    let title = v["title"].as_str().unwrap_or("").chars().take(60).collect::<String>();
    match v["status"].as_str().unwrap_or("") {
        "ok" => {}
        "login" => bail!("X is showing its login page: X_AUTH_TOKEN / X_CT0 are missing, wrong or expired (log in to x.com again and copy fresh cookies)"),
        "error" => bail!("X returned an error page (\"Something went wrong\"); try again later"),
        "ratelimit" => bail!("X answered \"Rate limit exceeded\"; the bot will back off"),
        _ => bail!("the timeline did not render in time (page title {title:?}); this browser may not be able to run X's JavaScript, see README"),
    }
    let mut out: Vec<FeedItem> = Vec::new();
    for p in v["posts"].as_array().map(Vec::as_slice).unwrap_or_default() {
        let (Some(id), Some(handle)) = (p["id"].as_str(), p["handle"].as_str()) else { continue };
        if p["promoted"].as_bool() == Some(true) || !valid_handle(handle) || id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        if out.iter().any(|x| x.id == id) {
            continue;
        }
        out.push(FeedItem { handle: handle.to_string(), id: id.to_string(), reposter: repost_label(p["context"].as_str().unwrap_or("")) });
    }
    Ok(out)
}

struct Cdp {
    ws: Ws,
    next_id: u64,
    events: Vec<String>,
}

impl Cdp {
    async fn call(&mut self, method: &str, params: Value, session: Option<&str>, to: Duration) -> Result<Value> {
        self.next_id += 1;
        let id = self.next_id;
        let mut msg = json!({ "id": id, "method": method, "params": params });
        if let Some(s) = session {
            msg["sessionId"] = json!(s);
        }
        self.ws.send(Message::Text(msg.to_string())).await?;
        let deadline = Instant::now() + to;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let m = timeout(left, self.ws.next())
                .await
                .map_err(|_| anyhow!("CDP timeout waiting for {method}"))?
                .ok_or_else(|| anyhow!("CDP socket closed"))??;
            if let Message::Text(t) = m {
                let v: Value = serde_json::from_str(&t)?;
                if v.get("id").and_then(Value::as_u64) == Some(id) {
                    if let Some(e) = v.get("error") {
                        bail!("CDP {method} error: {e}");
                    }
                    return Ok(v.get("result").cloned().unwrap_or(Value::Null));
                }
                if let Some(ev) = v.get("method").and_then(Value::as_str) {
                    if self.events.len() < 64 {
                        self.events.push(ev.to_string());
                    }
                }
            }
        }
    }

    /// Returns true if the event was seen before the timeout.
    async fn wait_event(&mut self, names: &[&str], to: Duration) -> bool {
        if self.events.iter().any(|e| names.contains(&e.as_str())) {
            return true;
        }
        let deadline = Instant::now() + to;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let Ok(Some(Ok(m))) = timeout(left, self.ws.next()).await else { return false };
            if let Message::Text(t) = m {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    if v.get("method").and_then(Value::as_str).is_some_and(|m| names.contains(&m)) {
                        return true;
                    }
                }
            }
        }
    }
}

pub struct Browser {
    s: BrowserSettings,
    child: Option<Child>,
    cdp: Option<Cdp>,
    /// True while a fetch is running. If the fetch future is dropped from outside (the scheduler's safety
    /// timeout, a shutdown), this stays true, so the next fetch knows a page may still be open.
    in_flight: bool,
}

impl Browser {
    pub fn new(s: BrowserSettings) -> Self {
        Self { s, child: None, cdp: None, in_flight: false }
    }

    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.s.bin);
        match self.s.kind {
            BrowserKind::Lightpanda => {
                cmd.arg("serve").arg("--host").arg("127.0.0.1").arg("--port").arg(self.s.port.to_string());
            }
            BrowserKind::Chromium => {
                cmd.args(["--headless=new", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage", "--remote-debugging-address=127.0.0.1"])
                    .arg(format!("--remote-debugging-port={}", self.s.port))
                    .arg(format!("--user-data-dir={}", std::env::temp_dir().join("xtb-chrome").display()))
                    .arg("about:blank");
            }
        }
        cmd.args(&self.s.extra_args).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit()).kill_on_drop(true);
        cmd
    }

    /// Lightpanda accepts a WebSocket on the root; Chromium announces its URL at /json/version.
    async fn ws_url(&self) -> String {
        if self.s.kind == BrowserKind::Chromium {
            if let Ok(resp) = reqwest::get(format!("http://127.0.0.1:{}/json/version", self.s.port)).await {
                if let Ok(v) = resp.json::<Value>().await {
                    if let Some(u) = v["webSocketDebuggerUrl"].as_str() {
                        return u.to_string();
                    }
                }
            }
        }
        format!("ws://127.0.0.1:{}", self.s.port)
    }

    async fn ensure(&mut self) -> Result<()> {
        if let Some(c) = self.child.as_mut() {
            if c.try_wait()?.is_some() {
                warn!("browser process exited; restarting");
                self.child = None;
                self.cdp = None;
            }
        }
        if self.child.is_none() {
            let child = self.command().spawn().map_err(|e| anyhow!("cannot start browser {}: {e}", self.s.bin))?;
            info!(pid = child.id(), kind = ?self.s.kind, "browser started");
            self.child = Some(child);
        }
        if self.cdp.is_none() {
            let mut last = None;
            for _ in 0..60 {
                match connect_async(self.ws_url().await).await {
                    Ok((ws, _)) => {
                        self.cdp = Some(Cdp { ws, next_id: 0, events: Vec::new() });
                        return Ok(());
                    }
                    Err(e) => {
                        last = Some(e);
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            self.shutdown().await;
            bail!("could not connect to browser CDP: {last:?}");
        }
        Ok(())
    }

    /// Opens the For-you page and returns the posts found. Restarts the browser on any failure.
    pub async fn fetch_for_you(&mut self) -> Result<Vec<FeedItem>> {
        if self.in_flight {
            // The previous fetch was cancelled mid-flight and its page is still open; Lightpanda refuses a
            // second page ("TargetAlreadyLoaded"), so start from a clean browser.
            warn!("previous browser fetch was interrupted; restarting the browser");
            self.shutdown().await;
        }
        self.in_flight = true;
        let result = self.fetch_for_you_inner().await;
        self.in_flight = false;
        result
    }

    async fn fetch_for_you_inner(&mut self) -> Result<Vec<FeedItem>> {
        self.ensure().await?;
        let pt = self.s.page_timeout;
        // load budget + the in-page waiting/scrolling the script does
        let budget = pt + self.s.wait + Duration::from_millis(self.s.scrolls as u64 * 2000 + 6000);
        let js = script(&self.s);
        let s = self.s.clone();
        let cdp = self.cdp.as_mut().ok_or_else(|| anyhow!("no CDP connection"))?;
        let r = timeout(budget, fetch_inner(cdp, &s, &js, budget)).await;
        let json = match r {
            Ok(Ok(j)) => j,
            Ok(Err(e)) => {
                self.shutdown().await;
                return Err(e);
            }
            Err(_) => {
                self.shutdown().await;
                bail!("page load timed out after {}s", budget.as_secs());
            }
        };
        parse_page(&json)
    }

    pub async fn shutdown(&mut self) {
        self.cdp = None;
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
            let _ = timeout(Duration::from_secs(5), c.wait()).await;
            info!("browser stopped");
        }
    }
}

async fn fetch_inner(cdp: &mut Cdp, s: &BrowserSettings, js: &str, budget: Duration) -> Result<String> {
    let step = Duration::from_secs(30).min(s.page_timeout);
    let r = cdp.call("Target.createTarget", json!({ "url": "about:blank" }), None, step).await?;
    let target = r["targetId"].as_str().ok_or_else(|| anyhow!("no targetId"))?.to_string();
    let res = navigate_and_extract(cdp, &target, s, js, step, budget).await;
    let _ = cdp.call("Target.closeTarget", json!({ "targetId": target }), None, Duration::from_secs(5)).await;
    cdp.events.clear();
    res
}

async fn navigate_and_extract(cdp: &mut Cdp, target: &str, s: &BrowserSettings, js: &str, step: Duration, budget: Duration) -> Result<String> {
    let r = cdp.call("Target.attachToTarget", json!({ "targetId": target, "flatten": true }), None, step).await?;
    let sid = r["sessionId"].as_str().ok_or_else(|| anyhow!("no sessionId"))?.to_string();
    let sess = Some(sid.as_str());
    cdp.call("Page.enable", json!({}), sess, step).await?;
    cdp.call("Network.enable", json!({}), sess, step).await?;
    if s.insensitive {
        // Runs before X's own scripts on every document; Lightpanda/Chromium both speak this CDP method.
        if let Err(e) = cdp.call("Page.addScriptToEvaluateOnNewDocument", json!({ "source": INSENSITIVE_JS }), sess, step).await {
            warn!("InsensitiveX: could not register the script for new documents ({e}); will inject after load instead");
        }
    }
    let secure = s.url.starts_with("https://");
    // The cookie values are secrets: they are never logged, and error messages below never include them.
    for (name, value, http_only) in [("auth_token", s.auth_token.as_str(), true), ("ct0", s.ct0.as_str(), false), ("lang", "en", false)] {
        let result = cdp
            .call(
                "Network.setCookie",
                json!({ "name": name, "value": value, "domain": s.cookie_domain, "path": "/", "secure": secure, "httpOnly": http_only, "sameSite": "Lax" }),
                sess,
                step,
            )
            .await
            .map_err(|_| anyhow!("browser failed to set the {name} cookie"))?;
        if result.get("success").and_then(Value::as_bool) == Some(false) {
            bail!("browser rejected the {name} cookie");
        }
    }
    cdp.events.clear();
    let nav = cdp.call("Page.navigate", json!({ "url": s.url }), sess, step).await?;
    if let Some(err) = nav.get("errorText").and_then(Value::as_str) {
        bail!("navigation failed: {err}");
    }
    if !cdp.wait_event(&["Page.loadEventFired", "Page.frameStoppedLoading"], s.page_timeout.min(Duration::from_secs(45))).await {
        warn!("no load event seen; extracting anyway");
    }
    if s.insensitive {
        // Fallback / second chance: the script is guarded, so running it twice is harmless. It still
        // covers the requests made while scrolling when the early registration above was refused.
        if let Err(e) = cdp.call("Runtime.evaluate", json!({ "expression": INSENSITIVE_JS, "returnByValue": true }), sess, step).await {
            warn!("InsensitiveX: injection after load failed ({e})");
        }
    }
    let r = cdp
        .call("Runtime.evaluate", json!({ "expression": js, "returnByValue": true, "awaitPromise": true }), sess, budget)
        .await?;
    if let Some(ex) = r.get("exceptionDetails") {
        bail!("extraction script threw: {ex}");
    }
    r["result"]["value"].as_str().map(String::from).ok_or_else(|| anyhow!("extraction returned no string value"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(status: &str, posts: Value) -> String {
        json!({"status": status, "url": "https://x.com/home", "title": "Home / X", "posts": posts}).to_string()
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
    fn fuzz_page_parser_never_panics() {
        let mut r = Rng(7);
        let tokens = ["{", "}", "\"status\"", "\"ok\"", "\"posts\"", "[", "]", ":", ",", "\"id\"", "\"handle\"", "\"context\"", "123", "null", "true", "\"x\""];
        for _ in 0..40_000 {
            let s = r.text(&tokens, 30);
            let _ = parse_page(&s);
            let _ = repost_label(&s);
        }
        let strs = ["", "ok", "login", "x reposted", "bad name", "123", "12x", "🎉 reposted"];
        for _ in 0..20_000 {
            let f = |r: &mut Rng| -> Value {
                match r.next() % 5 {
                    0 => Value::Null,
                    1 => json!(r.next() % 99),
                    2 => json!(r.next() % 2 == 0),
                    3 => json!([1, "a"]),
                    _ => json!(*r.pick(&strs)),
                }
            };
            let v = json!({"status": "ok", "title": f(&mut r), "posts": [{"id": f(&mut r), "handle": f(&mut r), "context": f(&mut r), "promoted": f(&mut r)}, f(&mut r)]});
            let _ = parse_page(&v.to_string());
        }
    }

    #[test]
    fn parses_posts_reposts_and_skips_ads_and_garbage() {
        let j = page(
            "ok",
            json!([
                {"id":"111","handle":"NASA","context":"","promoted":false},
                {"id":"222","handle":"esa","context":"Jane Doe reposted","promoted":false},
                {"id":"333","handle":"brand","context":"","promoted":true},
                {"id":"444","handle":"bad name!","context":"","promoted":false},
                {"id":"55x","handle":"ok","context":"","promoted":false},
                {"id":"111","handle":"NASA","context":"","promoted":false},
                {"id":"666","handle":"ana","context":"Bob liked","promoted":false},
                {"handle":"nope"}
            ]),
        );
        let items = parse_page(&j).unwrap();
        assert_eq!(
            items,
            vec![
                FeedItem { handle: "NASA".into(), id: "111".into(), reposter: None },
                FeedItem { handle: "esa".into(), id: "222".into(), reposter: Some("Jane Doe".into()) },
                FeedItem { handle: "ana".into(), id: "666".into(), reposter: None },
            ]
        );
        assert!(parse_page(&page("ok", json!([]))).unwrap().is_empty());
    }

    #[test]
    fn explains_login_error_ratelimit_and_unrendered_pages() {
        for (status, needle) in [("login", "X_AUTH_TOKEN"), ("error", "Something went wrong"), ("ratelimit", "Rate limit"), ("loading", "did not render")] {
            let e = parse_page(&page(status, json!([]))).unwrap_err().to_string();
            assert!(e.contains(needle), "{status}: {e}");
        }
        assert!(parse_page("not json").is_err());
        assert!(parse_page("{}").unwrap_err().to_string().contains("did not render"));
    }

    #[test]
    fn insensitive_script_is_embedded() {
        assert!(INSENSITIVE_JS.contains("possibly_sensitive") && INSENSITIVE_JS.contains("tombstone"));
        assert!(INSENSITIVE_JS.contains("__insensitiveX") && INSENSITIVE_JS.contains("__agebypassX"), "must be idempotent");
        assert!(INSENSITIVE_JS.contains("age_verification_gate_enabled"));
        assert!(!INSENSITIVE_JS.contains("MutationObserver("), "no UI code in the headless build");
    }

    #[test]
    fn repost_labels() {
        assert_eq!(repost_label("Jane Doe reposted").as_deref(), Some("Jane Doe"));
        assert_eq!(repost_label("  You reposted ").as_deref(), Some("You"));
        assert_eq!(repost_label("Pinned"), None);
        assert_eq!(repost_label("Bob liked"), None);
        assert_eq!(repost_label(" reposted"), None);
        assert_eq!(repost_label(&format!("{} reposted", "x".repeat(80))), None);
    }

    #[test]
    fn script_placeholders_are_all_replaced() {
        let get = |k: &str| match k {
            "TELEGRAM_BOT_TOKEN" => Some("t".to_string()),
            "FOR_YOU" => Some("true".to_string()),
            "DEFAULT_CHAT_ID" => Some("@c".to_string()),
            "X_AUTH_TOKEN" => Some("a".to_string()),
            "X_CT0" => Some("b".to_string()),
            "FOR_YOU_SCROLLS" => Some("7".to_string()),
            "FOR_YOU_WAIT_SECS" => Some("12".to_string()),
            "FOR_YOU_SELECT_TAB" => Some("false".to_string()),
            _ => None,
        };
        let cfg = crate::config::Config::from_lookup(&get, true).unwrap();
        let js = script(&cfg.for_you.unwrap().browser);
        assert!(!js.contains("__"), "unreplaced placeholder");
        assert!(js.contains("WAIT = 12000, SCROLLS = 7, PICK_TAB = false"));
    }
}
