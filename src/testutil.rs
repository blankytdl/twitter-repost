//! Test-only fake of every outside service (RSS instances, FxEmbed, media CDN, Telegram, GitHub),
//! served from one local HTTP server so the real HTTP code paths are exercised.
use crate::config::Config;
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::Response,
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

type Rule = Box<dyn Fn(&str, &str) -> Option<(u16, &'static str)> + Send + Sync>;

#[derive(Default)]
pub struct Mock {
    pub port: u16,
    /// "feedA/nasa" -> (status, body)
    pub feeds: Mutex<HashMap<String, (u16, String)>>,
    /// post id -> (status, body)
    pub fx: Mutex<HashMap<String, (u16, String)>>,
    pub media: Mutex<HashMap<String, Vec<u8>>>,
    /// (telegram method, request body as lossy text)
    pub tg: Mutex<Vec<(String, String)>>,
    pub tg_rules: Mutex<Vec<Rule>>,
    pub gh_file: Mutex<Option<(String, Vec<u8>)>>,
    pub gh_branch: AtomicBool,
    pub gh_puts: AtomicUsize,
    pub gh_gets: AtomicUsize,
    pub gh_fail_get: AtomicBool,
    /// Mimic GitHub's >1 MB behaviour: the JSON answer has no content, the raw Accept header returns the bytes
    pub gh_big: AtomicBool,
    pub requests: Mutex<Vec<String>>,
    /// HTML pages by path (e.g. "/home"): a fake x.com
    pub pages: Mutex<HashMap<String, String>>,
    /// Cookie headers the fake x.com received
    pub cookies: Mutex<Vec<String>>,
}

impl Mock {
    pub fn calls(&self, method: &str) -> Vec<String> {
        self.tg.lock().unwrap().iter().filter(|(m, _)| m == method).map(|(_, b)| b.clone()).collect()
    }
    pub fn count(&self, method: &str) -> usize {
        self.calls(method).len()
    }
    pub fn total_tg(&self) -> usize {
        self.tg.lock().unwrap().len()
    }
    pub fn clear_tg(&self) {
        self.tg.lock().unwrap().clear();
    }
    pub fn fail_when(&self, f: impl Fn(&str, &str) -> Option<(u16, &'static str)> + Send + Sync + 'static) {
        self.tg_rules.lock().unwrap().push(Box::new(f));
    }
    pub fn clear_rules(&self) {
        self.tg_rules.lock().unwrap().clear();
    }
    pub fn set_feed(&self, key: &str, status: u16, body: String) {
        self.feeds.lock().unwrap().insert(key.to_string(), (status, body));
    }
    pub fn set_fx(&self, id: &str, status: u16, body: String) {
        self.fx.lock().unwrap().insert(id.to_string(), (status, body));
    }
    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.port, path)
    }
}

fn reply(status: u16, ctype: &str, body: Vec<u8>) -> Response {
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header(header::CONTENT_TYPE, ctype)
        .body(axum::body::Body::from(body))
        .unwrap()
}

fn json_reply(status: u16, v: Value) -> Response {
    reply(status, "application/json", v.to_string().into_bytes())
}

async fn handler(State(m): State<Arc<Mock>>, method: Method, uri: Uri, headers: HeaderMap, body: Bytes) -> Response {
    let path = uri.path().to_string();
    m.requests.lock().unwrap().push(format!("{method} {path}"));
    let seg: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match seg.as_slice() {
        [feed, user, "rss"] if feed.starts_with("feed") => {
            let key = format!("{feed}/{user}");
            match m.feeds.lock().unwrap().get(&key) {
                Some((s, b)) => reply(*s, "application/rss+xml", b.clone().into_bytes()),
                None => reply(404, "text/plain", b"nope".to_vec()),
            }
        }
        ["fx", _user, "status", id] => match m.fx.lock().unwrap().get(*id) {
            Some((s, b)) => reply(*s, "application/json", b.clone().into_bytes()),
            None => json_reply(404, json!({"code":404})),
        },
        [page] if m.pages.lock().unwrap().contains_key(&format!("/{page}")) => {
            let cookie = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
            m.cookies.lock().unwrap().push(cookie);
            let html = m.pages.lock().unwrap().get(&format!("/{page}")).cloned().unwrap_or_default();
            reply(200, "text/html; charset=utf-8", html.into_bytes())
        }
        ["redir_ok"] => Response::builder().status(302).header(header::LOCATION, "/media/a.jpg").body(axum::body::Body::empty()).unwrap(),
        ["redir_bad"] => Response::builder()
            .status(302)
            .header(header::LOCATION, format!("http://localhost:{}/media/secret.jpg", m.port))
            .body(axum::body::Body::empty())
            .unwrap(),
        ["media", name] => match m.media.lock().unwrap().get(*name) {
            Some(b) => reply(200, "application/octet-stream", b.clone()),
            None => reply(404, "text/plain", b"nope".to_vec()),
        },
        [bot, tgmethod] if bot.starts_with("bot") => {
            let text = String::from_utf8_lossy(&body).to_string();
            m.tg.lock().unwrap().push((tgmethod.to_string(), text.clone()));
            for rule in m.tg_rules.lock().unwrap().iter() {
                if let Some((code, desc)) = rule(tgmethod, &text) {
                    let params = if code == 429 { json!({"retry_after": 0}) } else { json!({}) };
                    return json_reply(code, json!({"ok":false,"error_code":code,"description":desc,"parameters":params}));
                }
            }
            json_reply(200, json!({"ok":true,"result":{"username":"testbot"}}))
        }
        ["repos", ..] => github(&m, &method, &path, &headers, &body),
        _ => reply(404, "text/plain", b"unknown".to_vec()),
    }
}

fn github(m: &Mock, method: &Method, path: &str, headers: &HeaderMap, body: &Bytes) -> Response {
    match (method.as_str(), path) {
        ("GET", "/repos/o/r/contents/seen_posts.json") => {
            m.gh_gets.fetch_add(1, Ordering::SeqCst);
            if m.gh_fail_get.load(Ordering::SeqCst) {
                return reply(500, "text/plain", b"boom".to_vec());
            }
            let wants_raw = headers.get_all(header::ACCEPT).iter().any(|v| v.to_str().unwrap_or("").contains("raw"));
            match &*m.gh_file.lock().unwrap() {
                Some((_, bytes)) if m.gh_big.load(Ordering::SeqCst) && wants_raw => reply(200, "application/octet-stream", bytes.clone()),
                Some((sha, bytes)) if m.gh_big.load(Ordering::SeqCst) => json_reply(200, json!({"sha": sha, "size": bytes.len(), "encoding": "none", "content": ""})),
                Some((sha, bytes)) => json_reply(200, json!({"sha": sha, "content": STANDARD.encode(bytes)})),
                None => json_reply(404, json!({"message":"Not Found"})),
            }
        }
        ("GET", "/repos/o/r/git/ref/heads/bot-state") => {
            if m.gh_branch.load(Ordering::SeqCst) {
                json_reply(200, json!({"object":{"sha":"x"}}))
            } else {
                json_reply(404, json!({}))
            }
        }
        ("GET", "/repos/o/r") => json_reply(200, json!({"default_branch":"main"})),
        ("GET", "/repos/o/r/git/ref/heads/main") => json_reply(200, json!({"object":{"sha":"abc"}})),
        ("POST", "/repos/o/r/git/refs") => {
            m.gh_branch.store(true, Ordering::SeqCst);
            json_reply(201, json!({}))
        }
        ("PUT", "/repos/o/r/contents/seen_posts.json") => {
            let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let bytes = STANDARD.decode(v["content"].as_str().unwrap_or("")).unwrap_or_default();
            let n = m.gh_puts.fetch_add(1, Ordering::SeqCst) + 1;
            let sha = format!("sha{n}");
            *m.gh_file.lock().unwrap() = Some((sha.clone(), bytes));
            json_reply(200, json!({"content":{"sha": sha}}))
        }
        _ => reply(404, "text/plain", b"unknown repo route".to_vec()),
    }
}

pub async fn start() -> Arc<Mock> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mock = Arc::new(Mock { port, ..Default::default() });
    let app = Router::new().fallback(handler).with_state(mock.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    mock
}

// ---------------------------------------------------------------- builders

pub fn snow(ago_secs: u64, n: u64) -> String {
    let ms = crate::x_source::now_ms() - ago_secs * 1000;
    (((ms - 1_288_834_974_657) << 22) | n).to_string()
}

pub fn rss(items: &[(&str, &str, &str)]) -> String {
    let body: String = items
        .iter()
        .map(|(user, id, title)| format!("<item><title>{title}</title><link>https://xcancel.com/{user}/status/{id}#m</link></item>"))
        .collect();
    format!(r#"<?xml version="1.0"?><rss version="2.0"><channel><title>t</title>{body}</channel></rss>"#)
}

pub struct FxSpec<'a> {
    pub handle: &'a str,
    pub text: &'a str,
    pub likes: u64,
    pub photos: Vec<&'a str>,
    pub videos: Vec<(&'a str, bool)>,
    pub reply: bool,
}

impl<'a> FxSpec<'a> {
    pub fn new(handle: &'a str, text: &'a str) -> Self {
        Self { handle, text, likes: 3, photos: vec![], videos: vec![], reply: false }
    }
    pub fn photo(mut self, n: &'a str) -> Self {
        self.photos.push(n);
        self
    }
    pub fn video(mut self, n: &'a str, gif: bool) -> Self {
        self.videos.push((n, gif));
        self
    }
    pub fn likes(mut self, l: u64) -> Self {
        self.likes = l;
        self
    }
    pub fn reply(mut self) -> Self {
        self.reply = true;
        self
    }
    pub fn json(&self, port: u16) -> String {
        let u = |n: &str| format!("http://127.0.0.1:{port}/media/{n}");
        json!({"code":200,"tweet":{
            "text": self.text, "likes": self.likes,
            "replying_to": if self.reply { json!("someone") } else { Value::Null },
            "author": {"screen_name": self.handle, "name": format!("{} Name", self.handle)},
            "media": {
                "photos": self.photos.iter().map(|n| json!({"url": u(n)})).collect::<Vec<_>>(),
                "videos": self.videos.iter().map(|(n, g)| json!({"url": u(n), "type": if *g {"gif"} else {"video"}})).collect::<Vec<_>>(),
            }}})
        .to_string()
    }
}

/// Config pointing at the mock. `feeds` = number of RSS sources (feedA, feedB, ...).
pub fn config(m: &Mock, state: &std::path::Path, extra: &[(&str, &str)]) -> Config {
    let sources = ["feedA", "feedB", "feedC"].iter().map(|f| m.url(&format!("/{f}"))).collect::<Vec<_>>().join(",");
    let base = m.url("");
    let mut map: HashMap<String, String> = [
        ("TELEGRAM_BOT_TOKEN", "TESTTOKEN"),
        ("TELEGRAM_API_BASE", base.as_str()),
        ("FX_API", &m.url("/fx")),
        ("RSS_SOURCES", sources.as_str()),
        ("DEFAULT_CHAT_ID", "@chan"),
        ("X_ACCOUNTS", "nasa"),
        ("MEDIA_HOSTS", "127.0.0.1"),
        ("MEDIA_ALLOW_HTTP", "true"),
        ("SEND_GAP_MS", "0"),
        ("REQUEST_GAP_SECS", "0"),
        ("STATE_PATH", state.to_str().unwrap()),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    for (k, v) in extra {
        map.insert(k.to_string(), v.to_string());
    }
    Config::from_lookup(&|k| map.get(k).cloned(), true).unwrap()
}

/// One post on the fake timeline.
pub struct FakePost<'a> {
    pub handle: &'a str,
    pub id: &'a str,
    pub context: &'a str,
    /// An ad: has a timestamp link AND the "Ad" label
    pub ad: bool,
    /// No timestamp link at all (cells like "who to follow")
    pub no_time: bool,
}

pub fn fp<'a>(handle: &'a str, id: &'a str) -> FakePost<'a> {
    FakePost { handle, id, context: "", ad: false, no_time: false }
}

/// A page that behaves like x.com in the ways the bot cares about: the timeline is rendered by
/// JavaScript after a delay (not present at load time), the list is *replaced* later (X's
/// virtualised list removes off-screen posts), and there is a For you / Following tab bar.
pub fn x_page(first: &[FakePost], second: &[FakePost]) -> String {
    let js = |l: &[FakePost]| {
        l.iter()
            .map(|p| {
                format!(
                    "{{h:{:?},id:{:?},ctx:{:?},ad:{},nt:{}}}",
                    p.handle, p.id, p.context, p.ad, p.no_time
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        r#"<!doctype html><html><head><title>Home / X</title></head><body><main id="m"></main>
<script>
const render = (list) => {{
  document.getElementById('m').innerHTML =
    '<div role="tablist"><div role="tab" aria-selected="true">For you</div><div role="tab" aria-selected="false">Following</div></div>' +
    list.map(p => '<article data-testid="tweet">' +
      (p.ctx ? '<div data-testid="socialContext">' + p.ctx + '</div>' : '') +
      (p.nt ? '' : '<a href="/' + p.h + '/status/' + p.id + '"><time>1h</time></a>') +
      (p.ad ? '<span>Ad</span>' : '') + '</article>').join('');
}};
setTimeout(() => render([{}]), 800);
setTimeout(() => render([{}]), 3500);
</script></body></html>"#,
        js(first),
        js(second)
    )
}

pub const LOGIN_PAGE: &str = r#"<!doctype html><html><head><title>X</title></head><body><a href="/login">Log in</a></body></html>"#;
pub const ERROR_PAGE: &str = r#"<!doctype html><html><head><title>X</title></head><body><div data-testid="error-detail">Something went wrong. Try reloading.</div></body></html>"#;

/// Like `x_page`, but X opens on the Following tab; clicking "For you" swaps the list.
pub fn x_page_following_first(following: &[FakePost], for_you: &[FakePost]) -> String {
    let js = |l: &[FakePost]| l.iter().map(|p| format!("{{h:{:?},id:{:?}}}", p.handle, p.id)).collect::<Vec<_>>().join(",");
    format!(
        r#"<!doctype html><html><head><title>Home / X</title></head><body><div role="tablist">
<div role="tab" id="fy" aria-selected="false">For you</div><div role="tab" id="fo" aria-selected="true">Following</div></div><main id="m"></main>
<script>
const show = (list) => {{ document.getElementById('m').innerHTML = list.map(p => '<article data-testid="tweet"><a href="/' + p.h + '/status/' + p.id + '"><time>1h</time></a></article>').join(''); }};
setTimeout(() => show([{}]), 600);
document.getElementById('fy').onclick = () => {{ document.getElementById('fy').setAttribute('aria-selected', 'true'); document.getElementById('fo').setAttribute('aria-selected', 'false'); show([{}]); }};
</script></body></html>"#,
        js(following),
        js(for_you)
    )
}
