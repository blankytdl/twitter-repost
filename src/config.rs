use anyhow::{anyhow, bail, Result};
use std::{path::PathBuf, time::Duration};

const DEFAULT_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0";

/// One followed X account and the Telegram chat it is forwarded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub handle: String,
    pub chat_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrowserKind {
    Lightpanda,
    Chromium,
}

/// Settings for reading the logged-in "For you" feed through a headless browser.
/// Deliberately no `Debug` derive: holds the X login cookies (treat them like a password).
#[derive(Clone)]
pub struct BrowserSettings {
    pub kind: BrowserKind,
    pub bin: String,
    pub port: u16,
    pub extra_args: Vec<String>,
    pub page_timeout: Duration,
    pub auth_token: String,
    pub ct0: String,
    pub url: String,
    pub cookie_domain: String,
    pub scrolls: u32,
    pub wait: Duration,
    pub pick_tab: bool,
    /// Inject the InsensitiveX script (removes sensitive-content / age warnings from X's API answers).
    pub insensitive: bool,
    pub recycle_cycles: u64,
}

#[derive(Clone)]
pub struct ForYou {
    pub chat_id: String,
    pub max_new: usize,
    pub browser: BrowserSettings,
}

/// Deliberately no `Debug` derive: holds secrets.
#[derive(Clone)]
pub struct Config {
    pub for_you: Option<ForYou>,
    pub accounts: Vec<Account>,
    pub interval: Duration,
    pub cycle_budget: Duration,
    pub request_gap: Duration,
    pub send_gap: Duration,
    pub flush_min: Duration,
    pub rss_sources: Vec<String>,
    pub fx_base: String,
    pub user_agent: String,
    pub telegram_token: String,
    pub telegram_base: String,
    pub state_path: PathBuf,
    pub port: u16,
    pub self_ping: Option<(String, Duration)>,
    pub max_upload_bytes: u64,
    pub max_photo_bytes: u64,
    pub send_backlog: bool,
    pub media_only: bool,
    pub include_replies: bool,
    pub include_reposts: bool,
    pub captions: bool,
    pub max_age: Duration,
    pub min_likes: u64,
    pub likes_wait: Duration,
    pub media_hosts: Vec<String>,
    /// Testing only: allow http:// media URLs (MEDIA_ALLOW_HTTP=true).
    pub allow_http_media: bool,
    pub seen_cap: usize,
}

type Get<'a> = &'a dyn Fn(&str) -> Option<String>;

fn num<T>(get: Get, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match get(key) {
        None => Ok(default),
        Some(v) => v.trim().parse::<T>().map_err(|e| anyhow!("{key}: {e}")),
    }
}

fn truthy(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

fn flag(get: Get, key: &str, default: bool) -> bool {
    get(key).map(|v| truthy(&v)).unwrap_or(default)
}

fn s(get: Get, key: &str, default: &str) -> String {
    get(key).unwrap_or_else(|| default.to_string())
}

pub fn env_get(k: &str) -> Option<String> {
    std::env::var(k).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn list(v: &str) -> Vec<String> {
    v.split([',', '\n']).map(str::trim).filter(|x| !x.is_empty()).map(String::from).collect()
}

/// Accepts `NASA`, `@NASA`, `x.com/NASA`, `https://twitter.com/NASA/`.
pub fn norm_handle(n: &str) -> Result<String> {
    let mut n = n.trim();
    for p in ["https://", "http://"] {
        n = n.strip_prefix(p).unwrap_or(n);
    }
    for p in ["www.", "mobile.", "x.com/", "twitter.com/"] {
        n = n.strip_prefix(p).unwrap_or(n);
    }
    let n = n.trim_start_matches('@').trim_matches('/');
    if !crate::x_source::valid_handle(n) {
        bail!("invalid X username {n:?} (1-15 letters, digits or underscore)");
    }
    Ok(n.to_string())
}

/// Accepts the bare value or `name=value` / trailing `;` as copied from browser dev tools.
fn cookie_value(raw: &str, name: &str) -> Result<String> {
    let v = raw.trim();
    let v = v.strip_prefix(&format!("{name}=")).unwrap_or(v).trim_end_matches(';').trim();
    if v.is_empty() || v.len() > 4096 || v.chars().any(|c| c.is_whitespace() || matches!(c, ';' | '"' | '\'' | '\\')) {
        bail!("the {name} cookie value looks malformed (paste just the value, no quotes, spaces or semicolons)");
    }
    Ok(v.to_string())
}

fn browser_settings(get: Get) -> Result<BrowserSettings> {
    let kind = match s(get, "BROWSER_KIND", "lightpanda").to_ascii_lowercase().as_str() {
        "lightpanda" => BrowserKind::Lightpanda,
        "chromium" | "chrome" => BrowserKind::Chromium,
        other => bail!("BROWSER_KIND must be lightpanda or chromium, got {other:?}"),
    };
    let default_bin = match kind {
        BrowserKind::Lightpanda => "/usr/local/bin/lightpanda",
        BrowserKind::Chromium => "chromium",
    };
    let bin = get("BROWSER_BIN").or_else(|| get("LIGHTPANDA_BIN")).unwrap_or_else(|| default_bin.to_string());
    let auth = get("X_AUTH_TOKEN").ok_or_else(|| anyhow!("FOR_YOU=true needs X_AUTH_TOKEN (the auth_token cookie of a logged-in x.com session)"))?;
    let ct0 = get("X_CT0").ok_or_else(|| anyhow!("FOR_YOU=true needs X_CT0 (the ct0 cookie of the same session)"))?;
    let url = s(get, "FOR_YOU_URL", "https://x.com/home");
    if !(url.starts_with("https://") || url.starts_with("http://")) {
        bail!("FOR_YOU_URL must start with https://");
    }
    Ok(BrowserSettings {
        kind,
        bin,
        port: num(get, "BROWSER_PORT", 9222u16)?,
        extra_args: get("BROWSER_EXTRA_ARGS").map(|v| v.split_whitespace().map(String::from).collect()).unwrap_or_default(),
        page_timeout: Duration::from_secs(num(get, "PAGE_TIMEOUT_SECS", 90u64)?.clamp(10, 3600)),
        auth_token: cookie_value(&auth, "auth_token")?,
        ct0: cookie_value(&ct0, "ct0")?,
        url,
        cookie_domain: s(get, "X_COOKIE_DOMAIN", ".x.com"),
        scrolls: num(get, "FOR_YOU_SCROLLS", 4u32)?.min(30),
        wait: Duration::from_secs(num(get, "FOR_YOU_WAIT_SECS", 25u64)?.clamp(3, 120)),
        pick_tab: flag(get, "FOR_YOU_SELECT_TAB", true),
        insensitive: flag(get, "INSENSITIVE_X", true),
        recycle_cycles: num(get, "BROWSER_RECYCLE_CYCLES", 12u64)?.max(1),
    })
}

impl Config {
    pub fn from_env(require_accounts: bool) -> Result<Self> {
        Self::from_lookup(&env_get, require_accounts)
    }

    pub fn from_lookup(get: Get, require_accounts: bool) -> Result<Self> {
        let default_chat = get("DEFAULT_CHAT_ID");
        let max_accounts: usize = num(get, "MAX_ACCOUNTS", 100usize)?.clamp(1, 500);

        // X_ACCOUNTS="nasa, spacex=@otherchannel, @esa"
        let mut accounts: Vec<Account> = Vec::new();
        if let Some(v) = get("X_ACCOUNTS") {
            for item in list(&v) {
                let (name, chat) = match item.split_once('=') {
                    Some((n, c)) => (n, Some(c.trim()).filter(|c| !c.is_empty())),
                    None => (item.as_str(), None),
                };
                let handle = norm_handle(name)?;
                let chat_id = chat
                    .map(String::from)
                    .or_else(|| default_chat.clone())
                    .ok_or_else(|| anyhow!("account {handle}: no chat id given and DEFAULT_CHAT_ID is not set"))?;
                if !accounts.iter().any(|a| a.handle.eq_ignore_ascii_case(&handle) && a.chat_id == chat_id) {
                    accounts.push(Account { handle, chat_id });
                }
            }
        }
        if accounts.len() > max_accounts {
            bail!("{} accounts configured but MAX_ACCOUNTS={max_accounts}; raise it if you really want more", accounts.len());
        }
        let for_you = if flag(get, "FOR_YOU", false) {
            let chat_id = get("FOR_YOU_CHAT_ID")
                .or_else(|| default_chat.clone())
                .ok_or_else(|| anyhow!("FOR_YOU=true: set FOR_YOU_CHAT_ID (or DEFAULT_CHAT_ID)"))?;
            Some(ForYou { chat_id, max_new: num(get, "FOR_YOU_MAX_NEW", 40usize)?.clamp(1, 200), browser: browser_settings(get)? })
        } else {
            None
        };
        if require_accounts && accounts.is_empty() && for_you.is_none() {
            bail!("nothing to forward: set X_ACCOUNTS=nasa,spacex (and DEFAULT_CHAT_ID=@yourchannel) and/or FOR_YOU=true");
        }

        let telegram_token = get("TELEGRAM_BOT_TOKEN").ok_or_else(|| anyhow!("TELEGRAM_BOT_TOKEN is required"))?;

        let rss_sources: Vec<String> = list(&s(get, "RSS_SOURCES", "https://xcancel.com"))
            .into_iter()
            .map(|u| u.trim_end_matches('/').to_string())
            .collect();
        if rss_sources.is_empty() {
            bail!("RSS_SOURCES is empty");
        }
        if let Some(bad) = rss_sources.iter().find(|u| !(u.starts_with("https://") || u.starts_with("http://"))) {
            bail!("RSS_SOURCES entry {bad:?} must start with https:// (or http:// for a self-hosted instance)");
        }

        let interval_secs = num(get, "POLL_INTERVAL_SECS", if for_you.is_some() { 600u64 } else { 300u64 })?.clamp(60, 7 * 24 * 3600);

        let self_ping = if flag(get, "SELF_PING_ENABLED", true) {
            get("RENDER_EXTERNAL_URL").map(|u| {
                (
                    u.trim_end_matches('/').to_string(),
                    Duration::from_secs(num(get, "SELF_PING_INTERVAL_SECS", 600u64).unwrap_or(600).clamp(5, 86400)),
                )
            })
        } else {
            None
        };

        let max_age = Duration::from_secs(num(get, "MAX_AGE_HOURS", 24u64)?.saturating_mul(3600));
        // How many posts we examine (and must therefore remember) within the age window; an evicted id
        // that is still young enough to pass the age filter would be looked at again and re-sent.
        let per_window = max_age.as_secs().min(30 * 24 * 3600) / interval_secs + 1;
        let needed = for_you.as_ref().map(|f| f.max_new as u64 * per_window).unwrap_or(0) + accounts.len() as u64 * 40;
        let seen_cap = num(get, "SEEN_CAP", 20000usize)?.clamp(100, 100_000).max((needed * 13 / 10) as usize).min(100_000);

        Ok(Config {
            for_you,
            accounts,
            interval: Duration::from_secs(interval_secs),
            // Stop starting new accounts when this much of the interval is used; the rest
            // continue next cycle (round-robin), so any account count degrades gracefully.
            cycle_budget: Duration::from_secs(num(get, "CYCLE_BUDGET_SECS", interval_secs * 4 / 5)?.clamp(30, 7 * 24 * 3600)),
            request_gap: Duration::from_secs(num(get, "REQUEST_GAP_SECS", 3u64)?.min(3600)),
            send_gap: Duration::from_millis(num(get, "SEND_GAP_MS", 3000u64)?.min(3_600_000)),
            flush_min: Duration::from_secs(num(get, "STATE_FLUSH_MIN_SECS", 60u64)?.min(86400)),
            rss_sources,
            fx_base: s(get, "FX_API", "https://api.fxtwitter.com").trim_end_matches('/').to_string(),
            user_agent: s(get, "USER_AGENT", DEFAULT_UA),
            telegram_token,
            telegram_base: s(get, "TELEGRAM_API_BASE", "https://api.telegram.org").trim_end_matches('/').to_string(),
            state_path: get("STATE_PATH").map(PathBuf::from).unwrap_or_else(|| PathBuf::from(s(get, "DATA_DIR", "./data")).join("seen.json")),
            port: num(get, "PORT", 10000u16)?,
            self_ping,
            max_upload_bytes: num(get, "MAX_UPLOAD_BYTES", 50u64 * 1024 * 1024)?.min(50 * 1024 * 1024),
            max_photo_bytes: 10 * 1024 * 1024,
            send_backlog: flag(get, "SEND_BACKLOG_ON_FIRST_RUN", false),
            media_only: flag(get, "MEDIA_ONLY", true),
            include_replies: flag(get, "INCLUDE_REPLIES", false),
            include_reposts: flag(get, "INCLUDE_REPOSTS", true),
            captions: flag(get, "SEND_CAPTIONS", true),
            max_age,
            min_likes: num(get, "MIN_LIKES", 0u64)?,
            likes_wait: Duration::from_secs(num(get, "LIKES_WAIT_MIN", 60u64)?.saturating_mul(60)),
            media_hosts: list(&s(get, "MEDIA_HOSTS", "twimg.com")).into_iter().map(|h| h.to_ascii_lowercase()).collect(),
            allow_http_media: flag(get, "MEDIA_ALLOW_HTTP", false),
            seen_cap,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)], req: bool) -> Result<Config> {
        let m: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Config::from_lookup(&|k| m.get(k).cloned(), req)
    }

    #[test]
    fn defaults_and_account_parsing() {
        let c = cfg(
            &[
                ("TELEGRAM_BOT_TOKEN", "x"),
                ("DEFAULT_CHAT_ID", "@main"),
                ("X_ACCOUNTS", "NASA, @SpaceX ,esa=@other,https://x.com/NASA/,\nnasa"),
            ],
            true,
        )
        .unwrap();
        assert_eq!(
            c.accounts,
            vec![
                Account { handle: "NASA".into(), chat_id: "@main".into() },
                Account { handle: "SpaceX".into(), chat_id: "@main".into() },
                Account { handle: "esa".into(), chat_id: "@other".into() },
            ]
        );
        assert_eq!(c.interval.as_secs(), 300);
        assert_eq!(c.cycle_budget.as_secs(), 240);
        assert!(c.media_only && !c.include_replies && c.include_reposts && c.captions);
        assert_eq!(c.rss_sources, vec!["https://xcancel.com"]);
        assert_eq!(c.fx_base, "https://api.fxtwitter.com");
        assert_eq!(c.state_path, PathBuf::from("./data/seen.json"));
        assert!(c.self_ping.is_none() && !c.allow_http_media);
        assert_eq!(c.max_age.as_secs(), 86400);
    }

    #[test]
    fn validation_errors() {
        let base = [("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@c")];
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut v = base.to_vec();
            v.extend_from_slice(extra);
            cfg(&v, true)
        };
        assert!(with(&[]).is_err()); // nothing to forward
        assert!(with(&[("X_ACCOUNTS", "bad name!")]).is_err());
        assert!(with(&[("X_ACCOUNTS", "averyveryverylongusername")]).is_err());
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("X_ACCOUNTS", "nasa")], true).is_err()); // no chat
        assert!(cfg(&[("DEFAULT_CHAT_ID", "@c"), ("X_ACCOUNTS", "nasa")], true).is_err()); // no token
        assert!(with(&[("X_ACCOUNTS", "nasa"), ("RSS_SOURCES", "xcancel.com")]).is_err()); // no scheme
        assert!(with(&[("X_ACCOUNTS", "nasa"), ("POLL_INTERVAL_SECS", "abc")]).is_err());
        assert!(with(&[("X_ACCOUNTS", "a,b,c"), ("MAX_ACCOUNTS", "2")]).is_err());
        assert!(cfg(&base, false).unwrap().accounts.is_empty()); // allowed when accounts aren't required
    }

    #[test]
    fn overrides_and_self_ping() {
        let c = cfg(
            &[
                ("TELEGRAM_BOT_TOKEN", "x"),
                ("DEFAULT_CHAT_ID", "@c"),
                ("X_ACCOUNTS", "nasa"),
                ("POLL_INTERVAL_SECS", "10"),
                ("RSS_SOURCES", "https://a.example/, http://localhost:8080"),
                ("RENDER_EXTERNAL_URL", "https://svc.onrender.com/"),
                ("MEDIA_ONLY", "false"),
                ("MAX_UPLOAD_BYTES", "999999999"),
                ("MEDIA_HOSTS", "TwImg.com, cdn.example"),
            ],
            true,
        )
        .unwrap();
        assert_eq!(c.interval.as_secs(), 60); // clamped
        assert_eq!(c.rss_sources, vec!["https://a.example", "http://localhost:8080"]);
        assert_eq!(c.self_ping.as_ref().unwrap().0, "https://svc.onrender.com");
        assert!(!c.media_only);
        assert_eq!(c.max_upload_bytes, 50 * 1024 * 1024); // capped at Telegram's limit
        assert_eq!(c.media_hosts, vec!["twimg.com", "cdn.example"]);
    }

    #[test]
    fn for_you_settings_and_validation() {
        let base = [("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@c"), ("FOR_YOU", "true")];
        let with = |extra: &[(&'static str, &'static str)], req: bool| {
            let mut v = base.to_vec();
            v.extend_from_slice(extra);
            cfg(&v, req)
        };
        assert!(with(&[], true).is_err(), "cookies are required");
        assert!(with(&[("X_AUTH_TOKEN", "abc")], true).is_err());
        assert!(with(&[("X_AUTH_TOKEN", "abc"), ("X_CT0", "has space")], true).is_err());
        assert!(with(&[("X_AUTH_TOKEN", "a;b"), ("X_CT0", "c")], true).is_err());
        assert!(with(&[("X_AUTH_TOKEN", "abc"), ("X_CT0", "def"), ("BROWSER_KIND", "firefox")], true).is_err());
        assert!(with(&[("X_AUTH_TOKEN", "abc"), ("X_CT0", "def"), ("FOR_YOU_URL", "ftp://x")], true).is_err());
        let c = with(&[("X_AUTH_TOKEN", " auth_token=abc123; "), ("X_CT0", "ct0=def456")], true).unwrap();
        let fy = c.for_you.as_ref().unwrap();
        assert_eq!(fy.chat_id, "@c");
        assert_eq!((fy.browser.auth_token.as_str(), fy.browser.ct0.as_str()), ("abc123", "def456"));
        assert_eq!(fy.browser.url, "https://x.com/home");
        assert_eq!(fy.browser.kind, BrowserKind::Lightpanda);
        assert_eq!(fy.browser.bin, "/usr/local/bin/lightpanda");
        assert_eq!(c.interval.as_secs(), 600, "browser mode polls less often by default");
        assert!(c.accounts.is_empty());
        let c = with(&[("X_AUTH_TOKEN", "a"), ("X_CT0", "b"), ("BROWSER_KIND", "chrome"), ("FOR_YOU_CHAT_ID", "@fy"), ("X_ACCOUNTS", "nasa")], true).unwrap();
        assert_eq!(c.for_you.as_ref().unwrap().browser.kind, BrowserKind::Chromium);
        assert_eq!(c.for_you.as_ref().unwrap().browser.bin, "chromium");
        assert_eq!(c.for_you.as_ref().unwrap().chat_id, "@fy");
        assert_eq!(c.accounts.len(), 1);
        // without FOR_YOU the cookies are never required
        assert!(cfg(&[("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@c"), ("X_ACCOUNTS", "nasa")], true).unwrap().for_you.is_none());
    }

    #[test]
    fn absurd_numbers_do_not_overflow_or_panic() {
        let c = cfg(
            &[
                ("TELEGRAM_BOT_TOKEN", "x"),
                ("DEFAULT_CHAT_ID", "@c"),
                ("X_ACCOUNTS", "nasa"),
                ("MAX_AGE_HOURS", "18446744073709551615"),
                ("LIKES_WAIT_MIN", "18446744073709551615"),
                ("SEND_GAP_MS", "18446744073709551615"),
                ("POLL_INTERVAL_SECS", "18446744073709551615"),
                ("REQUEST_GAP_SECS", "18446744073709551615"),
            ],
            true,
        )
        .unwrap();
        assert!(c.max_age.as_secs() >= 365 * 24 * 3600);
        assert!(c.send_gap <= Duration::from_secs(3600) && c.request_gap <= Duration::from_secs(3600));
        assert!(c.interval <= Duration::from_secs(7 * 24 * 3600));
        let _ = c.send_gap * 10; // must not panic
    }

    #[test]
    fn seen_cap_always_covers_the_age_window() {
        // Posts must stay in the "seen" set for as long as the age filter would still let them through,
        // otherwise an evicted id is looked at again and re-sent.
        let base = [("TELEGRAM_BOT_TOKEN", "x"), ("DEFAULT_CHAT_ID", "@c"), ("FOR_YOU", "true"), ("X_AUTH_TOKEN", "a"), ("X_CT0", "b")];
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut v = base.to_vec();
            v.extend_from_slice(extra);
            cfg(&v, true).unwrap()
        };
        assert_eq!(with(&[]).seen_cap, 20000, "defaults need about 6000 per day: the default cap is plenty");
        let heavy = with(&[("FOR_YOU_MAX_NEW", "200"), ("POLL_INTERVAL_SECS", "300")]); // 57,600 posts looked at per day
        assert!(heavy.seen_cap >= 57_600, "cap {} too small for 24 h of examined posts", heavy.seen_cap);
        let long = with(&[("MAX_AGE_HOURS", "168"), ("FOR_YOU_MAX_NEW", "200")]);
        assert!(long.seen_cap >= 100_000 / 2 && long.seen_cap <= 100_000, "bounded: {}", long.seen_cap);
        let tiny = with(&[("SEEN_CAP", "50")]).seen_cap;
        assert!((5_800..20_000).contains(&tiny), "a too-small explicit cap is raised to what the window needs, got {tiny}");
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
    fn fuzz_config_parsers_never_panic() {
        let mut r = Rng(99);
        let tokens = ["@", "x.com/", "https://", "name=", "=", ",", "\n", " ", ";", "auth_token=", "ct0=", "abc", "0", "-1", "99999999999999999999", "NASA", "é"];
        for _ in 0..40_000 {
            let s = r.text(&tokens, 30);
            let _ = norm_handle(&s);
            let _ = cookie_value(&s, "auth_token");
            let _ = list(&s);
            let m: std::collections::HashMap<&str, String> = [
                ("TELEGRAM_BOT_TOKEN", "x".to_string()), ("DEFAULT_CHAT_ID", "@c".to_string()), ("X_ACCOUNTS", s.clone()),
                ("POLL_INTERVAL_SECS", s.clone()), ("MAX_AGE_HOURS", s.clone()), ("FOR_YOU", "true".to_string()),
                ("X_AUTH_TOKEN", s.clone()), ("X_CT0", s.clone()), ("RSS_SOURCES", s.clone()),
            ].into_iter().collect();
            let _ = Config::from_lookup(&|k| m.get(k).cloned(), true);
        }
    }

    #[test]
    fn handle_normalisation() {
        assert_eq!(norm_handle("  @Foo_1 ").unwrap(), "Foo_1");
        assert_eq!(norm_handle("https://www.twitter.com/Foo/").unwrap(), "Foo");
        assert_eq!(norm_handle("x.com/Foo").unwrap(), "Foo");
        assert!(norm_handle("").is_err());
        assert!(norm_handle("a b").is_err());
    }
}
