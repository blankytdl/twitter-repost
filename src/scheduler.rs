use crate::{
    config::{Account, Config},
    deliver::Deliverer,
    health::Health,
    seen_store::SeenStore,
    x_browser::Browser,
    x_source::{fetch_post, id_age_secs, now_ms, FeedItem, Sources},
};
use reqwest::Client;
use std::{collections::HashMap, sync::Arc, time::Duration};
#[cfg(test)]
use std::sync::atomic::Ordering;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub fn next_penalty(prev: Duration) -> Duration {
    if prev.is_zero() {
        Duration::from_secs(300)
    } else {
        (prev * 2).min(Duration::from_secs(3600))
    }
}

/// Per-channel + account key used to remember that an account has been "seeded".
pub fn sub_key(chat: &str, handle: &str) -> String {
    format!("{chat}|{handle}").to_ascii_lowercase()
}

/// Seen-tracking is per destination channel, so the same post can go to two channels.
pub fn seen_key(chat: &str, id: &str) -> String {
    format!("{chat}|{id}")
}

/// Failures that look like an outage or throttling (network, 5xx, 429) are temporary: they must not use up a
/// post's attempts quickly, so a long Telegram/FxEmbed problem doesn't lose posts. Only a post that keeps
/// failing this way for this many cycles (an hour or more) is given up, so it can't block its account forever.
const TEMPORARY_GIVE_UP_AFTER: u8 = 10;
/// Failures about the post itself (Telegram 4xx, a 404 media file) are given up quickly, and meanwhile they
/// don't hold up the other posts.
const PERMANENT_GIVE_UP_AFTER: u8 = 3;

pub fn is_temporary(e: &anyhow::Error) -> bool {
    if let Some(t) = e.downcast_ref::<crate::telegram::TgError>() {
        return t.code == 0 || t.code == 429 || t.code >= 500;
    }
    let msg = e.to_string();
    for prefix in ["download failed: HTTP ", "FxEmbed HTTP "] {
        if let Some(rest) = msg.strip_prefix(prefix) {
            let code: u16 = rest.trim().parse().unwrap_or(0);
            return code == 0 || code == 408 || code == 429 || code >= 500;
        }
    }
    true // network errors, timeouts, invalid JSON from a struggling service...
}

enum Next {
    /// Finished with this post (sent or deliberately skipped).
    Continue,
    /// Not decided yet (waiting for likes): keep it for the next cycle, but don't count it as handled.
    Wait,
    /// Temporary problem: stop with this account to keep posts in order and retry next cycle.
    Stop,
}

pub struct Scheduler {
    cfg: Arc<Config>,
    http: Client,
    sources: Sources,
    browser: Option<Browser>,
    store: SeenStore,
    deliverer: Deliverer,
    health: Arc<Health>,
    token: CancellationToken,
    fail_counts: HashMap<String, u8>,
    cycles: u64,
    penalty: Duration,
    next_account: usize,
    /// The For-you source backs off on its own (a bad cookie must not be retried every cycle).
    fy_backoff: Duration,
    fy_next_try: Option<Instant>,
}

impl Scheduler {
    pub fn new(cfg: Arc<Config>, http: Client, store: SeenStore, deliverer: Deliverer, health: Arc<Health>, token: CancellationToken) -> Self {
        let sources = Sources::new(http.clone(), cfg.rss_sources.clone());
        let browser = cfg.for_you.as_ref().map(|f| Browser::new(f.browser.clone()));
        Self { cfg, http, sources, browser, store, deliverer, health, token, fail_counts: HashMap::new(), cycles: 0, penalty: Duration::ZERO, next_account: 0, fy_backoff: Duration::ZERO, fy_next_try: None }
    }

    pub async fn run(mut self) {
        loop {
            if self.token.is_cancelled() {
                break;
            }
            self.health.set_stage("cycle starting", 0);
            let started = Instant::now();
            // On SIGTERM give the cycle 20 s to wrap up (Render kills after ~30 s), then
            // abandon it so the final state flush still happens.
            let tok = self.token.clone();
            let hard = self.cfg.cycle_budget + Duration::from_secs(600);
            let outcome = tokio::select! {
                r = timeout(hard, self.cycle()) => Some(r),
                _ = async { tok.cancelled().await; sleep(Duration::from_secs(20)).await; } => None,
            };
            match outcome {
                Some(Ok(())) => {}
                Some(Err(_)) => warn!("cycle exceeded its hard timeout; continuing with the next cycle"),
                None => warn!("shutdown grace period elapsed; abandoning current cycle"),
            }
            if self.token.is_cancelled() {
                break;
            }
            self.cycles += 1;
            self.health.cycle_done();
            let wait = self.cfg.interval.saturating_sub(started.elapsed()).max(Duration::from_secs(30)) + self.penalty;
            self.health.set_stage("idle until next poll", wait.as_secs());
            info!(next_in_secs = wait.as_secs(), "cycle finished");
            // Console heartbeat only; no extra X/RSS requests or state writes.
            let mut remaining = wait;
            while !remaining.is_zero() {
                let tick = remaining.min(Duration::from_secs(60));
                tokio::select! {
                    _ = self.token.cancelled() => break,
                    _ = sleep(tick) => {
                        remaining = remaining.saturating_sub(tick);
                        if !remaining.is_zero() {
                            info!(next_in_secs = remaining.as_secs(), "waiting for next poll");
                        }
                    }
                }
            }
            if self.token.is_cancelled() {
                break;
            }
        }
        info!("shutting down: flushing state");
        if let Some(b) = self.browser.as_mut() {
            b.shutdown().await;
        }
        self.store.flush().await;
    }

    pub async fn cycle(&mut self) {
        let cfg = self.cfg.clone();
        let n = cfg.accounts.len();
        let started = Instant::now();
        let first = self.next_account; // fixed for this cycle; next_account moves while we loop
        let (mut attempted, mut failed, mut longest) = (0usize, 0usize, Duration::ZERO);
        for k in 0..n {
            if self.token.is_cancelled() {
                break;
            }
            // Time budget: stop starting accounts we can't finish; resume round-robin next cycle.
            if k > 0 && started.elapsed() + longest > cfg.cycle_budget {
                warn!(done = attempted, total = n, "cycle time budget reached; remaining accounts run next cycle (raise POLL_INTERVAL_SECS or follow fewer accounts)");
                break;
            }
            let ai = (first + k) % n;
            let acc = &cfg.accounts[ai];
            if k > 0 {
                sleep(cfg.request_gap).await;
            }
            let t0 = Instant::now();
            self.health.set_stage(&format!("fetching RSS feed for @{}", acc.handle), 0);
            attempted += 1;
            if let Err(e) = self.process_account(acc).await {
                failed += 1;
                warn!(account = %acc.handle, "feed failed: {e:#}");
            }
            self.store.flush_if_due(cfg.flush_min).await;
            self.next_account = (ai + 1) % n;
            longest = longest.max(t0.elapsed());
        }

        if cfg.for_you.is_some() && !self.token.is_cancelled() {
            match self.fy_next_try {
                Some(t) if Instant::now() < t => {
                    info!(retry_in_secs = t.saturating_duration_since(Instant::now()).as_secs(), "For you is backing off after a failure");
                }
                _ => {
                    self.health.set_stage("fetching the For you timeline", 0);
                    match self.process_for_you().await {
                        Ok(()) => {
                            self.fy_backoff = Duration::ZERO;
                            self.fy_next_try = None;
                        }
                        Err(e) => {
                            self.fy_backoff = next_penalty(self.fy_backoff);
                            self.fy_next_try = Some(Instant::now() + self.fy_backoff);
                            warn!(backoff_secs = self.fy_backoff.as_secs(), "For you feed failed: {e:#}");
                        }
                    }
                }
            }
        }

        self.health.set_stage("saving seen-post state", 0);
        self.store.flush().await;
        // Every account feed failed: the RSS instances are probably down or blocking us, so back off.
        // (The For-you source has its own backoff above and must not slow the account feeds.)
        self.penalty = if attempted > 0 && failed == attempted { next_penalty(self.penalty) } else { Duration::ZERO };
        info!(sources = attempted, failures = failed, secs = started.elapsed().as_secs(), "cycle summary");
    }

    fn mark_seen(&mut self, chat: &str, id: &str) {
        self.store.insert(&seen_key(chat, id));
    }

    /// Err only when the feed itself could not be read.
    async fn process_account(&mut self, acc: &Account) -> anyhow::Result<()> {
        let items = self.sources.fetch_feed(&acc.handle).await?;
        self.process_items(&acc.chat_id, &acc.handle, items, usize::MAX, false).await;
        Ok(())
    }

    /// The logged-in "For you" feed, discovered through the headless browser.
    async fn process_for_you(&mut self) -> anyhow::Result<()> {
        let fy = self.cfg.for_you.clone().ok_or_else(|| anyhow::anyhow!("For you is not enabled"))?;
        if self.cycles > 0 && self.cycles % fy.browser.recycle_cycles == 0 {
            if let Some(b) = self.browser.as_mut() {
                info!("recycling the browser (memory hygiene)");
                b.shutdown().await;
            }
        }
        let items = self.browser.as_mut().ok_or_else(|| anyhow::anyhow!("no browser"))?.fetch_for_you().await?;
        if items.is_empty() {
            warn!("For you page rendered but no posts were found; X's page layout may have changed");
        }
        // "@" can't appear in a username, so this never collides with an account.
        self.process_items(&fy.chat_id, "@foryou", items, fy.max_new, true).await;
        Ok(())
    }

    /// Shared by account feeds and the For-you feed. `label` identifies the source for seeding.
    async fn process_items(&mut self, chat: &str, label: &str, items: Vec<FeedItem>, max_new: usize, for_you: bool) {
        self.health.set_stage(&format!("processing {} posts from {label}", items.len()), 0);
        let known = self.store.sub_known(chat, label);

        if !known && !self.cfg.send_backlog {
            if items.is_empty() {
                // An empty answer (blocked instance, page not loaded) says nothing about what exists:
                // staying "unknown" means the next non-empty answer is seeded instead of flooding the channel.
                warn!(source = %label, "first run: empty feed, will seed on the next non-empty one");
                return;
            }
            for it in &items {
                self.mark_seen(chat, &it.id);
            }
            self.store.mark_sub(&sub_key(chat, label));
            info!(source = %label, posts = items.len(), "first run: existing posts marked seen");
            return;
        }

        let mut fresh: Vec<FeedItem> = items.into_iter().filter(|i| !self.store.contains(&seen_key(chat, &i.id))).collect();
        // Account feeds go out in chronological order. The algorithmic For-you feed is capped per cycle,
        // and that budget must go to the freshest posts, so it is worked newest first.
        if for_you {
            fresh.sort_by_key(|i| std::cmp::Reverse(i.id.parse::<u64>().unwrap_or(0)));
        } else {
            fresh.sort_by_key(|i| i.id.parse::<u64>().unwrap_or(0));
        }
        let total = fresh.len();
        let mut handled = 0usize;
        for it in fresh {
            if self.store.contains(&seen_key(chat, &it.id)) {
                continue;
            }
            if handled >= max_new {
                break; // the rest waits for the next cycle
            }
            match self.handle_item(chat, &it, for_you).await {
                Next::Continue => handled += 1,
                Next::Wait => {} // doesn't use up the budget, so waiting posts can't starve newer ones
                Next::Stop => break,
            }
        }
        if !known {
            self.store.mark_sub(&sub_key(chat, label));
        }
        if total > 0 {
            info!(source = %label, new = total, handled, "source processed");
        }
    }

    #[cfg(test)]
    async fn process_items_for_test(&mut self, chat: &str, handle: &str, max_new: usize) {
        let items = self.sources.fetch_feed(handle).await.unwrap();
        self.process_items(chat, handle, items, max_new, false).await;
    }

    fn failed(&mut self, chat: &str, id: &str, why: &anyhow::Error) -> Next {
        let temporary = is_temporary(why);
        let limit = if temporary { TEMPORARY_GIVE_UP_AFTER } else { PERMANENT_GIVE_UP_AFTER };
        if self.fail_counts.len() > 1000 {
            self.fail_counts.clear();
        }
        let n = self.fail_counts.entry(seen_key(chat, id)).or_insert(0);
        *n += 1;
        warn!(post = %id, attempt = *n, "failed: {why:#}");
        if *n >= limit {
            warn!(post = %id, "giving up on post after {limit} failed cycles");
            self.mark_seen(chat, id);
            Next::Continue
        } else if temporary {
            Next::Stop // an outage: stop here, keep the order, don't hammer the service
        } else {
            Next::Continue // only this post is the problem: the others go ahead (it stays unseen for the next cycle)
        }
    }

    async fn handle_item(&mut self, chat: &str, it: &FeedItem, for_you: bool) -> Next {
        let cfg = self.cfg.clone();
        let age = id_age_secs(&it.id, now_ms()).unwrap_or(0);
        // `age` is the age of the ORIGINAL post. In an account feed a repost appearing now is new
        // activity even if the original is old, so only the algorithmic For-you feed (full of old
        // posts) is filtered by it.
        let fresh_repost = it.reposter.is_some() && !for_you;
        if age > cfg.max_age.as_secs() && !fresh_repost {
            self.mark_seen(chat, &it.id);
            return Next::Continue;
        }
        if it.reposter.is_some() && !cfg.include_reposts {
            self.mark_seen(chat, &it.id);
            return Next::Continue;
        }
        self.health.set_stage(&format!("fetching details for X post {}", it.id), 0);
        let post = match fetch_post(&self.http, &cfg.fx_base, &it.handle, &it.id, it.reposter.clone()).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                self.mark_seen(chat, &it.id); // deleted / private
                return Next::Continue;
            }
            Err(e) => return self.failed(chat, &it.id, &e),
        };
        if (post.reply && !cfg.include_replies) || (cfg.media_only && !post.has_media()) {
            self.mark_seen(chat, &it.id);
            return Next::Continue;
        }
        if post.likes < cfg.min_likes {
            // Young posts are still collecting likes: look again next cycle; drop it once it is old.
            if age >= cfg.likes_wait.as_secs() {
                self.mark_seen(chat, &it.id);
                return Next::Continue;
            }
            return Next::Wait;
        }
        self.health.set_stage(&format!("sending X post {}", it.id), 0);
        match self.deliverer.deliver(chat, &post).await {
            Ok(()) => {
                self.mark_seen(chat, &it.id);
                self.fail_counts.remove(&seen_key(chat, &it.id));
                Next::Continue
            }
            Err(e) => self.failed(chat, &it.id, &e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        telegram::Telegram,
        testutil::{self, config, rss, snow, FxSpec, Mock},
    };
    use std::path::PathBuf;

    struct Env {
        m: Arc<Mock>,
        sched: Scheduler,
        state: PathBuf,
    }

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("xtb-it-{}-{}-{}.json", std::process::id(), name, crate::x_source::now_ms()));
        let _ = std::fs::remove_file(&p);
        p
    }

    async fn build(m: &Arc<Mock>, state: &PathBuf, extra: &[(&str, &str)]) -> Scheduler {
        let cfg = Arc::new(config(m, state, extra));
        let http = Client::builder().user_agent("test").build().unwrap();
        let store = SeenStore::open(cfg.state_path.clone(), cfg.seen_cap, None).await.unwrap();
        let tg = Telegram::new(http.clone(), cfg.telegram_base.clone(), cfg.telegram_token.clone());
        let d = Deliverer::new(cfg.clone(), http.clone(), tg);
        Scheduler::new(cfg, http, store, d, Arc::new(Health::new()), CancellationToken::new())
    }

    async fn env(name: &str, extra: &[(&str, &str)]) -> Env {
        let m = testutil::start().await;
        let state = tmp(name);
        let sched = build(&m, &state, extra).await;
        Env { m, sched, state }
    }

    fn photo(m: &Mock, name: &str) {
        m.media.lock().unwrap().insert(name.to_string(), vec![0xFF, 0xD8, 0xFF, 0xE0, 1, 2, 3, 4]);
    }

    /// Seeds an account with one old post (first run), so later posts count as new.
    async fn seed(e: &mut Env, user: &str, old: &str) {
        e.m.set_feed(&format!("feedA/{user}"), 200, rss(&[(user, old, "old")]));
        e.sched.cycle().await;
        assert_eq!(e.m.total_tg(), 0, "first run must not send anything");
    }

    #[tokio::test]
    async fn first_run_seeds_then_forwards_only_new_media_posts() {
        let mut e = env("flow", &[]).await;
        let (old, p_photo, p_text, p_reply, p_repost) = (snow(5000, 1), snow(300, 2), snow(250, 3), snow(200, 4), snow(100, 5));
        seed(&mut e, "nasa", &old).await;
        assert!(e.sched.store.sub_known("@chan", "nasa"));

        let m = e.m.clone();
        photo(&m, "a.jpg");
        photo(&m, "r.jpg");
        photo(&m, "q.jpg");
        m.set_fx(&p_photo, 200, FxSpec::new("nasa", "Look at <this> & that").photo("a.jpg").likes(42).json(m.port));
        m.set_fx(&p_text, 200, FxSpec::new("nasa", "just words").json(m.port));
        m.set_fx(&p_reply, 200, FxSpec::new("nasa", "a reply").photo("q.jpg").reply().json(m.port));
        m.set_fx(&p_repost, 200, FxSpec::new("esa", "from esa").photo("r.jpg").json(m.port));
        m.set_feed(
            "feedA/nasa",
            200,
            rss(&[
                ("nasa", &p_repost.clone(), "ignored"),
                ("nasa", &p_reply, "t"),
                ("nasa", &p_text, "t"),
                ("nasa", &p_photo, "t"),
                ("nasa", &old, "old"),
            ])
            .replace(&format!("nasa/status/{p_repost}"), &format!("esa/status/{p_repost}"))
            .replace("<title>ignored</title>", "<title>RT by @nasa: from esa</title>"),
        );
        e.sched.cycle().await;

        let photos = m.calls("sendPhoto");
        assert_eq!(photos.len(), 2, "photo + repost, not the text-only post or the reply: {:?}", m.tg.lock().unwrap().iter().map(|x| &x.0).collect::<Vec<_>>());
        assert_eq!(m.count("sendMessage"), 0);
        // oldest first
        assert!(photos[0].contains("Look at &lt;this&gt; &amp; that") && photos[0].contains("❤️ 42"));
        assert!(photos[0].contains(&format!("https://x.com/nasa/status/{p_photo}")));
        assert!(photos[1].contains("🔁 @nasa") && photos[1].contains("(@esa)") && photos[1].contains("from esa"));
        assert!(photos[0].contains("name=\"chat_id\"") && photos[0].contains("@chan") && photos[0].contains("parse_mode"));

        m.clear_tg();
        e.sched.cycle().await;
        assert_eq!(m.total_tg(), 0, "nothing is sent twice");
        // processed posts, including skipped ones, are remembered
        for id in [&p_photo, &p_text, &p_reply, &p_repost] {
            assert!(e.sched.store.contains(&seen_key("@chan", id)));
        }
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn restart_does_not_resend() {
        let mut e = env("restart", &[]).await;
        let (old, p1) = (snow(5000, 1), snow(100, 2));
        seed(&mut e, "nasa", &old).await;
        photo(&e.m, "a.jpg");
        e.m.set_fx(&p1, 200, FxSpec::new("nasa", "hi").photo("a.jpg").json(e.m.port));
        e.m.set_feed("feedA/nasa", 200, rss(&[("nasa", &p1, "t"), ("nasa", &old, "o")]));
        e.sched.cycle().await;
        assert_eq!(e.m.count("sendPhoto"), 1);
        e.sched.store.flush().await;
        drop(e.sched);

        let mut again = build(&e.m, &e.state, &[]).await; // new process, same state file
        e.m.clear_tg();
        again.cycle().await;
        assert_eq!(e.m.total_tg(), 0, "state survived the restart");
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn rss_falls_back_to_next_source_and_remembers_it() {
        let mut e = env("fallback", &[]).await;
        e.m.set_feed("feedA/nasa", 403, "blocked".into());
        e.m.set_feed("feedB/nasa", 200, "<html>Just a moment</html>".into());
        e.m.set_feed("feedC/nasa", 200, rss(&[("nasa", &snow(60, 1), "t")]));
        e.sched.cycle().await;
        assert!(e.sched.store.sub_known("@chan", "nasa"), "seeded via the third source");
        assert!(e.sched.sources.primary().ends_with("/feedC"), "working source is tried first from now on");
        assert_eq!(e.sched.penalty, Duration::ZERO);
        let before = e.m.requests.lock().unwrap().len();
        e.sched.cycle().await;
        let reqs: Vec<String> = e.m.requests.lock().unwrap()[before..].to_vec();
        assert_eq!(reqs, vec!["GET /feedC/nasa/rss"], "no more wasted requests on dead sources");
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn all_feeds_failing_backs_off_and_recovers() {
        let mut e = env("penalty", &[]).await;
        for f in ["feedA", "feedB", "feedC"] {
            e.m.set_feed(&format!("{f}/nasa"), 500, "x".into());
        }
        e.sched.cycle().await;
        assert_eq!(e.sched.penalty, Duration::from_secs(300));
        e.sched.cycle().await;
        assert_eq!(e.sched.penalty, Duration::from_secs(600));
        assert!(!e.sched.store.sub_known("@chan", "nasa"), "a failed first fetch must not count as seeding");
        e.m.set_feed("feedA/nasa", 200, rss(&[("nasa", &snow(60, 1), "t")]));
        e.sched.cycle().await;
        assert_eq!(e.sched.penalty, Duration::ZERO);
        assert!(e.sched.store.sub_known("@chan", "nasa"));
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn a_post_telegram_rejects_does_not_block_the_others_and_is_given_up_after_three_cycles() {
        let mut e = env("giveup", &[]).await;
        let (old, a, b) = (snow(5000, 1), snow(200, 2), snow(100, 3));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "bad.jpg");
        photo(&m, "good.jpg");
        m.set_fx(&a, 200, FxSpec::new("nasa", "A").photo("bad.jpg").json(m.port));
        m.set_fx(&b, 200, FxSpec::new("nasa", "B").photo("good.jpg").json(m.port));
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &b, "t"), ("nasa", &a, "t"), ("nasa", &old, "o")]));
        // Telegram answers 400 for post A only (a permanent, post-specific problem)
        m.fail_when(|method, body| (method == "sendPhoto" && body.contains("\n\nA\n\n")).then_some((400, "Bad Request: chat not found")));
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 2, "A is tried and rejected, and B is still delivered in the same cycle");
        assert!(e.sched.store.contains(&seen_key("@chan", &b)) && !e.sched.store.contains(&seen_key("@chan", &a)));
        for _ in 0..2 {
            m.clear_tg();
            e.sched.cycle().await;
        }
        assert!(e.sched.store.contains(&seen_key("@chan", &a)), "given up after 3 failed cycles");
        m.clear_tg();
        e.sched.cycle().await;
        assert_eq!(m.total_tg(), 0);
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn telegram_flood_limits_and_outages_are_temporary_and_keep_order() {
        let mut e = env("flood", &[]).await;
        let (old, a, b) = (snow(5000, 1), snow(200, 2), snow(100, 3));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        photo(&m, "b.jpg");
        m.set_fx(&a, 200, FxSpec::new("nasa", "A").photo("a.jpg").json(m.port));
        m.set_fx(&b, 200, FxSpec::new("nasa", "B").photo("b.jpg").json(m.port));
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &b, "t"), ("nasa", &a, "t"), ("nasa", &old, "o")]));
        m.fail_when(|method, _| (method == "sendPhoto").then_some((429, "Too Many Requests: retry after 0")));
        for _ in 0..4 {
            m.clear_tg();
            e.sched.cycle().await;
            assert!(m.count("sendPhoto") >= 1);
            assert!(!e.sched.store.contains(&seen_key("@chan", &a)), "flood control must not use up A's attempts (4 failed cycles > the permanent limit of 3)");
            assert!(!e.sched.store.contains(&seen_key("@chan", &b)), "order is kept while Telegram is unavailable");
        }
        m.clear_rules();
        m.clear_tg();
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 2, "both delivered, oldest first, once Telegram recovers");
        assert!(m.calls("sendPhoto")[0].contains("\n\nA\n\n") && m.calls("sendPhoto")[1].contains("\n\nB\n\n"));
        let _ = std::fs::remove_file(&e.state);
    }

    #[test]
    fn error_classification() {
        use crate::telegram::TgError;
        let tg = |c: u16| anyhow::Error::new(TgError { code: c, desc: "x".into(), retry_after: None });
        for c in [0u16, 429, 500, 502, 503] {
            assert!(is_temporary(&tg(c)), "{c} is temporary");
        }
        for c in [400u16, 403, 404, 413] {
            assert!(!is_temporary(&tg(c)), "{c} is permanent");
        }
        assert!(is_temporary(&anyhow::anyhow!("download failed: HTTP 503")));
        assert!(is_temporary(&anyhow::anyhow!("download failed: HTTP 429")));
        assert!(!is_temporary(&anyhow::anyhow!("download failed: HTTP 404")));
        assert!(is_temporary(&anyhow::anyhow!("FxEmbed HTTP 502")));
        assert!(!is_temporary(&anyhow::anyhow!("FxEmbed HTTP 400")));
        assert!(is_temporary(&anyhow::anyhow!("error sending request: timed out")));
        assert!(is_temporary(&anyhow::anyhow!("FxEmbed returned invalid JSON: eof")));
    }

    #[tokio::test]
    async fn fxembed_outage_is_temporary_and_gone_posts_are_skipped() {
        let mut e = env("fx", &[]).await;
        let (old, a, gone) = (snow(5000, 1), snow(100, 2), snow(150, 3));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        m.set_fx(&a, 500, "oops".into());
        m.set_fx(&gone, 404, "{}".into());
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &a, "t"), ("nasa", &gone, "t"), ("nasa", &old, "o")]));
        e.sched.cycle().await;
        assert_eq!(m.total_tg(), 0);
        assert!(e.sched.store.contains(&seen_key("@chan", &gone)), "deleted post is marked seen");
        assert!(!e.sched.store.contains(&seen_key("@chan", &a)), "outage: try again later");
        m.set_fx(&a, 200, FxSpec::new("nasa", "back").photo("a.jpg").json(m.port));
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 1);
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn too_old_posts_and_disabled_reposts_are_ignored() {
        let mut e = env("age", &[("INCLUDE_REPOSTS", "false")]).await;
        let (old, ancient, rp) = (snow(5000, 1), snow(3 * 86400, 2), snow(100, 3));
        seed(&mut e, "nasa", &old).await;
        photo(&e.m, "a.jpg");
        for id in [&ancient, &rp] {
            e.m.set_fx(id, 200, FxSpec::new("esa", "x").photo("a.jpg").json(e.m.port));
        }
        e.m.set_feed(
            "feedA/nasa",
            200,
            rss(&[("esa", &ancient, "t"), ("esa", &rp, "RT by @nasa: x"), ("nasa", &old, "o")]),
        );
        e.sched.cycle().await;
        assert_eq!(e.m.total_tg(), 0);
        assert!(e.sched.store.contains(&seen_key("@chan", &ancient)) && e.sched.store.contains(&seen_key("@chan", &rp)));
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn min_likes_waits_for_likes_then_sends_or_drops() {
        let mut e = env("likes", &[("MIN_LIKES", "10"), ("LIKES_WAIT_MIN", "60")]).await;
        let (old, fresh, stale) = (snow(5000, 1), snow(120, 2), snow(7200, 3));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        m.set_fx(&fresh, 200, FxSpec::new("nasa", "f").photo("a.jpg").likes(2).json(m.port));
        m.set_fx(&stale, 200, FxSpec::new("nasa", "s").photo("a.jpg").likes(2).json(m.port));
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &fresh, "t"), ("nasa", &stale, "t"), ("nasa", &old, "o")]));
        e.sched.cycle().await;
        assert_eq!(m.total_tg(), 0);
        assert!(!e.sched.store.contains(&seen_key("@chan", &fresh)), "young post keeps being re-checked");
        assert!(e.sched.store.contains(&seen_key("@chan", &stale)), "old post below the threshold is dropped");
        m.set_fx(&fresh, 200, FxSpec::new("nasa", "f").photo("a.jpg").likes(25).json(m.port));
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 1);
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn media_kinds_album_video_gif_and_oversize() {
        let mut e = env("media", &[("MAX_UPLOAD_BYTES", "1000")]).await;
        let (old, album, vid, gif, big) = (snow(5000, 1), snow(400, 2), snow(300, 3), snow(200, 4), snow(100, 5));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        for n in ["1.jpg", "2.jpg", "3.jpg"] {
            photo(&m, n);
        }
        m.media.lock().unwrap().insert("v.mp4".into(), vec![7; 500]);
        m.media.lock().unwrap().insert("g.mp4".into(), vec![7; 500]);
        m.media.lock().unwrap().insert("big.mp4".into(), vec![7; 5000]);
        m.set_fx(&album, 200, FxSpec::new("nasa", "three").photo("1.jpg").photo("2.jpg").photo("3.jpg").json(m.port));
        m.set_fx(&vid, 200, FxSpec::new("nasa", "video").video("v.mp4", false).json(m.port));
        m.set_fx(&gif, 200, FxSpec::new("nasa", "gif").video("g.mp4", true).json(m.port));
        m.set_fx(&big, 200, FxSpec::new("nasa", "huge").video("big.mp4", false).json(m.port));
        m.set_feed(
            "feedA/nasa",
            200,
            rss(&[("nasa", &big, "t"), ("nasa", &gif, "t"), ("nasa", &vid, "t"), ("nasa", &album, "t"), ("nasa", &old, "o")]),
        );
        e.sched.cycle().await;
        assert_eq!(m.count("sendMediaGroup"), 1);
        assert!(m.calls("sendMediaGroup")[0].matches("attach://file").count() == 3, "three photos in one album");
        assert_eq!(m.count("sendVideo"), 1);
        assert_eq!(m.count("sendAnimation"), 1);
        assert_eq!(m.count("sendMessage"), 1, "oversize video falls back to a link message");
        assert!(m.calls("sendMessage")[0].contains(&format!("status/{big}")));
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn long_text_goes_to_followups_and_followup_failure_never_resends_media() {
        let mut e = env("long", &[]).await;
        let (old, p) = (snow(5000, 1), snow(100, 2));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        let long = "lorem ipsum & dolor ".repeat(80);
        m.set_fx(&p, 200, FxSpec::new("nasa", &long).photo("a.jpg").json(m.port));
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &p, "t"), ("nasa", &old, "o")]));
        m.fail_when(|method, _| (method == "sendMessage").then_some((400, "Bad Request: message is too long")));
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 1);
        assert!(m.count("sendMessage") >= 1, "follow-up attempted");
        assert!(!m.calls("sendPhoto")[0].contains("lorem"), "long text is not in the caption");
        assert!(e.sched.store.contains(&seen_key("@chan", &p)), "media was delivered, so the post is done");
        m.clear_tg();
        m.clear_rules();
        e.sched.cycle().await;
        assert_eq!(m.total_tg(), 0);
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn text_posts_when_media_only_is_off_and_foreign_media_hosts_become_links() {
        let mut e = env("text", &[("MEDIA_ONLY", "false"), ("MEDIA_HOSTS", "twimg.com")]).await;
        let (old, t, evil) = (snow(5000, 1), snow(200, 2), snow(100, 3));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        m.set_fx(&t, 200, FxSpec::new("nasa", "plain text post").json(m.port));
        m.set_fx(&evil, 200, FxSpec::new("nasa", "sneaky").photo("a.jpg").json(m.port)); // 127.0.0.1 is not an allowed host here
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &evil, "t"), ("nasa", &t, "t"), ("nasa", &old, "o")]));
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 0, "disallowed host is never downloaded");
        let msgs = m.calls("sendMessage");
        assert_eq!(msgs.len(), 2);
        assert!(msgs[0].contains("plain text post") && msgs[1].contains("sneaky"));
        assert!(!m.requests.lock().unwrap().iter().any(|r| r.contains("/media/")), "no request to the foreign host");
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn same_post_goes_to_two_channels_and_overrides_work() {
        let mut e = env("chans", &[("X_ACCOUNTS", "nasa, nasa=@other")]).await;
        let (old, p) = (snow(5000, 1), snow(100, 2));
        seed(&mut e, "nasa", &old).await;
        photo(&e.m, "a.jpg");
        e.m.set_fx(&p, 200, FxSpec::new("nasa", "hi").photo("a.jpg").json(e.m.port));
        e.m.set_feed("feedA/nasa", 200, rss(&[("nasa", &p, "t"), ("nasa", &old, "o")]));
        e.sched.cycle().await;
        let calls = e.m.calls("sendPhoto");
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().any(|c| c.contains("@chan")) && calls.iter().any(|c| c.contains("@other")));
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn backlog_on_first_run_when_requested() {
        let mut e = env("backlog", &[("SEND_BACKLOG_ON_FIRST_RUN", "true")]).await;
        let p = snow(100, 1);
        photo(&e.m, "a.jpg");
        e.m.set_fx(&p, 200, FxSpec::new("nasa", "hi").photo("a.jpg").json(e.m.port));
        e.m.set_feed("feedA/nasa", 200, rss(&[("nasa", &p, "t")]));
        e.sched.cycle().await;
        assert_eq!(e.m.count("sendPhoto"), 1);
        assert!(e.sched.store.sub_known("@chan", "nasa"));
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn round_robin_budget_covers_every_account() {
        let mut e = env("rr", &[("X_ACCOUNTS", "a1,a2,a3"), ("CYCLE_BUDGET_SECS", "30")]).await;
        for u in ["a1", "a2", "a3"] {
            e.m.set_feed(&format!("feedA/{u}"), 200, rss(&[(u, &snow(60, 1), "t")]));
        }
        e.sched.cycle().await;
        for u in ["a1", "a2", "a3"] {
            assert!(e.sched.store.sub_known("@chan", u), "{u} seeded");
        }
        assert_eq!(e.sched.next_account, 0);
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn github_state_backup_and_restore() {
        let m = testutil::start().await;
        let gh = |m: &Mock| crate::config::GithubSettings { token: "tok".into(), repo: "o/r".into(), branch: "bot-state".into(), api_base: m.url("") };
        let client = Client::new();
        let path = tmp("gh");
        let mut s = SeenStore::open(path.clone(), 100, Some((client.clone(), gh(&m)))).await.unwrap();
        assert_eq!(m.gh_puts.load(Ordering::SeqCst), 0);
        s.insert("@chan|111");
        s.mark_sub(&sub_key("@chan", "nasa"));
        s.flush().await;
        assert_eq!(m.gh_puts.load(Ordering::SeqCst), 1, "state pushed to GitHub (branch created on demand)");
        assert!(m.gh_branch.load(Ordering::SeqCst));
        drop(s);

        let _ = std::fs::remove_file(&path); // new instance: empty disk, state only on GitHub
        let s2 = SeenStore::open(path.clone(), 100, Some((client.clone(), gh(&m)))).await.unwrap();
        assert!(s2.contains("@chan|111") && s2.sub_known("@chan", "nasa"), "restored from GitHub");

        // GitHub down at startup: refuse to start with empty history (prevents a duplicate flood)
        m.gh_fail_get.store(true, Ordering::SeqCst);
        let gets_before = m.gh_gets.load(Ordering::SeqCst);
        let r = tokio::time::timeout(Duration::from_secs(60), SeenStore::open(tmp("gh2"), 100, Some((client, gh(&m))))).await.unwrap();
        assert!(r.is_err());
        assert_eq!(m.gh_gets.load(Ordering::SeqCst) - gets_before, 5, "five restore attempts");
        let _ = std::fs::remove_file(&path);
    }

    // ------------------------------------------------------------------ For you (real browser)

    /// These tests drive the real Lightpanda binary. They run when LIGHTPANDA_BIN points at it
    /// (CI downloads the pinned release) and are skipped, with a note, otherwise.
    fn lightpanda() -> Option<String> {
        let p = std::env::var("LIGHTPANDA_BIN").ok()?;
        std::path::Path::new(&p).exists().then_some(p)
    }

    async fn fy_env(name: &str, bin: &str, port: u16, page: &str, extra: &[(&str, &str)]) -> Env {
        std::env::set_var("LIGHTPANDA_DISABLE_TELEMETRY", "true");
        let m = testutil::start().await;
        let url = m.url(page);
        let port_s = port.to_string();
        let mut all: Vec<(&str, &str)> = vec![
            ("X_ACCOUNTS", ""),
            ("FOR_YOU", "true"),
            ("X_AUTH_TOKEN", "TOK123"),
            ("X_CT0", "CSRF456"),
            ("X_COOKIE_DOMAIN", "127.0.0.1"),
            ("FOR_YOU_URL", &url),
            ("BROWSER_BIN", bin),
            ("BROWSER_PORT", &port_s),
            ("FOR_YOU_SCROLLS", "3"),
            ("FOR_YOU_WAIT_SECS", "8"),
            ("PAGE_TIMEOUT_SECS", "30"),
        ];
        all.extend_from_slice(extra);
        let state = tmp(name);
        let sched = build(&m, &state, &all).await;
        Env { m, sched, state }
    }

    #[tokio::test]
    async fn for_you_feed_end_to_end_with_real_browser() {
        let Some(bin) = lightpanda() else {
            eprintln!("SKIPPED: set LIGHTPANDA_BIN to run the browser tests");
            return;
        };
        let mut e = fy_env("fy", &bin, 19311, "/home", &[]).await;
        let (old, a, b, c, ad) = (snow(5000, 1), snow(300, 2), snow(200, 3), snow(100, 4), snow(50, 5));
        let m = e.m.clone();
        // first visit: one old post -> seeding, nothing is sent
        m.pages.lock().unwrap().insert("/home".into(), testutil::x_page(&[testutil::fp("nasa", &old)], &[testutil::fp("nasa", &old)]));
        e.sched.cycle().await;
        assert_eq!(m.total_tg(), 0, "first visit only seeds");
        assert!(e.sched.store.sub_known("@chan", "@foryou"));

        // second visit: posts appear late (JS-rendered); the list is replaced mid-way (virtualised)
        photo(&m, "a.jpg");
        photo(&m, "b.jpg");
        photo(&m, "c.jpg");
        for (id, img, who) in [(&a, "a.jpg", "nasa"), (&b, "b.jpg", "esa"), (&c, "c.jpg", "spacex")] {
            m.set_fx(id, 200, FxSpec::new(who, "hello").photo(img).json(m.port));
        }
        m.set_fx(&ad, 200, FxSpec::new("brand", "BUY").photo("a.jpg").json(m.port));
        let mut repost = testutil::fp("esa", &b);
        repost.context = "Jane Doe reposted";
        let mut advert = testutil::fp("brand", &ad);
        advert.ad = true;
        let mut cell = testutil::fp("who", "999");
        cell.no_time = true;
        m.pages.lock().unwrap().insert(
            "/home".into(),
            testutil::x_page(&[testutil::fp("nasa", &a), repost, advert, cell], &[testutil::fp("spacex", &c), testutil::fp("nasa", &old)]),
        );
        m.cookies.lock().unwrap().clear();
        e.sched.cycle().await;

        let cookie = m.cookies.lock().unwrap().join(" | ");
        assert!(cookie.contains("auth_token=TOK123") && cookie.contains("ct0=CSRF456") && cookie.contains("lang=en"), "login cookies reach the page: {cookie:?}");
        let photos = m.calls("sendPhoto");
        assert_eq!(photos.len(), 3, "posts a, b (before the list was replaced) and c (after) - not the ad, the cell or the old post");
        assert!(photos.iter().any(|p| p.contains("🔁 Jane Doe")), "repost credited by name");
        assert!(!photos.iter().any(|p| p.contains("BUY")), "ads are never forwarded");
        assert_eq!(e.sched.penalty, Duration::ZERO);

        m.clear_tg();
        e.sched.cycle().await;
        assert_eq!(m.total_tg(), 0, "nothing is sent twice");
        e.sched.browser.as_mut().unwrap().shutdown().await;
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn for_you_login_wall_and_error_pages_back_off_with_a_clear_reason() {
        let Some(bin) = lightpanda() else {
            eprintln!("SKIPPED: set LIGHTPANDA_BIN to run the browser tests");
            return;
        };
        let mut e = fy_env("fylogin", &bin, 19312, "/home", &[("FOR_YOU_WAIT_SECS", "4")]).await;
        e.m.pages.lock().unwrap().insert("/home".into(), testutil::LOGIN_PAGE.to_string());
        e.sched.cycle().await;
        assert_eq!(e.m.total_tg(), 0);
        assert_eq!(e.sched.penalty, Duration::ZERO, "For you has its own backoff; it must not slow the account feeds");
        assert_eq!(e.sched.fy_backoff, Duration::from_secs(300), "a login wall backs the For-you source off");
        let n_before = e.m.requests.lock().unwrap().len();
        e.sched.cycle().await; // inside the backoff window: the browser is not even started
        assert_eq!(e.m.requests.lock().unwrap().len(), n_before, "no page request while backing off (don't hammer X with a bad cookie)");
        assert!(!e.sched.store.sub_known("@chan", "@foryou"), "a failed visit must not count as seeding");
        let err = e.sched.process_for_you().await.unwrap_err().to_string();
        assert!(err.contains("X_AUTH_TOKEN"), "{err}");

        e.m.pages.lock().unwrap().insert("/home".into(), testutil::ERROR_PAGE.to_string());
        let err = e.sched.process_for_you().await.unwrap_err().to_string();
        assert!(err.contains("Something went wrong"), "{err}");

        // recovers once the page is fine again (and the backoff window has passed)
        e.m.pages.lock().unwrap().insert("/home".into(), testutil::x_page(&[testutil::fp("nasa", &snow(60, 1))], &[testutil::fp("nasa", &snow(60, 1))]));
        e.sched.fy_next_try = None;
        e.sched.cycle().await;
        assert_eq!(e.sched.fy_backoff, Duration::ZERO);
        assert!(e.sched.store.sub_known("@chan", "@foryou"));
        e.sched.browser.as_mut().unwrap().shutdown().await;
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn for_you_and_accounts_run_together_and_respect_max_new() {
        let Some(bin) = lightpanda() else {
            eprintln!("SKIPPED: set LIGHTPANDA_BIN to run the browser tests");
            return;
        };
        let mut e = fy_env("fyboth", &bin, 19313, "/home", &[("X_ACCOUNTS", "nasa"), ("FOR_YOU_MAX_NEW", "1"), ("FOR_YOU_CHAT_ID", "@fy")]).await;
        let (old, a, b, acc) = (snow(5000, 1), snow(300, 2), snow(200, 3), snow(100, 4));
        let m = e.m.clone();
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &old, "o")]));
        m.pages.lock().unwrap().insert("/home".into(), testutil::x_page(&[testutil::fp("nasa", &old)], &[testutil::fp("nasa", &old)]));
        e.sched.cycle().await;
        assert!(e.sched.store.sub_known("@chan", "nasa") && e.sched.store.sub_known("@fy", "@foryou"), "both sources seeded, each to its own channel");

        photo(&m, "a.jpg");
        for (id, who) in [(&a, "nasa"), (&b, "esa"), (&acc, "nasa")] {
            m.set_fx(id, 200, FxSpec::new(who, "hi").photo("a.jpg").json(m.port));
        }
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &acc, "t"), ("nasa", &old, "o")]));
        m.pages.lock().unwrap().insert("/home".into(), testutil::x_page(&[testutil::fp("nasa", &a), testutil::fp("esa", &b)], &[testutil::fp("nasa", &a), testutil::fp("esa", &b)]));
        e.sched.cycle().await;
        let photos = m.calls("sendPhoto");
        assert_eq!(photos.len(), 2, "one account post to @chan + only 1 For-you post (FOR_YOU_MAX_NEW=1) to @fy");
        assert!(photos.iter().any(|p| p.contains("@chan")) && photos.iter().any(|p| p.contains("@fy")));
        e.sched.cycle().await; // the second For-you post goes out next cycle
        assert_eq!(m.count("sendPhoto"), 3);
        e.sched.browser.as_mut().unwrap().shutdown().await;
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn for_you_tab_is_selected_when_x_opens_on_following() {
        let Some(bin) = lightpanda() else {
            eprintln!("SKIPPED: set LIGHTPANDA_BIN to run the browser tests");
            return;
        };
        let mut e = fy_env("fytab", &bin, 19315, "/home", &[]).await;
        let (old, fol, fy) = (snow(5000, 1), snow(200, 2), snow(100, 3));
        let m = e.m.clone();
        photo(&m, "a.jpg");
        m.set_fx(&fy, 200, FxSpec::new("nasa", "from for you").photo("a.jpg").json(m.port));
        m.set_fx(&fol, 200, FxSpec::new("esa", "from following").photo("a.jpg").json(m.port));
        m.pages.lock().unwrap().insert("/home".into(), testutil::x_page_following_first(&[testutil::fp("esa", &old)], &[testutil::fp("nasa", &old)]));
        e.sched.cycle().await; // seeds with the For-you list
        m.pages.lock().unwrap().insert("/home".into(), testutil::x_page_following_first(&[testutil::fp("esa", &fol)], &[testutil::fp("nasa", &fy)]));
        e.sched.cycle().await;
        let photos = m.calls("sendPhoto");
        assert_eq!(photos.len(), 1);
        assert!(photos[0].contains("from for you") && !photos[0].contains("from following"), "must read the For you tab, not Following");
        e.sched.browser.as_mut().unwrap().shutdown().await;
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn browser_recovers_after_a_fetch_is_cancelled_mid_flight() {
        let Some(bin) = lightpanda() else {
            eprintln!("SKIPPED: set LIGHTPANDA_BIN to run the browser tests");
            return;
        };
        // The scheduler's safety timeout (or a shutdown) can drop a fetch while the page is still loading.
        let mut e = fy_env("fycancel", &bin, 19316, "/home", &[("FOR_YOU_WAIT_SECS", "20")]).await;
        e.m.pages.lock().unwrap().insert("/home".into(), testutil::LOGIN_PAGE.replace("<a href=\"/login\">Log in</a>", "<p>loading forever</p>"));
        let started = Instant::now();
        let r = tokio::time::timeout(Duration::from_secs(3), e.sched.browser.as_mut().unwrap().fetch_for_you()).await;
        assert!(r.is_err() && started.elapsed() < Duration::from_secs(10), "the fetch was cancelled from outside");
        // the next fetch must work on a healthy page, without any manual cleanup
        let id = snow(60, 1);
        e.m.pages.lock().unwrap().insert("/home".into(), testutil::x_page(&[testutil::fp("nasa", &id)], &[testutil::fp("nasa", &id)]));
        let items = e.sched.browser.as_mut().unwrap().fetch_for_you().await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, id);
        e.sched.browser.as_mut().unwrap().shutdown().await;
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn missing_browser_binary_is_a_clear_error_not_a_crash() {
        let mut e = fy_env("fynobin", "/nonexistent/lightpanda", 19314, "/home", &[]).await;
        e.sched.cycle().await;
        let err = e.sched.process_for_you().await.unwrap_err().to_string();
        assert!(err.contains("cannot start browser"), "{err}");
        assert_eq!(e.sched.fy_backoff, Duration::from_secs(300));
        let _ = std::fs::remove_file(&e.state);
    }

    // ------------------------------------------------------------------ regressions found in review

    #[tokio::test]
    async fn empty_first_fetch_does_not_count_as_seeding() {
        let mut e = env("emptyseed", &[]).await;
        e.m.set_feed("feedA/nasa", 200, rss(&[]));
        e.m.set_feed("feedB/nasa", 200, rss(&[]));
        e.m.set_feed("feedC/nasa", 200, rss(&[]));
        e.sched.cycle().await;
        assert!(!e.sched.store.sub_known("@chan", "nasa"), "an empty feed must not be treated as 'seeded'");
        // when posts finally show up, they are seeded (not flooded to the channel)
        let (a, b) = (snow(300, 1), snow(200, 2));
        e.m.set_feed("feedA/nasa", 200, rss(&[("nasa", &a, "t"), ("nasa", &b, "t")]));
        e.sched.cycle().await;
        assert_eq!(e.m.total_tg(), 0);
        assert!(e.sched.store.sub_known("@chan", "nasa") && e.sched.store.contains(&seen_key("@chan", &a)));
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn empty_feed_from_one_instance_falls_through_to_the_next() {
        let mut e = env("emptyfall", &[]).await;
        e.m.set_feed("feedA/nasa", 200, rss(&[])); // a blocked instance often answers with an empty feed
        e.m.set_feed("feedB/nasa", 200, rss(&[("nasa", &snow(60, 1), "t")]));
        let items = e.sched.sources.fetch_feed("nasa").await.unwrap();
        assert_eq!(items.len(), 1, "second instance has the posts");
        assert!(e.sched.sources.primary().ends_with("/feedB"), "the instance with content becomes primary");
        // if every instance is empty, an empty feed is returned (account really has no posts)
        e.m.set_feed("feedB/nasa", 200, rss(&[]));
        e.m.set_feed("feedC/nasa", 200, rss(&[]));
        assert!(e.sched.sources.fetch_feed("nasa").await.unwrap().is_empty());
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn posts_waiting_for_likes_do_not_starve_new_posts() {
        let mut e = env("starve", &[("MIN_LIKES", "100"), ("LIKES_WAIT_MIN", "600")]).await;
        let old = snow(5000, 1);
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        let waiting: Vec<String> = (0..3).map(|i| snow(900 - i * 10, 10 + i)).collect();
        let hot = snow(50, 99);
        for w in &waiting {
            m.set_fx(w, 200, FxSpec::new("nasa", "meh").photo("a.jpg").likes(1).json(m.port));
        }
        m.set_fx(&hot, 200, FxSpec::new("nasa", "viral").photo("a.jpg").likes(500).json(m.port));
        let mut feed: Vec<(&str, &str, &str)> = waiting.iter().map(|w| ("nasa", w.as_str(), "t")).collect();
        feed.push(("nasa", &hot, "t"));
        feed.push(("nasa", &old, "o"));
        m.set_feed("feedA/nasa", 200, rss(&feed));
        // budget of 2 per pass: the 3 waiting (older) posts must not eat it
        e.sched.process_items_for_test("@chan", "nasa", 2).await;
        assert_eq!(m.count("sendPhoto"), 1, "the viral post is delivered although 3 older posts are still waiting");
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn media_redirects_to_other_hosts_are_not_followed() {
        let mut e = env("redir", &[]).await;
        let (old, ok, bad) = (snow(5000, 1), snow(200, 2), snow(100, 3));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        photo(&m, "secret.jpg");
        // /redir_ok -> same allowed host; /redir_bad -> a host that is not in MEDIA_HOSTS
        let body = |p: &str| {
            let u = m.url(p);
            serde_json::json!({"tweet":{"text":"t","likes":1,"author":{"screen_name":"nasa","name":"N"},"media":{"photos":[{"url":u}],"videos":[]}}}).to_string()
        };
        m.set_fx(&ok, 200, body("/redir_ok"));
        m.set_fx(&bad, 200, body("/redir_bad"));
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &bad, "t"), ("nasa", &ok, "t"), ("nasa", &old, "o")]));
        e.sched.cycle().await;
        let reqs = m.requests.lock().unwrap().clone();
        assert!(reqs.iter().any(|r| r == "GET /media/a.jpg"), "same-host redirect is followed");
        assert!(!reqs.iter().any(|r| r == "GET /media/secret.jpg"), "redirect to a foreign host must not be followed: {reqs:?}");
        assert_eq!(m.count("sendPhoto"), 1);
        assert_eq!(m.count("sendMessage"), 1, "the blocked one falls back to a link message");
        let _ = std::fs::remove_file(&e.state);
    }

    // ------------------------------------------------------------------ regressions, review round 2

    #[tokio::test]
    async fn a_long_fxembed_outage_does_not_drop_posts() {
        let mut e = env("longoutage", &[]).await;
        let (old, a) = (snow(5000, 1), snow(100, 2));
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        m.set_fx(&a, 500, "down".into());
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &a, "t"), ("nasa", &old, "o")]));
        for _ in 0..9 {
            e.sched.cycle().await;
        }
        assert!(!e.sched.store.contains(&seen_key("@chan", &a)), "9 failed cycles (a long outage) must not make us give the post up");
        m.set_fx(&a, 200, FxSpec::new("nasa", "finally").photo("a.jpg").json(m.port));
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 1, "delivered once FxEmbed is back");
        // ...but a post that FxEmbed can never serve does not block the account forever
        let b = snow(50, 3);
        m.set_fx(&b, 500, "always broken".into());
        m.set_feed("feedA/nasa", 200, rss(&[("nasa", &b, "t"), ("nasa", &a, "t"), ("nasa", &old, "o")]));
        for _ in 0..10 {
            e.sched.cycle().await;
        }
        assert!(e.sched.store.contains(&seen_key("@chan", &b)), "given up after 10 failed cycles");
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn reposts_of_old_posts_are_forwarded_from_accounts_but_not_from_for_you() {
        let mut e = env("repostage", &[]).await;
        let old = snow(5000, 1);
        seed(&mut e, "nasa", &old).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        let ancient = snow(3 * 86400, 2); // original posted 3 days ago, reposted just now
        m.set_fx(&ancient, 200, FxSpec::new("esa", "classic").photo("a.jpg").json(m.port));
        m.set_feed("feedA/nasa", 200, rss(&[("esa", &ancient, "RT by @nasa: classic"), ("nasa", &old, "o")]));
        e.sched.cycle().await;
        assert_eq!(m.count("sendPhoto"), 1, "a fresh repost of an old post is news for an account feed");
        assert!(m.calls("sendPhoto")[0].contains("🔁 @nasa"));

        // the algorithmic For-you feed is full of old posts: those stay filtered, reposted or not
        m.clear_tg();
        let item = |id: &str, by: Option<&str>| crate::x_source::FeedItem { handle: "esa".into(), id: id.into(), reposter: by.map(String::from) };
        let anc2 = snow(4 * 86400, 3);
        m.set_fx(&anc2, 200, FxSpec::new("esa", "old viral").photo("a.jpg").json(m.port));
        e.sched.store.mark_sub(&sub_key("@fy", "@foryou"));
        e.sched.process_items("@fy", "@foryou", vec![item(&anc2, Some("Jane Doe"))], 10, true).await;
        assert_eq!(m.total_tg(), 0, "old For-you posts are skipped even when reposted");
        assert!(e.sched.store.contains(&seen_key("@fy", &anc2)));
        let _ = std::fs::remove_file(&e.state);
    }

    #[tokio::test]
    async fn state_file_over_github_contents_api_size_limit_is_restored() {
        // GitHub's contents API returns an empty body (encoding "none") for files over 1 MB.
        let m = testutil::start().await;
        let ids: Vec<String> = (0..40000).map(|i| format!("@somechannel|{}", 2_000_000_000_000_000_000u64 + i)).collect();
        let body = serde_json::json!({"ids": ids, "subs": ["@somechannel|nasa"]}).to_string();
        assert!(body.len() > 1_048_576, "test file must exceed 1 MB");
        *m.gh_file.lock().unwrap() = Some(("shaBIG".to_string(), body.into_bytes()));
        m.gh_big.store(true, Ordering::SeqCst);
        m.gh_branch.store(true, Ordering::SeqCst);
        let gh = crate::config::GithubSettings { token: "tok".into(), repo: "o/r".into(), branch: "bot-state".into(), api_base: m.url("") };
        let path = tmp("big");
        let s = SeenStore::open(path.clone(), 50000, Some((Client::new(), gh))).await.unwrap();
        assert!(s.contains("@somechannel|2000000000000000000") && s.contains("@somechannel|2000000000000039999"), "all ids restored from the raw download");
        assert!(s.sub_known("@somechannel", "nasa"));
        let _ = std::fs::remove_file(&path);
    }

    // ------------------------------------------------------------------ regressions, review round 3

    #[tokio::test]
    async fn for_you_budget_goes_to_the_newest_posts_not_the_oldest() {
        let mut e = env("fynewest", &[]).await;
        let m = e.m.clone();
        photo(&m, "a.jpg");
        let ids: Vec<String> = [3000u64, 2000, 100].iter().enumerate().map(|(i, ago)| snow(*ago, i as u64 + 1)).collect();
        for (i, id) in ids.iter().enumerate() {
            m.set_fx(id, 200, FxSpec::new("nasa", &format!("post number {i}")).photo("a.jpg").json(m.port));
        }
        e.sched.store.mark_sub(&sub_key("@fy", "@foryou"));
        let items: Vec<crate::x_source::FeedItem> = ids.iter().map(|id| crate::x_source::FeedItem { handle: "nasa".into(), id: id.clone(), reposter: None }).collect();
        e.sched.process_items("@fy", "@foryou", items.clone(), 1, true).await;
        let sent = m.calls("sendPhoto");
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("post number 2"), "the freshest post is the one worth the budget, got: {}", sent[0]);
        // accounts keep chronological order: oldest first
        m.clear_tg();
        e.sched.store.mark_sub(&sub_key("@chan", "nasa"));
        e.sched.process_items("@chan", "nasa", items, 2, false).await;
        let sent = m.calls("sendPhoto");
        assert_eq!(sent.len(), 2);
        assert!(sent[0].contains("post number 0") && sent[1].contains("post number 1"));
        let _ = std::fs::remove_file(&e.state);
    }

    #[test]
    fn penalty_doubles_up_to_an_hour() {
        let mut p = next_penalty(Duration::ZERO);
        assert_eq!(p.as_secs(), 300);
        for _ in 0..10 {
            p = next_penalty(p);
        }
        assert_eq!(p.as_secs(), 3600);
        assert_eq!(sub_key("@Chan", "NASA"), "@chan|nasa");
        assert_eq!(seen_key("@c", "5"), "@c|5");
    }
}
