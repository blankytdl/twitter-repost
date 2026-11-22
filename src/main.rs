mod config;
mod deliver;
mod health;
mod media_downloader;
mod scheduler;
mod seen_store;
mod self_ping;
mod telegram;
#[cfg(test)]
mod testutil;
mod x_browser;
mod x_source;

use anyhow::{anyhow, Result};
use config::{Config, GithubSettings};
use reqwest::Client;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{fmt::MakeWriter, prelude::*, EnvFilter};


const LOG_ROTATE_BYTES: u64 = 5 * 1024 * 1024;

struct RotatingLog {
    path: PathBuf,
    file: File,
    written: u64,
}

#[derive(Clone)]
struct LogMakeWriter(Arc<Mutex<RotatingLog>>);

struct LogWriter(Arc<Mutex<RotatingLog>>);

impl<'a> MakeWriter<'a> for LogMakeWriter {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(self.0.clone())
    }
}

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut log = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if log.written.saturating_add(buf.len() as u64) > LOG_ROTATE_BYTES {
            log.file.flush()?;
            let previous = log.path.with_extension("log.1");
            match fs::remove_file(&previous) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            fs::rename(&log.path, &previous)?;
            log.file = OpenOptions::new().create(true).append(true).open(&log.path)?;
            log.written = 0;
        }
        let n = log.file.write(buf)?;
        log.written = log.written.saturating_add(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).file.flush()
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let path = PathBuf::from(
        std::env::var_os("LOG_PATH").unwrap_or_else(|| "/tmp/xtb-logs/bot.log".into()),
    );
    let file_writer = (|| -> io::Result<LogMakeWriter> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        if fs::metadata(&path).map(|meta| meta.len() >= LOG_ROTATE_BYTES).unwrap_or(false) {
            let previous = path.with_extension("log.1");
            match fs::remove_file(&previous) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            fs::rename(&path, &previous)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let written = file.metadata()?.len();
        Ok(LogMakeWriter(Arc::new(Mutex::new(RotatingLog {
            path: path.clone(),
            file,
            written,
        }))))
    })();

    let console_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    match file_writer {
        Ok(writer) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(console_layer)
                .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(writer))
                .init();
            tracing::info!(log_path = %path.display(), "file logging enabled; console logging remains enabled");
        }
        Err(e) => {
            eprintln!("Cannot open log file {}: {e}; continuing with console-only logging", path.display());
            tracing_subscriber::registry().with(filter).with(console_layer).init();
        }
    }
}

fn http_client(user_agent: &str) -> Result<Client> {
    Ok(Client::builder()
        .user_agent(user_agent)
        .pool_max_idle_per_host(1)
        .connect_timeout(Duration::from_secs(15))
        .build()?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("check") => check().await,
        Some("fetch-test") => fetch_test(&args).await,
        _ => run().await,
    }
}

async fn wait_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = term.recv() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run() -> Result<()> {
    let cfg = Arc::new(Config::from_env(true)?);
    let handles = cfg.accounts.iter().map(|a| a.handle.as_str()).collect::<Vec<_>>().join(",");
    info!(
        accounts = cfg.accounts.len(),
        handles = %handles,
        sources = %cfg.rss_sources.join(","),
        for_you = cfg.for_you.is_some(),
        media_only = cfg.media_only,
        interval_secs = cfg.interval.as_secs(),
        "starting"
    );
    let http = http_client(&cfg.user_agent)?;
    let token = CancellationToken::new();
    let health = Arc::new(health::Health::new());

    tokio::spawn({
        let t = token.clone();
        async move {
            wait_signal().await;
            info!("shutdown signal received");
            t.cancel();
        }
    });

    // Bind the port first so Render's port scan succeeds while state is restored.
    let hs = tokio::spawn({
        let (port, h, t) = (cfg.port, health.clone(), token.clone());
        async move {
            // A failed bind (port in use) must be loud: without /health Render would restart us with no clue why.
            if let Err(e) = health::serve(port, h, t).await {
                tracing::error!("health server stopped: {e:#}");
            }
        }
    });

    let gh: Option<(Client, GithubSettings)> = cfg.github.clone().map(|g| (http.clone(), g));
    let store = seen_store::SeenStore::open(cfg.state_path.clone(), cfg.seen_cap, gh).await?;

    if let Some((url, every)) = cfg.self_ping.clone() {
        tokio::spawn(self_ping::run(http.clone(), url, every, token.clone()));
    } else {
        info!("self-ping disabled (not on Render or SELF_PING_ENABLED=false)");
    }

    let tg = telegram::Telegram::new(http.clone(), cfg.telegram_base.clone(), cfg.telegram_token.clone());
    let sched = scheduler::Scheduler::new(
        cfg.clone(),
        http.clone(),
        store,
        deliver::Deliverer::new(cfg.clone(), http.clone(), tg),
        health,
        token.clone(),
    );
    sched.run().await;
    token.cancel();
    let _ = timeout(Duration::from_secs(5), hs).await;
    info!("bye");
    Ok(())
}

/// `x-telegram-bot check` : tests Telegram, every RSS feed and FxEmbed once. Sends nothing to your channels.
async fn check() -> Result<()> {
    let cfg = Config::from_env(true)?;
    let http = http_client(&cfg.user_agent)?;
    let mut good = true;

    let tg = telegram::Telegram::new(http.clone(), cfg.telegram_base.clone(), cfg.telegram_token.clone());
    match tg.get_me().await {
        Ok(name) => println!("Telegram bot: OK (@{name})"),
        Err(e) => {
            good = false;
            println!("Telegram bot: FAILED ({e}); check TELEGRAM_BOT_TOKEN");
        }
    }

    let mut sources = x_source::Sources::new(http.clone(), cfg.rss_sources.clone());
    for acc in &cfg.accounts {
        match sources.fetch_feed(&acc.handle).await {
            Ok(items) => {
                println!("@{}: feed OK, {} posts via {}", acc.handle, items.len(), sources.primary());
                if let Some(it) = items.first() {
                    match x_source::fetch_post(&http, &cfg.fx_base, &it.handle, &it.id, it.reposter.clone()).await {
                        Ok(Some(_)) => println!("@{}: FxEmbed OK", acc.handle),
                        Ok(None) => println!("@{}: FxEmbed answered, but the newest post is unavailable", acc.handle),
                        Err(e) => {
                            good = false;
                            println!("@{}: FxEmbed FAILED ({e:#})", acc.handle);
                        }
                    }
                }
            }
            Err(e) => {
                good = false;
                println!("@{}: FAILED ({e:#})", acc.handle);
            }
        }
    }
    if let Some(fy) = &cfg.for_you {
        let mut browser = x_browser::Browser::new(fy.browser.clone());
        match browser.fetch_for_you().await {
            Ok(items) => {
                println!("For you: OK, {} posts found (first ids: {})", items.len(), items.iter().take(3).map(|i| i.id.as_str()).collect::<Vec<_>>().join(", "));
                if items.is_empty() {
                    good = false;
                    println!("For you: the page rendered but no posts were found; X's layout may have changed");
                }
            }
            Err(e) => {
                good = false;
                println!("For you: FAILED ({e:#})");
            }
        }
        browser.shutdown().await;
    }
    println!("{}", if good { "All good." } else { "Some checks failed: see README, Troubleshooting." });
    if good {
        Ok(())
    } else {
        std::process::exit(1)
    }
}

/// `x-telegram-bot fetch-test <username>` : feed + parser only, prints what the bot would see.
async fn fetch_test(args: &[String]) -> Result<()> {
    let name = args.get(2).ok_or_else(|| anyhow!("usage: fetch-test <username>"))?;
    let handle = config::norm_handle(name)?;
    if std::env::var("TELEGRAM_BOT_TOKEN").map(|v| v.trim().is_empty()).unwrap_or(true) {
        std::env::set_var("TELEGRAM_BOT_TOKEN", "unused-by-fetch-test"); // this command never talks to Telegram
    }
    let cfg = Config::from_env(false)?;
    let http = http_client(&cfg.user_agent)?;
    let mut sources = x_source::Sources::new(http.clone(), cfg.rss_sources.clone());
    let items = sources.fetch_feed(&handle).await?;
    println!("{} posts via {}", items.len(), sources.primary());
    for it in items.iter().take(5) {
        match x_source::fetch_post(&http, &cfg.fx_base, &it.handle, &it.id, it.reposter.clone()).await {
            Ok(Some(p)) => println!(
                "{} | @{} | likes {} | reply {} | photos {} | videos {} | repost by {:?} | {}",
                p.id,
                p.handle,
                p.likes,
                p.reply,
                p.photos.len(),
                p.videos.len(),
                p.reposter,
                p.text.chars().take(60).collect::<String>().replace('\n', " ")
            ),
            Ok(None) => println!("{} | unavailable", it.id),
            Err(e) => println!("{} | error: {e:#}", it.id),
        }
    }
    if items.is_empty() {
        std::process::exit(2);
    }
    Ok(())
}
