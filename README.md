# X → Telegram bot (Rust, free, Render Free)

Forwards X (Twitter) posts to a Telegram channel with native photos, albums, videos and GIFs. **No
paid X API.** It is built from your `reddit-to-telegram` bot and reuses its Lightpanda browser
client, Telegram uploader, GitHub state branch, `/health` endpoint, self-ping and Render setup.

Two sources, usable together:

| | **For you feed** (`FOR_YOU=true`) | **Chosen accounts** (`X_ACCOUNTS=...`) |
|---|---|---|
| How | Lightpanda opens `x.com/home` with your login cookies, selects the **For you** tab, scrolls, collects the posts | Free Nitter-style RSS feeds, with fallback instances |
| Login needed | Yes (two cookies) | No |
| Risk | Automating a logged-in session is against X's terms; the account can be limited or suspended | Unofficial but anonymous |
| Reliability | Depends on X's page layout and on Lightpanda being able to run X's JavaScript | Depends on public instances being up |

In both cases each post's text, likes and media come from the free FxEmbed API, so the browser part only has
to find post ids. That keeps the page-scraping code tiny.

> **Status: tested against a fake X, never against the real one.** 66 automated tests pass, including
> ones that drive the **real Lightpanda 1.0.3** against a fake x.com page that renders its timeline with
> JavaScript after a delay, swaps the list while scrolling (like X's virtualised timeline), opens on the
> Following tab, shows ads, a login wall and an error page, and checks that your cookies reach the page.
> Also tested: about 230,000 random inputs thrown at every parser (no panics), recovery after a browser fetch is cancelled mid-flight, and end-to-end flows against fake RSS / FxEmbed / Telegram / GitHub servers, and the real
> binary (seeding, delivery, `/health`, graceful shutdown, `check`). **Not tested:** the real x.com (I
> can't reach it from my sandbox), live Nitter instances, a real Telegram channel, GitHub, the Docker
> build and Render. Whether Lightpanda can run X's real JavaScript is the biggest unknown. Run
> `x-telegram-bot check` first.

## Getting your cookies (For you feed)
1. Log in to x.com in a normal browser with the account whose **For you** feed you want (use a spare account).
2. Open developer tools → *Application* (Chrome) or *Storage* (Firefox) → *Cookies* → `https://x.com`.
3. Copy the **values** of `auth_token` and `ct0` into `X_AUTH_TOKEN` and `X_CT0`.
4. Treat them like a password: only put them in Render's secret environment variables, never in git.
   Logging out or changing the password invalidates them; then `check` shows the login-page error.

## Quick start
```
cp .env.example .env     # fill it in, then export the variables (or set them in Render)
cargo run --release -- check             # live test: Telegram, every RSS feed, FxEmbed and the For you page. Sends nothing.
cargo run --release -- fetch-test NASA   # shows what the bot sees for one account
cargo run --release                      # run it
```
Locally you also need the Lightpanda binary (`BROWSER_BIN`); the Docker image installs a pinned, checksum-verified one.

## What it does
- First run per source only records existing posts ("seeding"), so nothing floods your channel.
- New posts go out oldest first. Photos → `sendPhoto` (2–10 → one album); videos → `sendVideo`; GIFs → `sendAnimation`.
  Over 50 MB or from an unexpected host → a link message instead.
- Caption = author, text, likes and "Open on X". Reposts show who reposted. If the text doesn't fit a
  caption, the media goes out with a short caption and the full text follows as messages.
- Skips ads, replies (default) and text-only posts (`MEDIA_ONLY=true`, default). Never sends posts older than `MAX_AGE_HOURS` (default 24); in an account feed a *repost* made now counts as new even if the original is older.
- The For you feed is algorithmic, so many of its posts are old; with the default 24 h limit those are marked seen and skipped. Its per-cycle budget (`FOR_YOU_MAX_NEW`) goes to the **newest** posts first; account feeds go out oldest first.
- The "seen" memory is sized automatically so it always covers the age window (an evicted id would otherwise be re-sent); `SEEN_CAP` can only raise it.
- Remembers what it sent (per channel) in a local file plus a backup on a GitHub branch. Failures are sorted into two kinds. *Temporary* ones (network errors, 5xx, Telegram flood limits / 429, FxEmbed outages) keep the order, stop the cycle for that source and are retried for up to 10 cycles, so an outage doesn't lose posts. *Permanent* ones (Telegram 4xx, a 404 media file) don't hold up the other posts and are given up after 3 cycles.
- If every account feed fails (RSS instances down or blocking) it backs off 5 min, doubling up to 1 h. The For-you source backs off on its own the same way after a login wall, X error or timeout, so a bad cookie is not retried every cycle and the account feeds are not slowed down. Both recover by themselves.

## Settings
See `.env.example` for everything with defaults. The important ones:

| Variable | Meaning |
|---|---|
| `TELEGRAM_BOT_TOKEN`, `DEFAULT_CHAT_ID` | Required |
| `FOR_YOU`, `X_AUTH_TOKEN`, `X_CT0` | Enable the For you feed and give it the login cookies |
| `FOR_YOU_CHAT_ID` | Send For you posts to a different channel |
| `FOR_YOU_MAX_NEW` | Max For-you posts examined per cycle (default 40); posts waiting for `MIN_LIKES` don't count |
| `X_ACCOUNTS` | `NASA,SpaceX,esa=@otherchannel` |
| `RSS_SOURCES` | Nitter-style instances, tried in order |
| `MEDIA_ONLY`, `INCLUDE_REPLIES`, `INCLUDE_REPOSTS`, `MIN_LIKES`, `MAX_AGE_HOURS` | Filters |
| `POLL_INTERVAL_SECS` | Default 300, or 600 when `FOR_YOU=true` (minimum 60) |
| `INSENSITIVE_X` | `true` (default): the browser runs two MIT userscripts on every X page: AgebypassX (Saganaki22; patches X's age-gate / sensitive-warning feature switches) and InsensitiveX (Alican Aktas; strips sensitive flags and tombstones from API answers). Works in Lightpanda and Chromium without any extension |
| `BROWSER_KIND`, `BROWSER_BIN` | `lightpanda` (default) or `chromium` |
| `GITHUB_TOKEN`, `GITHUB_REPO`, `GITHUB_STATE_BRANCH` | Durable state backup (recommended on Render Free) |

## Deploy on Render (free)
1. Put this folder in a GitHub repo (it contains `Dockerfile` and `render.yaml`).
2. Create a GitHub fine-grained token with **Contents: read/write** on one repo. The bot creates the `bot-state` branch itself.
3. Render → New → Blueprint (uses `render.yaml`), or a Docker web service. Enter the secrets: `TELEGRAM_BOT_TOKEN`, `GITHUB_TOKEN`, `GITHUB_REPO`, `DEFAULT_CHAT_ID`, and `X_ACCOUNTS` and/or (`FOR_YOU=true`, `X_AUTH_TOKEN`, `X_CT0`).
4. Add your bot as an admin of each target channel.
5. Watch the logs for `first run: existing posts marked seen`, then for new posts on later cycles.
6. **Keep it awake:** Render Free stops a service after ~15 minutes without inbound requests, and the self-ping can't wake a stopped service. Add a free external uptime monitor (for example UptimeRobot) that requests `https://<your-service>.onrender.com/health` every 5 minutes. 750 free hours per month covers one always-on service.

## Testing
```
cargo test --locked                       # browser tests are skipped unless LIGHTPANDA_BIN is set
LIGHTPANDA_BIN=/path/to/lightpanda cargo test --locked   # includes the real-browser tests (about 100 s)
scripts/verify.sh                         # tests + release build + live check if your settings are exported
```
CI (`.github/workflows/rust-ci.yml`) downloads the pinned Lightpanda and runs all of them.

## Honest limitations
- **GitHub state history grows.** The state file is pushed after every cycle that changed it, and each push is a commit (a few hundred KB, mostly compressible). Use a dedicated private repo for `GITHUB_REPO`, raise `STATE_FLUSH_MIN_SECS` (e.g. 1800) to push less often (a crash can then re-send up to that much recent history), and squash or recreate the `bot-state` branch now and then.
- **For you through a browser is the fragile part.** X can change its page, show a login challenge or captcha (datacenter IPs such as Render's are more likely to get one), or block automated sessions. The scanner depends on stable markers (`article[data-testid="tweet"]`, the timestamp link, the For you tab) and reports a clear error when the page has no timeline, shows a login wall or an error. `check` tells you which one.
- Lightpanda is a lightweight browser without a rendering engine; X's real front end may not run in it. If `check` says the timeline did not render, run the bot on your own PC or a VPS with `BROWSER_KIND=chromium` (headless Chrome/Chromium, needs about 1 GB RAM, **untested**) or fall back to `X_ACCOUNTS`.
- Using a logged-in account this way can get it restricted or suspended. Use a spare account.
- Account mode: public Nitter instances come and go. Add several `RSS_SOURCES` or self-host one (it must serve RSS 2.0 at `/<user>/rss`). Threads: replies are skipped by default, so only a thread's first post is forwarded. FxEmbed gives one video quality.
- Render deploys can briefly run two instances; with one shared state branch that can cause a duplicate or a last-write-wins overwrite. The disk is wiped on restart, so only the GitHub branch survives (state files over GitHub's 1 MB JSON limit are restored via the raw download). A crash between sending and the next state flush can re-send a few posts.
- If GitHub is configured and the state can't be restored after 5 tries, the bot exits instead of starting with empty history (prevents a duplicate flood). Render restarts it.

## Troubleshooting
| Message | Meaning |
|---|---|
| `X is showing its login page` | Cookies missing, wrong or expired: log in again and copy fresh ones |
| `the timeline did not render in time` | The browser can't run X's JavaScript or X is blocking it: see limitations |
| `X returned an error page` / `Rate limit exceeded` | X-side problem: the bot backs off |
| `For you page rendered but no posts were found` | X's layout changed |
| `cannot start browser` | `BROWSER_BIN` path is wrong |
| `all RSS sources failed` | Add more `RSS_SOURCES` or self-host |
| `telegram error 400: chat not found` | Wrong chat id, or the bot isn't a channel admin |
| Stops posting after a while | Render spun it down: add the external uptime monitor |

## Layout
`x_browser.rs` (Lightpanda/CDP client and For-you scanner), `x_source.rs` (RSS + FxEmbed), `deliver.rs` (captions, photos, albums, videos),
`scheduler.rs` (cycles, retries, filters), `config.rs`, `seen_store.rs` (state + GitHub backup), `telegram.rs`, `media_downloader.rs`,
`health.rs`, `self_ping.rs`, `testutil.rs` (fake servers and fake x.com for tests).
