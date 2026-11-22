#!/usr/bin/env bash
# Offline checks (unit + end-to-end tests against built-in fake servers), then, if your real
# settings are exported, a live connectivity check that sends nothing to your channels.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --locked
cargo build --release --locked
if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${X_ACCOUNTS:-}" ]; then
  echo "--- live check"
  ./target/release/x-telegram-bot check
else
  echo "Set TELEGRAM_BOT_TOKEN, X_ACCOUNTS and DEFAULT_CHAT_ID to also run the live check."
fi
