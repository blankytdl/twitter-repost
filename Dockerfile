# ---- build stage ----
FROM rust:1.89-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release --locked

# ---- runtime stage (glibc, needed by the Lightpanda binary) ----
FROM debian:bookworm-slim
# Pinned to the official Lightpanda 1.0.3 Linux x86_64 release; the checksum is verified at build time.
ARG LIGHTPANDA_VERSION=1.0.3
ARG LIGHTPANDA_SHA256=53eb34da09159dcf8f04865dbfe3e63e9a652062684495aa7ced5ddd7c82e8b0
ARG LIGHTPANDA_ASSET=lightpanda-x86_64-linux

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/local/bin/lightpanda \
      "https://github.com/lightpanda-io/browser/releases/download/${LIGHTPANDA_VERSION}/${LIGHTPANDA_ASSET}" \
 && echo "${LIGHTPANDA_SHA256}  /usr/local/bin/lightpanda" | sha256sum -c - \
 && chmod +x /usr/local/bin/lightpanda

COPY --from=build /src/target/release/x-telegram-bot /usr/local/bin/x-telegram-bot
RUN useradd -r -m bot \
 && mkdir -p /data \
 && chown -R bot:bot /data
VOLUME ["/data"]
USER bot
ENV BROWSER_BIN=/usr/local/bin/lightpanda \
    LIGHTPANDA_DISABLE_TELEMETRY=true \
    DATA_DIR=/data \
    STATE_PATH=/data/seen.json \
    LOG_PATH=/tmp/xtb-logs/bot.log \
    RUST_LOG=info
CMD ["x-telegram-bot"]
