# ── Stage 1: Builder ────────────────────────────────────────────────────────
# Uses dynamic-linking against system librdkafka — no cmake compilation needed.
FROM rust:1.78-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    librdkafka-dev \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency compilation separately from source changes.
COPY Cargo.toml ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Rebuild with real source (only changed crates recompile)
COPY src ./src
COPY config.toml ./
RUN touch src/main.rs && cargo build --release

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
# slim image with librdkafka runtime library present
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/datahub-kafka-consumer /app/datahub-kafka-consumer
COPY --from=builder /app/config.toml /app/config.toml

EXPOSE 3001

ENTRYPOINT ["/app/datahub-kafka-consumer"]
