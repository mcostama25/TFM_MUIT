# ── Stage 1: Builder ────────────────────────────────────────────────────────
# Uses cmake-build to compile bundled librdkafka 2.12.1 from source.
# librdkafka is statically linked — no system librdkafka needed at runtime.
FROM rust:1.86-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    build-essential \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency compilation separately from source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Rebuild with real source (only changed crates recompile)
COPY src ./src
COPY config.toml ./
RUN touch src/main.rs && cargo build --release

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
# librdkafka is statically linked into the binary — no runtime library needed.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/datahub-kafka-consumer /app/datahub-kafka-consumer
COPY --from=builder /app/config.toml /app/config.toml

EXPOSE 3001

ENTRYPOINT ["/app/datahub-kafka-consumer"]
