# --- Build stage ---
FROM rust:1.78-alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig openssl-dev

WORKDIR /app
COPY Cargo.toml ./
# Cache dependencies by building an empty main first
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

# --- Runtime stage ---
FROM gcr.io/distroless/static-debian12

WORKDIR /app
COPY --from=builder /app/target/release/datahub-dcat-client /app/datahub-dcat-client
COPY config.toml /app/config.toml

EXPOSE 3000
ENTRYPOINT ["/app/datahub-dcat-client"]
