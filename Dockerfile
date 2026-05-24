# Multi-stage build for `dd-rust-network-mutex` (the live-mutex-rs broker).
#
# Stage 1: build the release binary against a pinned Rust toolchain.
#          Default features (`tls`, `otel`) are on so the resulting image
#          works as both a TLS-fronted broker and an OTel-instrumented
#          one out of the box. To opt out, override the build with
#          `--build-arg CARGO_BUILD_FLAGS="--no-default-features"`.
#
# Stage 2: minimal runtime image with only the binary and CA certs.
#          Runs as a non-root user (uid:gid 65532:65532) and exposes the
#          two listeners the broker uses: TCP 6970 (newline-delimited
#          JSON wire protocol) and HTTP 6971 (status page + serverless
#          callers + Prometheus `/metrics`).
#
# Build:
#   docker build -t oresoftware/live-mutex-rs:0.1.124 .
#
# Run:
#   docker run --rm -p 6970:6970 -p 6971:6971 \
#     oresoftware/live-mutex-rs:0.1.124
#
# See `readme.md` for the full env-var surface
# (`LMX_BIND_HOST`, `LMX_TCP_PORT`, `LMX_HTTP_PORT`, `LMX_AUTH_TOKEN`,
# `LMX_DEFAULT_TTL_MS`, etc.).

FROM rust:1.90-bookworm AS build

ARG CARGO_BUILD_FLAGS=""

WORKDIR /app

# Cache deps separately from src to keep rebuilds fast.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
  && echo "fn main() {}" > src/main.rs \
  && echo "" > src/lib.rs \
  && cargo build --release ${CARGO_BUILD_FLAGS} \
  && rm -rf src

COPY src ./src
COPY tests ./tests
COPY examples ./examples
COPY PROTOCOL.md ./PROTOCOL.md
COPY readme.md ./readme.md
COPY LICENSE ./LICENSE

RUN cargo build --release --bin dd-rust-network-mutex ${CARGO_BUILD_FLAGS}

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/dd-rust-network-mutex /usr/local/bin/dd-rust-network-mutex

ENV LMX_BIND_HOST=0.0.0.0 \
    LMX_TCP_PORT=6970 \
    LMX_HTTP_PORT=6971 \
    LMX_LOG_FORMAT=text \
    RUST_LOG=info,lmx=info

EXPOSE 6970 6971

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/dd-rust-network-mutex"]
CMD []
