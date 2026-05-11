# ── Stage 1: build ───────────────────────────────────────────────────────────
# When building ON the Orange Pi this image will be pulled as arm64 automatically.
# Cross-compiling from x86: docker buildx build --platform linux/arm64 .
FROM rust:1.78-slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
        pkg-config \
        libudev-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies separately from source so rebuilds are fast.
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release \
    && rm -f target/release/deps/modbus_mqtt_bridge* src/main.rs

COPY src ./src
RUN touch src/main.rs && cargo build --release

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
        libudev1 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# The serial device group is usually 'dialout' (gid 20) on Debian/Ubuntu.
# Run the container with --group-add dialout or set the device permissions on the host.
COPY --from=builder /app/target/release/modbus-mqtt-bridge /usr/local/bin/modbus-mqtt-bridge

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/modbus-mqtt-bridge"]
