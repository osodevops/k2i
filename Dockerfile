# Build stage
FROM rust:1.75-bookworm AS builder

WORKDIR /build

# Install dependencies for rdkafka (librdkafka requires cmake)
RUN apt-get update && apt-get install -y \
    cmake \
    libssl-dev \
    libsasl2-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build release binary
RUN cargo build --release -p k2i-cli

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /build/target/release/k2i /usr/local/bin/k2i

# Create non-root user
RUN useradd -r -u 1000 -m k2i

# Create directories for config and transaction logs
RUN mkdir -p /etc/k2i /var/lib/k2i/txlog && \
    chown -R k2i:k2i /var/lib/k2i

USER k2i

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

# Expose health and metrics ports
EXPOSE 8080 9090

ENTRYPOINT ["k2i"]
CMD ["ingest", "--config", "/etc/k2i/config.toml"]
