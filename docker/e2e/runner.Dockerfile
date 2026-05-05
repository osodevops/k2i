FROM rust:1-bookworm

RUN apt-get update && apt-get install -y \
    ca-certificates \
    cmake \
    curl \
    libssl-dev \
    libsasl2-dev \
    pkg-config \
    python3 \
    python3-pip \
    && pip3 install --break-system-packages duckdb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY . .

CMD ["cargo", "run", "-p", "k2i-e2e-runner"]
