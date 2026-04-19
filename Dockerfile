FROM rust:1.95-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang \
    libclang-dev \
    cmake \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p greenmqtt-cli

FROM rust:1.95-bookworm
WORKDIR /app
COPY --from=builder /app/target/release/greenmqtt-cli /usr/local/bin/greenmqtt
ENTRYPOINT ["/usr/local/bin/greenmqtt"]

