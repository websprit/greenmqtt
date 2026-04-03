FROM rust:1.89-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p greenmqtt-cli

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/greenmqtt-cli /usr/local/bin/greenmqtt
ENTRYPOINT ["/usr/local/bin/greenmqtt"]

