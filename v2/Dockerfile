FROM rust:1.85-bookworm AS build
WORKDIR /build
COPY v2/ /build/v2/
RUN cargo build --locked --release --manifest-path /build/v2/Cargo.toml --package shopping-sessions-app

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /build/v2/target/release/shopping-sessions-v2 /app/shopping-sessions-v2
COPY v2/run.sh /app/run.sh
RUN chmod 0555 /app/run.sh /app/shopping-sessions-v2
ENV RUST_BACKTRACE=1
WORKDIR /app
CMD ["/app/run.sh"]
