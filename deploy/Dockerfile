# Build from the repository root:
#   docker build -f deploy/Dockerfile -t raftkv:1.0.0 .
FROM rust:1.89-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo build --release --locked --bin raftkv-server --bin raftkv-cli

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /var/lib/raftkv raftkv \
 && mkdir -p /var/lib/raftkv \
 && chown raftkv:raftkv /var/lib/raftkv
COPY --from=build /src/target/release/raftkv-server /usr/local/bin/
COPY --from=build /src/target/release/raftkv-cli /usr/local/bin/
USER raftkv
VOLUME /var/lib/raftkv
EXPOSE 7000 7100 9100
# SIGTERM triggers a leadership hand-off before exit.
STOPSIGNAL SIGTERM
ENTRYPOINT ["raftkv-server"]
