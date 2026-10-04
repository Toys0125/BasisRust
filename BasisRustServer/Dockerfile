FROM rust:1.95.0-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 AS build
WORKDIR /src
COPY . .
RUN CARGO_BUILD_JOBS=2 cargo build --locked --release -p basis-server-console

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /src/target/release/basis-server-console /usr/local/bin/basis-server-console
ENV SetPort=4296 \
    EnableConsole=false
EXPOSE 4296/udp
EXPOSE 10666/tcp
CMD ["basis-server-console", "--base-dir", "/app", "--config", "config/config.xml", "--no-console"]
