FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p basis-server-console

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /src/target/release/basis-server-console /usr/local/bin/basis-server-console
ENV SetPort=4296 \
    EnableConsole=false \
    Password=default_password
EXPOSE 4296/udp
EXPOSE 10666/tcp
CMD ["basis-server-console", "--config", "config/config.xml", "--no-console"]

