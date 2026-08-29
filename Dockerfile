FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 brew
COPY --from=build /src/target/release/brew-server /usr/local/bin/brew-server
COPY brew-server.toml /etc/brew-server.toml
USER brew
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/brew-server", "/etc/brew-server.toml"]
