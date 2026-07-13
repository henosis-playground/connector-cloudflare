FROM rust:1.96-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p henosis-cloudflare-server

FROM node:24-bookworm-slim
RUN npm install --global wrangler@4.82.2 && apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/henosis-cloudflare-server /usr/local/bin/henosis-cloudflare-server
ENTRYPOINT ["/usr/local/bin/henosis-cloudflare-server"]
