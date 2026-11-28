# Build on trixie-based rust so the binary matches the trixie-slim runtime
# glibc. The build is self-contained: public base images + crates.io deps, no
# MusicBrainz DB required (sqlx runtime queries, not compile-time macros).
FROM rust:1.94-slim-trixie AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY migrations/ migrations/

RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/shirabe /usr/local/bin/shirabe
COPY --from=builder /app/migrations /migrations
EXPOSE 8800
CMD ["shirabe"]
