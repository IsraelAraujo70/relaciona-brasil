# syntax=docker/dockerfile:1.7

# ─── build stage ──────────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    ca-certificates \
    curl \
  && rm -rf /var/lib/apt/lists/*

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release && \
    cp target/release/relaciona-brasil /usr/local/bin/relaciona-brasil

# Pré-cria o diretório de cache de dumps com ownership do usuário nonroot
# (uid/gid 65532 do distroless). Volumes do Docker copiam o estado inicial
# do mountpoint, então isso garante write-access pro worker/downloader.
RUN mkdir -p /var/empty/data/dumps && chown -R 65532:65532 /var/empty/data

# ─── runtime stage ────────────────────────────────────────────────────────────
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

COPY --from=builder /usr/local/bin/relaciona-brasil /app/relaciona-brasil
COPY --from=builder /app/migrations /app/migrations
COPY --from=builder /var/empty/data /app/data

EXPOSE 8080

ENTRYPOINT ["/app/relaciona-brasil"]
CMD ["--mode", "api"]
