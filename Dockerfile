#
# Multi-stage build for the icegres GA image.
#
#   docker build -t icegres:local .
#   docker run --rm -p 5439:5439 -p 8080:8080 \
#     -e ICEGRES_CATALOG_URI=https://catalog.example.com/catalog \
#     -e ICEGRES_WAREHOUSE=prod \
#     -e ICEGRES_S3_ENDPOINT=https://s3.example.com \
#     -e ICEGRES_S3_ACCESS_KEY=... -e ICEGRES_S3_SECRET_KEY=... \
#     icegres:local serve --host 0.0.0.0 --health-port 8080 \
#       --auth-file /etc/icegres/users --insecure=false
#
# The runtime stage is a slim Debian with a non-root user and CA certificates
# only — no compiler, no shell tooling beyond the base. See docs/deployment.md.

# ---- builder ---------------------------------------------------------------
# Pinned to the same toolchain as rust-toolchain.toml. TLS is rustls
# (pure-Rust), so the build needs no system OpenSSL.
FROM rust:1.96.1-bookworm AS builder

WORKDIR /build

# Warm the dependency layer first: copy only the manifests (and the second
# bin's source, which the crate references) so `cargo build` compiles the huge
# pinned dependency graph (datafusion 52 / arrow 57 / iceberg 0.9.1 / tonic
# 0.14) into a cached layer that only busts when Cargo.toml/Cargo.lock change.
COPY icegres/Cargo.toml icegres/Cargo.lock ./icegres/
RUN mkdir -p icegres/src/bin \
 && echo 'fn main() {}' > icegres/src/main.rs \
 && echo 'fn main() {}' > icegres/src/bin/icegresd.rs \
 && printf 'fn main() {}\n' > icegres/build.rs \
 && cargo build --release --manifest-path icegres/Cargo.toml \
 && rm -rf icegres/src icegres/build.rs

# Now the real sources. `.git` is copied (see .dockerignore) so build.rs can
# stamp the commit SHA into `icegres --version`; without it the stamp is
# "unknown" and the build still succeeds.
COPY .git ./.git
COPY icegres ./icegres
# Touch so cargo rebuilds the crate (not the cached deps) with real sources.
RUN touch icegres/src/main.rs icegres/build.rs \
 && cargo build --release --manifest-path icegres/Cargo.toml \
 && cp icegres/target/release/icegres  /usr/local/bin/icegres \
 && cp icegres/target/release/icegresd /usr/local/bin/icegresd

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates: rustls verifies the REST catalog / S3 TLS chains against the
# system trust store. tini: PID 1 signal forwarding so SIGTERM reaches icegres
# and triggers the graceful drain (R14) instead of being swallowed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

# Non-root, no home, no shell login. UID/GID fixed so volume ownership is
# predictable across hosts.
RUN groupadd --gid 10001 icegres \
 && useradd  --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin icegres

COPY --from=builder /usr/local/bin/icegres  /usr/local/bin/icegres
COPY --from=builder /usr/local/bin/icegresd /usr/local/bin/icegresd

USER 10001:10001

# pgwire (5439), Arrow Flight SQL (50051), health/metrics (8080 by convention;
# enable with --health-port 8080). Documentation only — publish with -p.
EXPOSE 5439 50051 8080

# No Docker HEALTHCHECK by design: this service's health is served on
# --health-port (liveness `/health`, catalog-aware readiness `/ready`,
# `/metrics`) for orchestrators to probe — the correct split for k8s/Nomad
# (see docs/deployment.md). A Docker HEALTHCHECK that connects to the catalog
# would conflate a catalog outage with an unhealthy container; a pure-TCP one
# would need extra tooling in this minimal image. Enable the port with
# `serve --health-port 8080` and point your platform's probes at it.

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/icegres"]
CMD ["serve", "--host", "0.0.0.0"]
