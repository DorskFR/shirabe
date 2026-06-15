DATABASE_URL ?= postgres://musicbrainz:musicbrainz@localhost:5490/musicbrainz_db
SHIRABE_BIND ?= 0.0.0.0:8800

export DATABASE_URL
export SHIRABE_BIND

.PHONY: build check test fmt lint clean run
.PHONY: db/up db/down db/migrate/up db/psql
.PHONY: image/build image/push image/release

# The homelab builds shirabe into Harbor (harbor.dorsk.dev, project "cyberia").
# Override IMAGE_REGISTRY to publish elsewhere (e.g. ghcr.io/dorskfr).
IMAGE_REGISTRY ?= harbor.dorsk.dev/cyberia
IMAGE_REPO     ?= shirabe
IMAGE_VERSION  ?= $(shell awk -F'"' '/^\[package\]/{f=1} f && /^version/{print $$2; exit}' Cargo.toml)
IMAGE          ?= $(IMAGE_REGISTRY)/$(IMAGE_REPO)

# ── Build ──────────────────────────────────────────────────

build:  ## Build in release mode
	cargo build --release

check:  ## Type check
	cargo check

# ── Format & Lint ──────────────────────────────────────────

fmt:  ## Auto-format (nightly rustfmt for unstable options)
	cargo +nightly fmt

lint:  ## Run clippy with deny warnings
	cargo clippy --all-targets -- -D warnings

# ── Test ───────────────────────────────────────────────────

test:  ## Run unit tests (no DB required)
	cargo test

# ── Run ────────────────────────────────────────────────────

run:  ## Run the server locally (needs DATABASE_URL pointing at a MB mirror)
	cargo run

# ── Database (MusicBrainz mirror) ──────────────────────────

db/up:  ## Start the local (empty) postgres for smoke-testing
	docker compose up -d shirabe-postgres
	@until docker exec shirabe-postgres pg_isready -U musicbrainz > /dev/null 2>&1; do sleep 1; done
	@echo "Postgres ready on port 5490"

db/down:  ## Stop the local postgres
	docker compose down -v --remove-orphans

db/migrate/up:  ## Apply the shirabe index migration to $(DATABASE_URL)
	sqlx migrate run --source migrations

db/psql:  ## Open psql shell to the local database
	docker exec -it shirabe-postgres psql -U musicbrainz -d musicbrainz_db

# ── Image ──────────────────────────────────────────────────

image/build:  ## Build container image tagged with the Cargo.toml version (no :latest)
	docker build --platform linux/amd64 -f deploy/Dockerfile \
	  -t $(IMAGE):$(IMAGE_VERSION) .

image/push:  ## Push the versioned image tag
	docker push $(IMAGE):$(IMAGE_VERSION)

image/release: image/build image/push  ## Build + push container image

# ── Clean ──────────────────────────────────────────────────

clean:  ## Remove build artifacts
	cargo clean

# ── Help ───────────────────────────────────────────────────

help:  ## Show this help
	@grep -E '^[a-zA-Z_/]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*##"}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
