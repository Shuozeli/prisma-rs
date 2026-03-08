#!/bin/bash
# sync-public.sh -- Sync prisma-rs/ directory to public repo (Shuozeli/prisma-rs)
#
# Architecture:
#   Private monorepo:  /home/cyuan/projects/prisma-rs/        (git root, NEVER pushed directly)
#   Public code source: prisma-rs/                            (Cargo workspace subdirectory)
#   Public repo clone:  ../prisma-rs-public/                  (clone of Shuozeli/prisma-rs)
#
# The public repo is a FLAT workspace -- Cargo.toml is at the repo root, NOT in a
# prisma-rs/ subdirectory. Scripts and hooks must work in both layouts.
#
# Usage:
#   ./scripts/sync-public.sh                          # validate, sync, commit, push
#   ./scripts/sync-public.sh --dry-run                # show what would change, don't push
#   ./scripts/sync-public.sh -m "commit msg"          # custom commit message
#   ./scripts/sync-public.sh --skip-validate          # skip fmt/clippy (use with caution)
#   ./scripts/sync-public.sh --force-push             # force push (for squash workflows)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
PROJECTS_DIR="$(dirname "$REPO_ROOT")"

# Source: the Cargo workspace inside the private monorepo
SRC="$REPO_ROOT/prisma-rs"

# Destination: the public repo clone (sibling directory)
DST="$PROJECTS_DIR/prisma-rs-public"

DRY_RUN=false
SKIP_VALIDATE=false
FORCE_PUSH=false
COMMIT_MSG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)       DRY_RUN=true; shift ;;
        --skip-validate) SKIP_VALIDATE=true; shift ;;
        --force-push)    FORCE_PUSH=true; shift ;;
        -m)              COMMIT_MSG="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --dry-run        Show what would change without pushing"
            echo "  --skip-validate  Skip cargo fmt/clippy checks before sync"
            echo "  --force-push     Use git push --force (for squash workflows)"
            echo "  -m \"message\"     Custom commit message"
            echo "  -h, --help       Show this help"
            exit 0
            ;;
        *) echo "Unknown option: $1. Use --help for usage."; exit 1 ;;
    esac
done

log()  { echo "[sync] $*"; }
warn() { echo "[sync] WARN: $*" >&2; }
die()  { echo "[sync] ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Step 1: Preflight checks
# ---------------------------------------------------------------------------

log "=== Preflight checks ==="

# Verify source exists
[[ -f "$SRC/Cargo.toml" ]] || die "Source workspace not found at $SRC/Cargo.toml"

# Verify destination exists and is a git repo
[[ -d "$DST/.git" ]] || die "Public repo clone not found at $DST. Run: git clone git@github.com:Shuozeli/prisma-rs.git $DST"

# Verify destination is on main branch
DST_BRANCH=$(cd "$DST" && git branch --show-current)
if [[ "$DST_BRANCH" != "main" ]]; then
    die "Public repo is on branch '$DST_BRANCH', expected 'main'. Fix before syncing."
fi

# Verify destination has correct remote
DST_REMOTE=$(cd "$DST" && git remote get-url origin 2>/dev/null || echo "none")
if [[ "$DST_REMOTE" != *"Shuozeli/prisma-rs"* ]]; then
    warn "Public repo remote is '$DST_REMOTE', expected Shuozeli/prisma-rs"
fi

# Verify destination working tree is clean (no uncommitted local changes that we'd overwrite)
if ! (cd "$DST" && git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null); then
    die "Public repo has uncommitted changes. Commit or discard them before syncing."
fi

log "  Source:      $SRC"
log "  Destination: $DST"
log "  Branch:      $DST_BRANCH"

# ---------------------------------------------------------------------------
# Step 2: Validate source code (unless --skip-validate)
# ---------------------------------------------------------------------------

if [[ "$SKIP_VALIDATE" == false ]]; then
    log ""
    log "=== Validating source code ==="

    log "  Checking formatting..."
    if ! (cd "$SRC" && cargo fmt --all -- --check) 2>&1; then
        die "Formatting check failed. Run: cd prisma-rs && cargo fmt --all"
    fi

    log "  Running clippy..."
    if ! (cd "$SRC" && cargo clippy --all-targets -- -D warnings) 2>&1; then
        die "Clippy check failed. Fix warnings before syncing."
    fi

    log "  Validation passed."
else
    warn "Skipping validation (--skip-validate). CI may fail if code has issues."
fi

# ---------------------------------------------------------------------------
# Step 3: Parse workspace members from Cargo.toml
# ---------------------------------------------------------------------------

log ""
log "=== Syncing files ==="

# Extract workspace member directories from the [workspace] members array.
# This reads lines between 'members = [' and ']', extracting quoted strings.
CRATES=$(sed -n '/^members = \[/,/^\]/{ s/.*"\([^"]*\)".*/\1/p }' "$SRC/Cargo.toml")

if [[ -z "$CRATES" ]]; then
    die "Failed to parse workspace members from $SRC/Cargo.toml"
fi

CRATE_COUNT=$(echo "$CRATES" | wc -l)
log "  Found $CRATE_COUNT workspace members"

# ---------------------------------------------------------------------------
# Step 4: Sync files
# ---------------------------------------------------------------------------

# Root-level files (explicit list -- add new files here as needed)
ROOT_FILES=(
    README.md LICENSE llms.txt Cargo.toml Cargo.lock
    Makefile docker-compose.yml rust-toolchain.toml rustfmt.toml
)
for f in "${ROOT_FILES[@]}"; do
    if [[ -f "$SRC/$f" ]]; then
        cp "$SRC/$f" "$DST/$f"
    fi
done
log "  Synced root files"

# Directories synced in full (with --delete to remove stale files)
SYNC_DIRS=(.github scripts docs)
for dir in "${SYNC_DIRS[@]}"; do
    if [[ -d "$SRC/$dir" ]]; then
        rsync -a --delete "$SRC/$dir/" "$DST/$dir/"
        log "  Synced $dir/"
    fi
done

# Workspace crate directories
for crate in $CRATES; do
    if [[ -d "$SRC/$crate" ]]; then
        rsync -a --delete --exclude='target' "$SRC/$crate/" "$DST/$crate/"
        log "  Synced $crate/"
    else
        warn "Workspace member '$crate' directory not found at $SRC/$crate"
    fi
done

# ---------------------------------------------------------------------------
# Step 5: Verify synced code builds (sanity check)
# ---------------------------------------------------------------------------

# Quick check: ensure Cargo.toml is valid in the destination
if ! (cd "$DST" && cargo metadata --format-version 1 --no-deps > /dev/null 2>&1); then
    warn "cargo metadata failed in public repo -- Cargo.toml may be broken"
fi

# ---------------------------------------------------------------------------
# Step 6: Commit and push
# ---------------------------------------------------------------------------

log ""
log "=== Committing ==="

cd "$DST"

# Check for changes
if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null \
   && [ -z "$(git ls-files --others --exclude-standard 2>/dev/null)" ]; then
    log "No changes to sync."
    exit 0
fi

# Show what changed
git diff --stat 2>/dev/null || true
UNTRACKED=$(git ls-files --others --exclude-standard 2>/dev/null | wc -l)
if [[ "$UNTRACKED" -gt 0 ]]; then
    log "  + $UNTRACKED new files"
fi

if [[ "$DRY_RUN" == true ]]; then
    log "[dry-run] Would commit and push. Exiting."
    exit 0
fi

MSG="${COMMIT_MSG:-sync: update from private repo $(date +%Y-%m-%d)}"
git add -A
git commit -m "$MSG"

if [[ "$FORCE_PUSH" == true ]]; then
    log "Force pushing to Shuozeli/prisma-rs..."
    git push --force
else
    git push
fi

log "Pushed to Shuozeli/prisma-rs."
log ""
log "=== Done ==="
