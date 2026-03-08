#!/bin/bash
# sync-public.sh -- Sync prisma-rs/ directory to public repo
#
# The private repo has a directory that maps to a public repo:
#   prisma-rs/    -> Shuozeli/prisma-rs   (Rust Prisma implementation)
#
# Usage:
#   ./scripts/sync-public.sh                    # sync and push
#   ./scripts/sync-public.sh --dry-run          # show what would be synced, don't push
#   ./scripts/sync-public.sh -m "commit msg"    # use custom commit message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
PROJECTS_DIR="$(dirname "$REPO_ROOT")"

# Source directory (inside this repo)
PRISMA_RS_SRC="$REPO_ROOT/prisma-rs"

# Target public repo clone
PRISMA_RS_DST="$PROJECTS_DIR/prisma-rs-public"

DRY_RUN=false
COMMIT_MSG=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        -m) COMMIT_MSG="$2"; shift 2 ;;
        *) echo "Usage: $0 [--dry-run] [-m \"message\"]"; exit 1 ;;
    esac
done

log() { echo "[sync] $*"; }
die() { echo "[sync] ERROR: $*" >&2; exit 1; }

sync_prisma_rs() {
    log "Syncing prisma-rs -> $PRISMA_RS_DST"
    [[ -d "$PRISMA_RS_DST" ]] || die "$PRISMA_RS_DST does not exist. Clone Shuozeli/prisma-rs first."

    # Root-level files
    for f in README.md LICENSE llms.txt Cargo.toml Cargo.lock Makefile \
             docker-compose.yml rust-toolchain.toml rustfmt.toml; do
        [[ -f "$PRISMA_RS_SRC/$f" ]] && cp "$PRISMA_RS_SRC/$f" "$PRISMA_RS_DST/$f"
    done

    # .github/workflows
    if [[ -d "$PRISMA_RS_SRC/.github" ]]; then
        rsync -a --delete "$PRISMA_RS_SRC/.github/" "$PRISMA_RS_DST/.github/"
    fi

    # scripts/
    if [[ -d "$PRISMA_RS_SRC/scripts" ]]; then
        rsync -a --delete "$PRISMA_RS_SRC/scripts/" "$PRISMA_RS_DST/scripts/"
    fi

    # Derive crate list from workspace Cargo.toml members
    local crates
    crates=$(grep -oP '^\s*"([^"]+)"' "$PRISMA_RS_SRC/Cargo.toml" \
        | sed 's/.*"\([^"]*\)".*/\1/' \
        | head -20)

    for crate in $crates; do
        if [[ -d "$PRISMA_RS_SRC/$crate" ]]; then
            log "  rsync $crate/"
            rsync -a --delete \
                --exclude='target' \
                "$PRISMA_RS_SRC/$crate/" "$PRISMA_RS_DST/$crate/"
        fi
    done

    commit_and_push "$PRISMA_RS_DST" "Shuozeli/prisma-rs"
}

commit_and_push() {
    local dst="$1"
    local repo_name="$2"

    cd "$dst"
    if git diff --quiet 2>/dev/null && git diff --cached --quiet 2>/dev/null && [ -z "$(git ls-files --others --exclude-standard 2>/dev/null)" ]; then
        log "  No changes to sync."
    else
        git diff --stat 2>/dev/null || true
        if [[ "$DRY_RUN" == true ]]; then
            log "  [dry-run] Would commit and push."
        else
            local msg="${COMMIT_MSG:-sync: update from private repo $(date +%Y-%m-%d)}"
            git add -A
            git commit -m "$msg

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
            git push
            log "  Pushed to $repo_name."
        fi
    fi
}

log "Syncing prisma-rs to public repo"
[[ "$DRY_RUN" == true ]] && log "DRY RUN MODE"
echo

sync_prisma_rs

log "Done."
