# Prisma TypeScript to Rust Migration

## Goal

Replace the Prisma ORM TypeScript implementation with a pure Rust implementation.
The Rust implementation should be a drop-in replacement: same Prisma schema input,
same client API behavior, same query results, same migration workflow.

## Two-Repository Architecture

The project is split into an internal monorepo and a public repo:

| Directory | Visibility | Public Repo | Contents |
|-----------|-----------|-------------|----------|
| `/` (root) | Private | (this repo) | Internal monorepo: reference TS code, engines, docs, scripts |
| `prisma-rs/` | Public | `Shuozeli/prisma-rs` | Rust implementation: 17-crate Cargo workspace |

**CRITICAL structural difference:**
- In the **private** repo, the Cargo workspace is at `<git-root>/prisma-rs/`
- In the **public** repo, the Cargo workspace IS `<git-root>/`
- All scripts, hooks, and CI must work in BOTH layouts. Never hardcode
  `$(git rev-parse --show-toplevel)/prisma-rs` -- detect the workspace
  location by checking where `Cargo.toml` exists.

**Internal-only directories** (never synced to public):
- `prisma/` -- Original TypeScript Prisma repo (reference only)
- `prisma-engines/` -- Upstream Rust engines repo (reference only)
- `docs/` -- Internal migration docs, task tracking, phase plans
- `scripts/` -- Sync scripts, internal tooling

**Public directory** (`prisma-rs/`):
- Self-contained Cargo workspace, builds independently
- Has its own README.md, LICENSE, CI, docs, scripts (including pre-commit hooks)
- Synced to `Shuozeli/prisma-rs` via `scripts/sync-public.sh`

**Public repo clone location:** `../prisma-rs-public/` (sibling of the private monorepo root)

## Repository Layout

```
prisma-rs/                         Root repository (PRIVATE)
  prisma/                          Original TypeScript Prisma repo (reference only)
    packages/cli                   CLI entry point
    packages/client                Client runtime
    packages/client-engine-runtime Query interpreter (TypeScript)
    packages/client-generator-ts   prisma-client generator
    packages/client-generator-js   prisma-client-js generator (legacy)
    packages/migrate               Migrate/DB CLI commands
    packages/config                PrismaConfigInternal + loader
    packages/driver-adapter-utils  Driver adapter interfaces
    packages/adapter-*             Database driver adapters (pg, mysql, sqlite, etc.)
    packages/internals             Shared CLI + engine glue
    packages/engines               Rust binaries download wrapper
    packages/integration-tests     Matrix test suites
  prisma-engines/                  Upstream Rust engines (reference only)
  prisma-rs/                       Rust implementation (PUBLIC -> Shuozeli/prisma-rs)
    driver-core/                   Database driver trait + common types
    driver-pg/                     PostgreSQL driver (tokio-postgres + deadpool)
    driver-mysql/                  MySQL driver (mysql_async)
    driver-sqlite/                 SQLite driver (rusqlite)
    driver-duckdb/                 DuckDB driver (ADBC-based)
    driver-adbc/                   ADBC (Arrow Database Connectivity) driver
    driver-flightsql/              Arrow Flight SQL driver
    prisma-schema/                 Schema types (mirrors Prisma schema AST)
    prisma-compiler/               Query planning and compilation
    prisma-migrate/                Schema migration engine (RPC bridge)
    prisma-error/                  Shared error types
    cross-compat/                  Cross-compat testing (Rust vs TypeScript)
    query-executor/                In-memory query execution engine
    prisma-client/                 Client runtime (Accelerate, transactions)
    prisma-codegen/                Client code generation (Rust, TypeScript)
    prisma-cli/                    CLI binary (migrate, generate, db push/pull)
  docs/                            Internal migration documentation
    migration-plan.md              Phased migration plan
    tasks.md                       Task tracking (code review, security)
    phases.md                      Phase summaries
  scripts/                         Internal scripts
    sync-public.sh                 Sync prisma-rs/ to public repo
```

## Key Context

The original Prisma project already has some Rust components:

- **PSL parser** (schema parsing) -- Rust, compiled to Wasm
- **Query compiler/planner** -- Rust, compiled to Wasm
- **Schema engine** (migrations) -- Rust, native binary

These live in a separate `prisma-engines` repo. Our migration replaces the
**TypeScript** components (query execution, client runtime, CLI, generators,
driver adapters) with Rust, and consolidates everything into one Rust workspace.

## What Works

- 16-crate Cargo workspace builds and passes tests
- PostgreSQL, MySQL, SQLite drivers with connection pooling
- DuckDB, ADBC, and Flight SQL drivers
- Query execution engine with in-memory filtering, sorting, pagination, aggregation
- Schema parsing and code generation
- Migration engine (RPC bridge to schema engine)
- CLI with migrate, generate, db push/pull/execute commands
- Cross-compatibility test framework
- CI pipeline (GitHub Actions) with fmt, clippy, build, test, audit
- Pre-commit hooks (fmt + clippy)
- Production security audit completed (20 findings resolved)

## What We Are Building

See `docs/migration-plan.md` for the detailed phased plan.

## Key Design Decisions

1. **Consolidate Rust code** -- Merge prisma-engines components (PSL parser,
   query compiler, schema engine) into this workspace rather than maintaining
   separate repos.
2. **Cross-compat testing** -- Run the same queries through both TypeScript and
   Rust implementations, assert identical results against all supported databases.
3. **Golden test system** -- Each test is a Prisma schema + operations + expected
   results triple, auto-discovered by the test framework.
4. **Driver trait abstraction** -- Database-specific behavior behind a trait, with
   implementations for PostgreSQL, MySQL, SQLite, DuckDB, FlightSQL, ADBC.
5. **Incremental replacement** -- Each component can be swapped independently.
   Start with query execution (most performance-critical), then expand outward.

## Development Commands

```bash
# --- Rust implementation ---
cd prisma-rs

# Build all crates
cargo build --workspace

# Run all tests
cargo test --workspace

# Regenerate golden files after intentional output changes
UPDATE_GOLDEN=1 cargo test --workspace

# --- Sync to public repo ---
./scripts/sync-public.sh                    # validate (fmt+clippy), sync, commit, push
./scripts/sync-public.sh --dry-run          # preview without pushing
./scripts/sync-public.sh -m "commit msg"    # custom commit message
./scripts/sync-public.sh --skip-validate    # skip fmt/clippy (use with caution)
./scripts/sync-public.sh --force-push       # force push (for squash workflows)

# --- Install pre-commit hooks (do this in BOTH repos) ---
cd prisma-rs && ./scripts/install-hooks.sh  # private repo
cd ../prisma-rs-public && ./scripts/install-hooks.sh  # public repo clone

# --- Original TypeScript (reference) ---
cd prisma

# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests for a specific package
pnpm --filter @prisma/<pkg> test
```

## Rules

- **Do not modify the TypeScript source.** We are replacing it, not patching it.
  The `prisma/` directory is reference-only.
- **Cross-compat tests are the ground truth.** If our Rust output differs from
  the TypeScript implementation, we are wrong (unless the TS output has a known bug).
- **Golden tests must pass before any commit.** Run `cargo test --workspace`.
- **Use `UPDATE_GOLDEN=1` only after verifying** the output change is intentional.
- **Query results must be identical.** For the same schema and input, the Rust
  implementation must produce the same query results as the TypeScript one.
- **No panics in production paths.** Malformed schema input, bad queries, and
  driver errors should produce proper error types, not panics.
- **Wire compatibility is non-negotiable.** Driver adapter wire protocol, query
  plan format, and client API surface must match the TypeScript implementation
  for the same schema.
- **Test against real databases.** Unit tests with mocked drivers are insufficient.
  Integration tests must run against PostgreSQL, MySQL, and SQLite.
- **No SQL Server support.** SQL Server is explicitly out of scope for this project.
- **No HTTP-based driver adapters.** Neon, PlanetScale, Cloudflare D1, and Prisma
  Postgres are out of scope. These are serverless/edge HTTP adapters from the
  original TypeScript Prisma for environments without TCP access. Our Rust
  implementation targets native server environments with direct database connections.
- **Public repo must be self-contained.** `prisma-rs/` must build and test
  independently without any files from the root repo. Never add cross-directory
  path dependencies.
- **Sync before public pushes.** Use `scripts/sync-public.sh` to sync changes
  to the public repo. Never push to the public repo directly.
- **Never push the root repo to the public remote.** The git root
  (`/home/cyuan/projects/prisma-rs/`) is the private monorepo. The `prisma-rs/`
  subdirectory is what gets synced to `Shuozeli/prisma-rs`. Never run `git push`
  from the root repo -- it would push private code (prisma-engines, docs, scripts)
  to the public repo. Always use `scripts/sync-public.sh` for public pushes.
  When committing, commit in the root repo but only push via the sync script.
- **Sync script validates before pushing.** The sync script runs `cargo fmt` and
  `cargo clippy` before syncing. If validation fails, fix the issues first. Do
  not use `--skip-validate` unless you have a good reason.
- **Scripts must be repo-layout agnostic.** The public repo is a flat workspace
  (Cargo.toml at root) while the private repo nests it under `prisma-rs/`. Any
  script or hook that references the workspace must detect its location, not
  hardcode a path. Use: `if [ -f "$ROOT/Cargo.toml" ]; then ... elif [ -f "$ROOT/prisma-rs/Cargo.toml" ]; then ... fi`
- **CI package names must match Cargo.toml `name` fields.** The `cargo test -p`
  flag uses the package `name` from each crate's Cargo.toml, NOT the directory
  name. E.g., directory `cross-compat/` has package name `prisma-cross-compat`.
  When adding new crates, verify the CI yaml uses the correct package name.
- **Install hooks in both repos.** After cloning or setting up either repo,
  run `./scripts/install-hooks.sh` to install pre-commit hooks.
