# prisma-cli

Command-line interface for Prisma schema management, code generation, and migrations.

## Purpose

Single binary (`prisma`) that exposes all Prisma development workflow commands.
Uses `clap` for argument parsing.

## Commands

| Command | Description |
|---------|-------------|
| `prisma validate` | Validate a Prisma schema file |
| `prisma format` | Format a Prisma schema (with `--check` for CI) |
| `prisma generate` | Generate client code (`--language typescript\|rust`, `--output <path>`) |
| `prisma db push` | Push schema to database without migration history |
| `prisma db pull` | Introspect database and update schema |
| `prisma db execute` | Execute raw SQL against the database |
| `prisma migrate dev` | Create and apply a migration (`--name <name>`) |
| `prisma migrate deploy` | Apply pending migrations (production) |
| `prisma migrate reset` | Reset database and re-apply all migrations |
| `prisma migrate resolve` | Mark a migration as applied or rolled back |
| `prisma migrate diff` | Show diff between schema states |

## Common Flags

| Flag | Description |
|------|-------------|
| `--schema <path>` | Path to schema.prisma (default: `./schema.prisma`) |
| `--datasource-url <url>` | Override the datasource URL |

## Installation

```bash
cargo install --git https://github.com/Shuozeli/prisma-rs.git prisma-cli
```

## Dependencies

`prisma-schema`, `prisma-codegen`, `prisma-migrate`, `clap`
