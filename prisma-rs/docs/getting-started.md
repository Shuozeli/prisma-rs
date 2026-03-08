# Getting Started

## Prerequisites

- Rust 1.85+ (edition 2024)
- A supported database: PostgreSQL, MySQL, or SQLite

## Installation

The fastest way to get started is to install the `prisma` binary directly from GitHub:

```bash
cargo install --git https://github.com/Shuozeli/prisma-rs.git prisma-cli
```

This compiles and installs the `prisma` binary to `~/.cargo/bin/`. Make sure
`~/.cargo/bin` is in your `PATH`.

To update to the latest version:

```bash
cargo install --git https://github.com/Shuozeli/prisma-rs.git prisma-cli --force
```

## Building from Source

```bash
# Clone the repository
git clone https://github.com/Shuozeli/prisma-rs.git
cd prisma-rs

# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace
```

## Creating a Prisma Schema

Create a `schema.prisma` file:

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-rs"
}

model User {
  id    Int     @id @default(autoincrement())
  email String  @unique
  name  String?
  posts Post[]
}

model Post {
  id        Int      @id @default(autoincrement())
  title     String
  content   String?
  published Boolean  @default(false)
  author    User     @relation(fields: [authorId], references: [id])
  authorId  Int
}
```

## CLI Usage

```bash
# Set your database URL
export DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"

# Validate schema
prisma validate --schema schema.prisma

# Format schema
prisma format --schema schema.prisma

# Push schema to database (no migration history)
prisma db push --schema schema.prisma

# Create and apply a migration
prisma migrate dev --schema schema.prisma --name init

# Deploy migrations (production)
prisma migrate deploy --schema schema.prisma

# Generate client code
prisma generate --schema schema.prisma

# Pull schema from existing database
prisma db pull --schema schema.prisma
```

If running from source instead of an installed binary, replace `prisma` with
`cargo run -p prisma-cli --`.

## Database Setup

### PostgreSQL

```bash
# Using Docker
docker run -d --name postgres \
  -e POSTGRES_USER=prisma \
  -e POSTGRES_PASSWORD=prisma \
  -e POSTGRES_DB=prisma \
  -p 5432:5432 \
  postgres:16

export DATABASE_URL="postgresql://prisma:prisma@localhost:5432/prisma"
```

### MySQL

```bash
# Using Docker
docker run -d --name mysql \
  -e MYSQL_ROOT_PASSWORD=prisma \
  -e MYSQL_DATABASE=prisma \
  -p 3306:3306 \
  mysql:8

export DATABASE_URL="mysql://root:prisma@localhost:3306/prisma"
```

### SQLite

```bash
# No setup needed -- SQLite is bundled
export DATABASE_URL="file:./dev.db"
```

## Running Tests

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p query-executor
cargo test -p driver-pg

# With database integration tests (requires running databases)
cargo test -p cross-compat
```
