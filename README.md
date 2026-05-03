# litewire

MySQL, PostgreSQL, SQL Server, and Hrana protocol proxy for SQLite. Connect your existing apps to SQLite without changing a line of code.

litewire accepts connections from MySQL, PostgreSQL, SQL Server, and libsql SDK clients, translates the SQL dialect on the fly, and executes against a SQLite backend. Your app thinks it's talking to a real database server -- it's actually talking to SQLite.

```
PHP/Rails/Django (pdo_mysql, pdo_pgsql, pdo_sqlsrv)
libsql SDK (Rust, JS, Python, Go)
        |
        v
   +---------+
   | litewire |  <-- MySQL :3306 / PG :5432 / TDS :1433 / Hrana :8080
   +----+----+
        |  SQL translation (MySQL/PG/T-SQL -> SQLite)
        |  or direct passthrough (Hrana -> SQLite)
        v
     SQLite
```

## Why

- **Zero-config development** -- no Docker, no database server, just SQLite
- **CI/CD** -- spin up a full stack with one process, tear it down when done
- **Edge deployments** -- single binary, no external dependencies
- **Drop-in replacement** -- existing MySQL/PG/SQL Server apps work without code changes

## Quick Start

```bash
# Start with a MySQL frontend
litewire --mysql-listen 127.0.0.1:3306 --db app.db

# Start with all frontends
litewire --mysql-listen 127.0.0.1:3306 --postgres-listen 127.0.0.1:5432 --tds-listen 127.0.0.1:1433 --hrana-listen 127.0.0.1:8080 --db app.db

# Connect from any MySQL client
mysql -h 127.0.0.1 -P 3306 -e "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
mysql -h 127.0.0.1 -P 3306 -e "INSERT INTO users (name) VALUES ('Alice')"
mysql -h 127.0.0.1 -P 3306 -e "SELECT * FROM users"

# Or PostgreSQL
psql -h 127.0.0.1 -p 5432 -c "SELECT * FROM users"

# Or SQL Server
sqlcmd -S 127.0.0.1,1433 -Q "SELECT * FROM users"

# Or via libsql SDK (Hrana protocol -- no SQL translation, native SQLite)
# Any libsql client SDK works: Rust, JavaScript, Python, Go
```

litewire also serves as a **lightweight drop-in replacement for sqld** (libsql-server). Apps using the Turso/libsql SDK can point at litewire instead of sqld for CI, development, and single-node deployments -- no replication server needed.

```bash
# CI/CD: replace sqld with litewire
litewire --hrana-listen 127.0.0.1:8080 --db test.db
```

## As a Library

litewire is also a Rust crate with a pluggable backend:

```toml
[dependencies]
litewire = { version = "0.1", features = ["mysql", "postgres", "tds", "hrana"] }
```

```rust
use litewire::{LiteWire, backend::Rusqlite};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let backend = Rusqlite::open("app.db")?;

    LiteWire::new(backend)
        .mysql("127.0.0.1:3306")
        .postgres("127.0.0.1:5432")
        .tds("127.0.0.1:1433")
        .hrana("127.0.0.1:8080")
        .serve()
        .await
}
```

### Pluggable Backends

| Backend | Feature flag | Use case |
|---------|-------------|----------|
| `Rusqlite` | `backend-rusqlite` | Direct in-process SQLite |
| `HranaClient` | `backend-hrana-client` | Remote SQLite via the Hrana HTTP protocol (sqld / Turso) |
| Custom | implement `Backend` trait | Bring your own |

The `HranaClient` backend connects to [sqld](https://github.com/tursodatabase/libsql) via HTTP, enabling embedded replicas and distributed SQLite clusters.

## SQL Translation

litewire translates MySQL and PostgreSQL SQL dialects to SQLite on the fly:

| MySQL / PostgreSQL / T-SQL | SQLite |
|---------------------------|--------|
| `AUTO_INCREMENT` / `SERIAL` / `IDENTITY(1,1)` | `INTEGER` (relies on SQLite's rowid alias when combined with `PRIMARY KEY`) |
| `NOW()` / `GETDATE()` | `datetime('now')` |
| `ON DUPLICATE KEY UPDATE` | `ON CONFLICT DO UPDATE` |
| `SHOW TABLES` / `sys.tables` | `SELECT name FROM sqlite_master WHERE type='table'` |
| `DESCRIBE table` / `sp_columns` | `PRAGMA table_info(table)` |
| `INFORMATION_SCHEMA.*` | `sqlite_master` + `PRAGMA` queries |
| `TRUE` / `FALSE` | `1` / `0` |
| `TOP n` | `LIMIT n` |
| `ISNULL(a, b)` | `IFNULL(a, b)` |
| `SET NAMES utf8mb4` / `SET NOCOUNT ON` | No-op |
| Backtick / `[bracket]` quoting | Passed through or converted |

See [docs/architecture.md](docs/architecture.md) for the full translation reference.

## Tested With

- WordPress (via `pdo_mysql`)
- Laravel (via `pdo_mysql` / `pdo_pgsql` / `pdo_sqlsrv`)
- Drupal
- `mysql` CLI
- `psql` CLI
- `sqlcmd` CLI
- DBeaver, pgAdmin, SSMS, TablePlus

## Limitations

- **Single-writer**: SQLite is single-writer. Concurrent writes are serialized.
- **No stored procedures**: SQLite doesn't support them.
- **No replication built-in**: Use sqld/libSQL for replication, litewire is the protocol layer only.
- **Translation coverage**: Not every MySQL/PG/T-SQL construct is translatable. Unsupported constructs return a clear error.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full design.

## License

MIT OR Apache-2.0
