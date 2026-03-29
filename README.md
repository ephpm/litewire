# litewire

MySQL, PostgreSQL, and SQL Server wire protocol proxy for SQLite. Connect your existing apps to SQLite without changing a line of code.

litewire accepts connections from MySQL, PostgreSQL, and SQL Server clients, translates the SQL dialect on the fly, and executes against a SQLite backend. Your app thinks it's talking to a real database server -- it's actually talking to SQLite.

```
PHP/Rails/Django (pdo_mysql, pdo_pgsql, pdo_sqlsrv, etc.)
        |
        v
   +---------+
   | litewire |  <-- MySQL :3306 / PostgreSQL :5432 / TDS :1433
   +----+----+
        |  SQL translation (MySQL/PG/T-SQL -> SQLite)
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

# Start with all three frontends
litewire --mysql-listen 127.0.0.1:3306 --pg-listen 127.0.0.1:5432 --tds-listen 127.0.0.1:1433 --db app.db

# Connect from any MySQL client
mysql -h 127.0.0.1 -P 3306 -e "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
mysql -h 127.0.0.1 -P 3306 -e "INSERT INTO users (name) VALUES ('Alice')"
mysql -h 127.0.0.1 -P 3306 -e "SELECT * FROM users"

# Or PostgreSQL
psql -h 127.0.0.1 -p 5432 -c "SELECT * FROM users"

# Or SQL Server
sqlcmd -S 127.0.0.1,1433 -Q "SELECT * FROM users"
```

## As a Library

litewire is also a Rust crate with a pluggable backend:

```toml
[dependencies]
litewire = { version = "0.1", features = ["mysql", "postgres", "tds"] }
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
        .serve()
        .await
}
```

### Pluggable Backends

| Backend | Feature flag | Use case |
|---------|-------------|----------|
| `rusqlite` | `backend-rusqlite` | Direct in-process SQLite |
| `libsql` | `backend-libsql` | libSQL (Turso's SQLite fork) via HTTP/Hrana |
| Custom | implement `Backend` trait | Bring your own |

The `libsql` backend connects to [sqld](https://github.com/tursodatabase/libsql) via HTTP, enabling embedded replicas and distributed SQLite clusters.

## SQL Translation

litewire translates MySQL and PostgreSQL SQL dialects to SQLite on the fly:

| MySQL / PostgreSQL / T-SQL | SQLite |
|---------------------------|--------|
| `AUTO_INCREMENT` / `SERIAL` / `IDENTITY(1,1)` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
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

See [docs/sql-translation.md](docs/sql-translation.md) for the full translation reference.

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
