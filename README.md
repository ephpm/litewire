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

# Start with all frontends (postgres + tds require --features postgres,tds at build time)
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
| `Turso` | `turso` | **Experimental** — [Turso Database](https://github.com/tursodatabase/turso) engine (Rust rewrite of SQLite, Beta upstream; pinned `=0.7.0`). See `crates/litewire-turso` docs for limitations (no `VACUUM`, no multi-process access) |
| Custom | implement `Backend` trait | Bring your own |

The `HranaClient` backend connects to [sqld](https://github.com/tursodatabase/libsql) via HTTP, enabling embedded replicas and distributed SQLite clusters.

### Multi-Tenant: One Listener, Many Databases

SQLite has no `GRANT` and no per-schema ACL, so the database *file* is the only
tenant boundary. `LiteWire::with_authenticator` lets one MySQL listener sit in
front of many of them: the backend is chosen per connection, during the
handshake, and a connection that is not accepted gets **no** backend at all.

```rust
use std::sync::Arc;
use litewire::backend::{AuthRequest, ConnectionAuthenticator, SharedBackend};
use litewire::litewire_mysql::native_password;

struct Tenants { /* username -> (SHA1(SHA1(password)), backend) */ }

impl ConnectionAuthenticator for Tenants {
    fn authenticate(&self, req: &AuthRequest<'_>) -> Option<SharedBackend> {
        let tenant = self.lookup(req.username)?;
        // The username only selects a *candidate*; the password is what
        // entitles the client to it.
        native_password::verify(&tenant.password_hash, req.salt, req.auth_response)
            .then(|| tenant.backend.clone())
    }
}

LiteWire::with_authenticator(Arc::new(tenants))
    .mysql("127.0.0.1:3306")
    .serve()
    .await
```

**The username in a handshake is a claim, not an identity** — a hostile client
will happily type its neighbour's. Selecting a backend from it alone is not
isolation. Bind the choice to something the client cannot forge: either a
per-tenant secret (as above), or `req.local_addr` when tenants are separated by
OS credentials and genuinely cannot reach each other's listener. See the
`litewire_backend::auth` module docs for the full contract.

MySQL is the only frontend that can resolve a backend per connection;
`serve()` refuses to start if Hrana, PostgreSQL, or TDS is also configured
under an authenticator, rather than quietly serving them one shared database.

**Tenant sessions cannot reach past their own database file.** Every tenant's
database is opened by the same process under the same uid, so nothing at the
filesystem level stops a session from `ATTACH`ing its neighbour's file — which
would make the authenticator's boundary a suggestion. litewire therefore
screens every authenticator-established session at the backend boundary:
`ATTACH`, `DETACH`, `VACUUM` with a target (`VACUUM INTO '<path>'`), and the
path-bearing or schema-reopening `PRAGMA`s (`data_store_directory`,
`temp_store_directory`, `writable_schema`) are refused with a clean SQL error,
on every backend, regardless of what the engine underneath would do with them.
The screen sees through `EXPLAIN` wrappers, schema-qualified and quoted
`PRAGMA` spellings, and statements hidden behind a `;`. Everything else —
including bare `VACUUM` and ordinary tuning pragmas — passes through
untouched.

Single-tenant deployments (a fixed backend, no authenticator) are deliberately
**not** screened: `ATTACH` is legitimate and useful in a single-user embedded
setup. The screen keys off the session being tenant-scoped, never off the
statement alone. See `litewire_backend::tenant_screen` for the full contract.

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

See [docs/architecture.md](docs/architecture.md) for the full architecture and translation reference.

## Compatibility

The MySQL frontend is exercised end-to-end by an in-process test suite
(`crates/litewire/tests/mysql_e2e.rs`) that drives the wire protocol via
`mysql_async` -- CRUD, prepared statements, transactions (`START TRANSACTION` /
`BEGIN` / `COMMIT` / `ROLLBACK`), `LAST_INSERT_ID()`, `SHOW TABLES`,
`DESCRIBE`, `INFORMATION_SCHEMA` probes, `SET NAMES` / `SET autocommit`, and
the metadata queries used at connection setup.

The PostgreSQL and TDS frontends are wire-compatible enough for basic CRUD
against `psql` / `sqlcmd` and the extended-query flow used by `pdo_pgsql` /
`pdo_sqlsrv`; the TDS frontend is **experimental** -- authentication is
simplified, the type coverage is a subset (BigInt / Float8 / NVARCHAR /
VarBinary), and the SSL handshake is not implemented. Real SQL Server tools
(SSMS, sqlcmd with encryption) will not connect until those land.

Anywhere you would normally point at MySQL/PG/SQL Server -- PHP PDO drivers,
`mysql` / `psql` / `sqlcmd` CLIs, DBeaver, pgAdmin -- should work for standard
CRUD workloads. `SQL_CALC_FOUND_ROWS` + `SELECT FOUND_ROWS()` (WordPress
pagination) is emulated at the session layer. Anything that depends on
server-side features SQLite doesn't have (stored procedures, `LOCK TABLES`
isolation, row-level locking semantics, dollar-quoted PL/pgSQL bodies, etc.)
will not.

## Limitations

- **Single-writer**: SQLite is single-writer. Concurrent writes are serialized.
- **No stored procedures**: SQLite doesn't support them.
- **No replication built-in**: Use sqld/libSQL for replication, litewire is the protocol layer only.
- **Translation coverage**: Not every MySQL/PG/T-SQL construct is translatable. Unsupported constructs return a clear error.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full design.

## License

MIT
