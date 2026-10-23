//! Evaluation of MySQL named-lock calls inside a [`Session`].
//!
//! [`litewire_translate::user_locks`] finds the calls and rewrites the
//! statement around them; this module turns each call into a
//! [`UserLockOp`] — validating and normalizing the name the way MySQL does —
//! runs it through the session's connection
//! ([`BackendConn::user_lock`](litewire_backend::BackendConn::user_lock)),
//! and executes the completed statement.
//!
//! # Name rules (MySQL 8)
//!
//! * `NULL` or empty → error 3057 `ER_USER_LOCK_WRONG_NAME`.
//! * Longer than 64 characters → error 3938 `ER_USER_LOCK_OVERLONG_NAME`.
//! * Otherwise lowercased: lock names are case-insensitive, so `GET_LOCK('A')`
//!   and `IS_FREE_LOCK('a')` name the same lock.
//! * A numeric name is its decimal text (`GET_LOCK(42, 0)` locks `"42"`).
//!
//! # Timeout rules
//!
//! `NULL` makes `GET_LOCK` return `NULL` without touching the lock; a
//! negative value waits forever; `0` tries once; a fractional value is
//! rounded to whole seconds; non-numeric text counts as `0`.

use litewire_backend::Value;
use litewire_backend::user_lock::{LockTimeout, MAX_NAME_CHARS, UserLockOp};
use litewire_translate::StatementKind;
use litewire_translate::user_locks::{LockArg, LockCall, LockFunction, LockStatement, LockValue};

use crate::{Session, SessionError, SessionResult, error_map, map_backend_err};

/// Run a statement that [`litewire_translate::user_locks::detect`] matched:
/// evaluate every lock call in order, then execute the completed statement.
pub(crate) async fn run(
    session: &mut Session,
    plan: &LockStatement,
    params: &[Value],
) -> Result<SessionResult, SessionError> {
    let mut values = Vec::with_capacity(plan.calls.len());
    for call in &plan.calls {
        let value = match build_op(call, params)? {
            Some(op) => {
                let reply = session.conn.user_lock(&op).await.map_err(map_backend_err)?;
                session.note_user_lock(&op, reply);
                reply.map_or(LockValue::Null, LockValue::Int)
            }
            // A NULL timeout: MySQL answers NULL and does nothing.
            None => LockValue::Null,
        };
        values.push(value);
    }

    let sqlite = plan.to_sqlite(&values)?;
    let remaining = remaining_params(params, &plan.consumed_placeholders());

    if plan.is_do {
        // `DO`: evaluate and discard. The lock calls already ran; whatever is
        // left (`DO 1`, `DO NOW()`) runs for its side effects, if any.
        session
            .conn
            .query(&sqlite, &remaining)
            .await
            .map_err(map_backend_err)?;
        return Ok(SessionResult::Ok(session.ok(0, 0, false)));
    }
    session
        .run_translated(&sqlite, StatementKind::Query, &remaining)
        .await
}

/// The parameters to bind to the SQLite statement: `params` minus the ones
/// the lock calls consumed (`consumed` is ascending).
fn remaining_params(params: &[Value], consumed: &[usize]) -> Vec<Value> {
    let mut next = consumed.iter().peekable();
    params
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            if next.peek() == Some(&i) {
                next.next();
                false
            } else {
                true
            }
        })
        .map(|(_, v)| v.clone())
        .collect()
}

/// Build the operation for one call. `Ok(None)` means the call evaluates to
/// `NULL` without reaching the registry (a `NULL` timeout).
fn build_op(call: &LockCall, params: &[Value]) -> Result<Option<UserLockOp>, SessionError> {
    let arg = |i: usize| resolve_arg(&call.args[i], params, call.function);
    let op = match call.function {
        LockFunction::GetLock => {
            let name = lock_name(&arg(0)?)?;
            let Some(seconds) = timeout_seconds(&arg(1)?) else {
                return Ok(None);
            };
            UserLockOp::Get {
                name,
                timeout: LockTimeout::from_seconds(seconds),
            }
        }
        LockFunction::ReleaseLock => UserLockOp::Release {
            name: lock_name(&arg(0)?)?,
        },
        LockFunction::ReleaseAllLocks => UserLockOp::ReleaseAll,
        LockFunction::IsFreeLock => UserLockOp::IsFree {
            name: lock_name(&arg(0)?)?,
        },
        LockFunction::IsUsedLock => UserLockOp::IsUsed {
            name: lock_name(&arg(0)?)?,
        },
    };
    Ok(Some(op))
}

/// The concrete value of an argument: a literal as written, or the bound
/// parameter a placeholder refers to.
fn resolve_arg(
    arg: &LockArg,
    params: &[Value],
    function: LockFunction,
) -> Result<Value, SessionError> {
    Ok(match arg {
        LockArg::Null => Value::Null,
        LockArg::Text(s) => Value::Text(s.clone()),
        LockArg::Number(n) => n
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| n.parse::<f64>().map(Value::Float))
            .unwrap_or_else(|_| Value::Text(n.clone())),
        LockArg::Placeholder(i) => params.get(*i).cloned().ok_or_else(|| SessionError::Db {
            code: error_map::ER_WRONG_ARGUMENTS,
            sqlstate: *b"HY000",
            message: format!(
                "Incorrect arguments to {}: placeholder {} has no bound parameter",
                function.name(),
                i + 1
            ),
        })?,
    })
}

/// Validate and normalize a lock name per MySQL's `check_and_convert_ull_name`.
fn lock_name(value: &Value) -> Result<String, SessionError> {
    let wrong_name = |shown: &str| SessionError::Db {
        code: error_map::ER_USER_LOCK_WRONG_NAME,
        sqlstate: *b"42000",
        message: format!(
            "Incorrect user-level lock name '{shown}'. The name is empty, NULL, or can not be \
             expressed in the current character-set."
        ),
    };
    let name = match value {
        Value::Null => return Err(wrong_name("NULL")),
        Value::Text(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Blob(b) => match std::str::from_utf8(b) {
            Ok(s) => s.to_string(),
            Err(_) => return Err(wrong_name("<binary>")),
        },
    };
    if name.is_empty() {
        return Err(wrong_name(""));
    }
    if name.chars().count() > MAX_NAME_CHARS {
        return Err(SessionError::Db {
            code: error_map::ER_USER_LOCK_OVERLONG_NAME,
            sqlstate: *b"42000",
            message: format!(
                "User-level lock name '{name}' should not exceed {MAX_NAME_CHARS} characters."
            ),
        });
    }
    Ok(name.to_lowercase())
}

/// The whole seconds a `GET_LOCK` timeout argument means; `None` for `NULL`.
fn timeout_seconds(value: &Value) -> Option<i64> {
    Some(match value {
        Value::Null => return None,
        Value::Integer(i) => *i,
        Value::Float(f) => round_seconds(*f),
        Value::Text(s) => s.trim().parse::<f64>().map_or(0, round_seconds),
        Value::Blob(_) => 0,
    })
}

/// MySQL's `val_int()` on a non-integer: round half away from zero, saturate.
#[allow(
    clippy::cast_possible_truncation,
    reason = "`f64::round` yields an integral value; the `as` cast saturates at the i64 bounds"
)]
fn round_seconds(f: f64) -> i64 {
    if f.is_nan() { 0 } else { f.round() as i64 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_validated_and_lowercased() {
        assert_eq!(
            lock_name(&Value::Text("Sendy_Campaign".into())).unwrap(),
            "sendy_campaign"
        );
        assert_eq!(lock_name(&Value::Integer(42)).unwrap(), "42");
        assert_eq!(
            lock_name(&Value::Null).unwrap_err().code(),
            error_map::ER_USER_LOCK_WRONG_NAME
        );
        assert_eq!(
            lock_name(&Value::Text(String::new())).unwrap_err().code(),
            error_map::ER_USER_LOCK_WRONG_NAME
        );
        let sixty_four = "x".repeat(64);
        assert_eq!(lock_name(&Value::Text(sixty_four)).unwrap().len(), 64);
        let err = lock_name(&Value::Text("x".repeat(65))).unwrap_err();
        assert_eq!(err.code(), error_map::ER_USER_LOCK_OVERLONG_NAME);
        assert_eq!(err.sqlstate(), *b"42000");
        // Characters, not bytes.
        assert!(lock_name(&Value::Text("é".repeat(64))).is_ok());
    }

    #[test]
    fn timeouts() {
        assert_eq!(timeout_seconds(&Value::Null), None);
        assert_eq!(timeout_seconds(&Value::Integer(-1)), Some(-1));
        assert_eq!(timeout_seconds(&Value::Float(0.5)), Some(1));
        assert_eq!(timeout_seconds(&Value::Text(" 7 ".into())), Some(7));
        assert_eq!(timeout_seconds(&Value::Text("soon".into())), Some(0));
    }

    #[test]
    fn remaining_params_drops_consumed_ordinals() {
        let params = vec![
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ];
        assert_eq!(
            remaining_params(&params, &[1, 2]),
            vec![Value::Integer(0), Value::Integer(3)]
        );
        assert_eq!(remaining_params(&params, &[]), params);
    }
}
