//! Server-side verification for the `mysql_native_password` auth plugin.
//!
//! This is **protocol**, not policy: it answers "did the client prove it knows
//! the password behind this hash?", and nothing about which tenant owns which
//! password. That part stays with the embedder — see
//! [`ConnectionAuthenticator`](litewire_backend::ConnectionAuthenticator).
//!
//! It lives here because every implementor of that trait otherwise has to
//! hand-roll the same SHA-1 XOR dance, and getting it subtly wrong (comparing
//! the wrong stage, comparing non-constant-time, accepting a short response)
//! fails *open*.
//!
//! # The scheme
//!
//! MySQL never sends the password. The server stores
//! `stage2 = SHA1(SHA1(password))` — that is literally the value in
//! `mysql.user.authentication_string`, minus its leading `*`. Per connection
//! the server sends a random 20-byte challenge (`salt`), and the client replies
//! with
//!
//! ```text
//! auth_response = SHA1(password) XOR SHA1(salt || stage2)
//! ```
//!
//! The server can recompute `SHA1(salt || stage2)` from what it stores, XOR it
//! back out to recover the client's claimed `SHA1(password)`, hash that once
//! more, and compare against `stage2`.
//!
//! Two properties worth being explicit about, because they bound what this can
//! promise:
//!
//! * **The stored value is password-equivalent.** Anyone who reads `stage2` can
//!   answer any challenge without knowing the password. It is not a
//!   password-hashing function (no salt-per-user, no work factor) and must be
//!   treated as a secret at rest, not as a safe digest of a user's password.
//! * **The challenge must be fresh.** With a fixed challenge, `auth_response`
//!   becomes a constant that replays forever. litewire generates a random
//!   challenge per connection whenever an authenticator is installed.

use sha1::{Digest, Sha1};

/// Length of a SHA-1 digest, and so of every value in this module.
const SHA1_LEN: usize = 20;

/// Compute the value a server stores for `password`: `SHA1(SHA1(password))`.
///
/// This is the input to [`verify`]. Derive it once (at startup, or when a
/// tenant's credential is minted) rather than per connection.
#[must_use]
pub fn password_hash(password: &[u8]) -> [u8; SHA1_LEN] {
    let stage1 = Sha1::digest(password);
    Sha1::digest(stage1).into()
}

/// Check a client's `auth_response` against the stored [`password_hash`].
///
/// `salt` is the challenge this connection's handshake used — pass
/// [`AuthRequest::salt`](litewire_backend::AuthRequest::salt) straight through;
/// using any other value silently rejects every client.
///
/// Returns `false` for an empty or wrong-length `auth_response`, which is what
/// a client that sent no password at all produces.
///
/// The final comparison is constant-time. The earlier length check is not, and
/// does not need to be: the length of a `mysql_native_password` response is
/// fixed by the protocol and reveals nothing about the secret.
#[must_use]
pub fn verify(stored_hash: &[u8; SHA1_LEN], salt: &[u8], auth_response: &[u8]) -> bool {
    // A client with no password sends an empty response. Anything that is not
    // exactly one digest long is not a well-formed response.
    if auth_response.len() != SHA1_LEN {
        return false;
    }

    // mask = SHA1(salt || stage2)
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(stored_hash);
    let mask = hasher.finalize();

    // Recover the client's claimed SHA1(password), then hash it once more; a
    // client that knows the password lands back on `stored_hash`.
    let mut stage1 = [0u8; SHA1_LEN];
    for (i, out) in stage1.iter_mut().enumerate() {
        *out = auth_response[i] ^ mask[i];
    }
    let candidate: [u8; SHA1_LEN] = Sha1::digest(stage1).into();

    constant_time_eq(&candidate, stored_hash)
}

/// Compare two digests without an early exit.
///
/// A `==` here would return as soon as bytes differ, leaking through timing
/// how much of a guess was correct and turning verification into a
/// byte-at-a-time oracle.
fn constant_time_eq(a: &[u8; SHA1_LEN], b: &[u8; SHA1_LEN]) -> bool {
    let mut diff = 0u8;
    for i in 0..SHA1_LEN {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reproduce what a client computes, so the tests drive the real scheme
    /// rather than a mirror of `verify`'s own arithmetic.
    fn client_response(password: &[u8], salt: &[u8]) -> Vec<u8> {
        let stage1: [u8; SHA1_LEN] = Sha1::digest(password).into();
        let stage2: [u8; SHA1_LEN] = Sha1::digest(stage1).into();
        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(stage2);
        let mask = hasher.finalize();
        stage1.iter().zip(mask).map(|(s, m)| s ^ m).collect()
    }

    const SALT: &[u8] = b"12345678901234567890";

    #[test]
    fn correct_password_verifies() {
        let stored = password_hash(b"hunter2");
        assert!(verify(&stored, SALT, &client_response(b"hunter2", SALT)));
    }

    #[test]
    fn wrong_password_is_rejected() {
        let stored = password_hash(b"hunter2");
        assert!(!verify(&stored, SALT, &client_response(b"hunter3", SALT)));
    }

    /// The cross-tenant case: a response computed for *another* account's
    /// password never verifies against this account's stored hash, however the
    /// client labels itself.
    #[test]
    fn another_accounts_response_is_rejected() {
        let site_a = password_hash(b"secret-for-a");
        let response_from_b = client_response(b"secret-for-b", SALT);
        assert!(!verify(&site_a, SALT, &response_from_b));
    }

    /// A response captured under one challenge is worthless under another —
    /// this is what makes the per-connection random scramble load-bearing.
    #[test]
    fn response_does_not_replay_across_salts() {
        let stored = password_hash(b"hunter2");
        let captured = client_response(b"hunter2", SALT);
        let other_salt = b"09876543210987654321";
        assert!(
            verify(&stored, SALT, &captured),
            "sanity: valid on its own salt"
        );
        assert!(!verify(&stored, other_salt, &captured));
    }

    #[test]
    fn empty_response_is_rejected() {
        let stored = password_hash(b"hunter2");
        assert!(!verify(&stored, SALT, b""));
    }

    #[test]
    fn short_and_long_responses_are_rejected() {
        let stored = password_hash(b"hunter2");
        assert!(!verify(&stored, SALT, &[0u8; 19]));
        assert!(!verify(&stored, SALT, &[0u8; 21]));
    }

    /// `password_hash` really is `SHA1(SHA1(pw))` — i.e. the value MySQL
    /// stores as `authentication_string` (minus its leading `*`), so an
    /// operator can move a credential between litewire and MySQL in either
    /// direction.
    ///
    /// The expected value is pinned against an independent implementation
    /// rather than against this module's own output, so a bug in `sha1`'s
    /// wiring here cannot make the test agree with itself:
    ///
    /// ```sh
    /// printf 'hunter2' | openssl dgst -sha1 -binary | openssl dgst -sha1 -r
    /// ```
    #[test]
    fn password_hash_matches_mysql_authentication_string() {
        let expected = "58815970BE77B3720276F63DB198B1FA42E5CC02";
        let got = password_hash(b"hunter2")
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<String>();
        assert_eq!(got, expected);
    }
}
