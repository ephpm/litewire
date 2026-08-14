//! A MySQL command-packet filter that sits in front of `opensrv-mysql`.
//!
//! # Why this exists
//!
//! `opensrv-mysql` parses exactly nine client commands (`COM_QUERY`,
//! `COM_FIELD_LIST`, `COM_INIT_DB`, `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`,
//! `COM_STMT_SEND_LONG_DATA`, `COM_STMT_CLOSE`, `COM_QUIT`, `COM_PING`).
//! Every other command byte falls through to a single catch-all that answers
//! with a default `OK` packet and logs nothing:
//!
//! ```text
//! Err(_) => {
//!     // if parser err, we need also stay the conn,
//!     // because we can not support all command.
//!     writers::write_ok_packet(..., OkResponse::default()).await?;
//! }
//! ```
//!
//! An `OK` is the *correct* reply to some of those commands and flatly wrong
//! for others — `COM_SET_OPTION` is answered with `EOF`, `COM_STATISTICS`
//! with a bare status string, `COM_STMT_FETCH` with a row stream. A client
//! that gets an `OK` where it expects a differently-shaped reply reads the
//! wrong number of packets, and from that point on every response it sees
//! belongs to the previous command. That is the failure reported in
//! [issue #19](https://github.com/ephpm/litewire/issues/19): PHP persistent
//! connections dying with `Wrong COM_STMT_PREPARE response size. Received 7`
//! (7 bytes is exactly a `PROTOCOL_41` `OK` payload) followed by `Packets out
//! of order`, with no server-side log line to explain it.
//!
//! # What this filter does
//!
//! It sits between the socket and `opensrv-mysql`'s reader and inspects the
//! first byte of every *command* packet:
//!
//! * Commands `opensrv-mysql` handles are forwarded byte-for-byte.
//! * `COM_STMT_RESET`, `COM_RESET_CONNECTION` and `COM_CHANGE_USER` are
//!   rewritten into a `COM_PING` packet. All three are answered by real MySQL
//!   with a plain `OK`, and `COM_PING` is `opensrv-mysql`'s own `OK` path — so
//!   the reply is byte-identical to what the negotiated capability flags call
//!   for, without this module having to re-derive the `OK` encoding.
//! * Every other command byte is answered here with
//!   `ER_UNKNOWN_COM_ERROR (1047 / 08S01)` — what real MySQL sends for a
//!   command it does not implement — and logged at `warn` with the command
//!   byte. The packet never reaches `opensrv-mysql`, so the stray `OK` cannot
//!   happen.
//!
//! # Telling commands from handshake packets
//!
//! The filter must not treat the client's handshake response as a command:
//! its first payload byte is the low byte of the capability flags and can be
//! any value at all. The discriminator is the packet sequence id, which the
//! MySQL protocol resets to 0 at the start of every command and increments
//! within an exchange. A command packet therefore always carries sequence 0,
//! and the handshake response (sequence 1) and auth-switch response
//! (sequence 3) never do. Packets with a non-zero sequence are forwarded
//! untouched, which also covers the continuation packets of a payload larger
//! than 16 MiB.
//!
//! # Session state
//!
//! `COM_RESET_CONNECTION` and `COM_CHANGE_USER` are defined to reset session
//! state. The filter records that a reset is due in a shared flag; the
//! handler drains the flag before its next statement and rolls back an open
//! transaction and drops its prepared statements. It is applied lazily rather
//! than inline because the rewritten `COM_PING` is answered inside
//! `opensrv-mysql` and never reaches the shim.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::{debug, warn};

// ── Command bytes ────────────────────────────────────────────────────────────
// <https://dev.mysql.com/doc/dev/mysql-server/latest/my__command_8h.html>

/// `COM_QUIT` — client is closing the connection.
const COM_QUIT: u8 = 0x01;
/// `COM_INIT_DB` — `USE <db>`.
const COM_INIT_DB: u8 = 0x02;
/// `COM_QUERY` — text-protocol statement.
const COM_QUERY: u8 = 0x03;
/// `COM_FIELD_LIST` — deprecated column listing.
const COM_FIELD_LIST: u8 = 0x04;
/// `COM_PING` — liveness check; answered with `OK`.
const COM_PING: u8 = 0x0e;
/// `COM_CHANGE_USER` — re-authenticate and reset the session.
const COM_CHANGE_USER: u8 = 0x11;
/// `COM_STMT_PREPARE` — prepare a statement.
const COM_STMT_PREPARE: u8 = 0x16;
/// `COM_STMT_EXECUTE` — execute a prepared statement.
const COM_STMT_EXECUTE: u8 = 0x17;
/// `COM_STMT_SEND_LONG_DATA` — stream one parameter; no reply.
const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
/// `COM_STMT_CLOSE` — deallocate a prepared statement; no reply.
const COM_STMT_CLOSE: u8 = 0x19;
/// `COM_STMT_RESET` — drop a prepared statement's accumulated parameter
/// data and cursor. Answered with `OK`.
const COM_STMT_RESET: u8 = 0x1a;
/// `COM_RESET_CONNECTION` — reset session state without re-authenticating.
/// Answered with `OK`.
const COM_RESET_CONNECTION: u8 = 0x1f;

/// MySQL `ER_UNKNOWN_COM_ERROR` — "Unknown command", SQLSTATE `08S01`.
///
/// What a real server answers with when a client sends a command byte it
/// does not implement.
const ER_UNKNOWN_COM_ERROR: u16 = 1047;

/// SQLSTATE that accompanies [`ER_UNKNOWN_COM_ERROR`].
const UNKNOWN_COM_SQLSTATE: &[u8; 5] = b"08S01";

/// Size of a MySQL packet header: 3-byte payload length + 1-byte sequence.
const HEADER_LEN: usize = 4;

/// Header plus the one payload byte that identifies the command.
const HEADER_AND_COMMAND: usize = HEADER_LEN + 1;

/// True for commands `opensrv-mysql` parses and handles itself.
///
/// These are forwarded untouched; the filter exists only for what is *not*
/// on this list.
const fn is_handled_upstream(command: u8) -> bool {
    matches!(
        command,
        COM_QUIT
            | COM_INIT_DB
            | COM_QUERY
            | COM_FIELD_LIST
            | COM_PING
            | COM_STMT_PREPARE
            | COM_STMT_EXECUTE
            | COM_STMT_SEND_LONG_DATA
            | COM_STMT_CLOSE
    )
}

/// Build a `COM_PING` packet carrying `seq`.
///
/// Used as the replacement for the housekeeping commands whose reply is a
/// plain `OK`; `opensrv-mysql` writes that `OK` with the connection's
/// negotiated capability flags, which this module does not have.
const fn ping_packet(seq: u8) -> [u8; HEADER_AND_COMMAND] {
    [0x01, 0x00, 0x00, seq, COM_PING]
}

/// Build a `PROTOCOL_41` ERR packet.
///
/// `opensrv-mysql` refuses any client that does not negotiate
/// `CLIENT_PROTOCOL_41`, so the `#SQLSTATE` marker is always present and this
/// encoding does not depend on the capability flags.
fn err_packet(seq: u8, code: u16, sqlstate: &[u8; 5], message: &str) -> Vec<u8> {
    let payload_len = 1 + 2 + 1 + sqlstate.len() + message.len();
    let mut packet = Vec::with_capacity(HEADER_LEN + payload_len);
    // The messages built here are a fixed prefix plus one hex byte, orders of
    // magnitude below the 16 MiB a 3-byte length field can express.
    debug_assert!(payload_len < 0x00ff_ffff);
    #[allow(clippy::cast_possible_truncation)]
    {
        packet.push((payload_len & 0xff) as u8);
        packet.push(((payload_len >> 8) & 0xff) as u8);
        packet.push(((payload_len >> 16) & 0xff) as u8);
    }
    packet.push(seq);
    packet.push(0xff); // ERR packet marker
    packet.extend_from_slice(&code.to_le_bytes());
    packet.push(b'#');
    packet.extend_from_slice(sqlstate);
    packet.extend_from_slice(message.as_bytes());
    packet
}

/// What to do once an intercepted packet has been fully consumed.
#[derive(Clone, Debug)]
enum Action {
    /// Hand `opensrv-mysql` this replacement command packet.
    Substitute([u8; HEADER_AND_COMMAND]),
    /// Answer the client directly with this packet.
    Reply(Vec<u8>),
}

/// Where the filter is in the packet it is currently working on.
#[derive(Debug)]
enum State {
    /// Collecting the next packet's header, then its command byte.
    Head {
        /// Header bytes received so far, followed by the command byte.
        buf: [u8; HEADER_AND_COMMAND],
        /// How much of `buf` is filled.
        have: usize,
    },
    /// Handing `buf[pos..len]` to the reader, then `body` more bytes verbatim.
    Emit {
        /// The (possibly rewritten) header + command byte.
        buf: [u8; HEADER_AND_COMMAND],
        /// How much of `buf` has been handed over.
        pos: usize,
        /// How much of `buf` is meaningful.
        len: usize,
        /// Payload bytes still to forward verbatim after `buf`.
        body: usize,
    },
    /// Forwarding `remaining` payload bytes verbatim.
    Pass {
        /// Bytes still to forward.
        remaining: usize,
    },
    /// Reading and discarding `remaining` bytes of an intercepted packet.
    Drain {
        /// Bytes still to discard.
        remaining: usize,
        /// What to do once they are gone.
        then: Action,
    },
    /// Writing a synthesized reply straight to the client.
    Write {
        /// The complete packet.
        packet: Vec<u8>,
        /// How much of it has reached the socket.
        pos: usize,
    },
    /// The client closed the connection.
    Eof,
}

impl State {
    /// The starting state for a new packet.
    const fn head() -> Self {
        Self::Head {
            buf: [0; HEADER_AND_COMMAND],
            have: 0,
        }
    }
}

/// Filters MySQL command packets on their way to `opensrv-mysql`.
///
/// Implements [`AsyncRead`] over the connection's read half, so it drops into
/// the place the raw socket used to occupy. `writer` must be an independent
/// handle to the *same* socket: the filter uses it to answer the commands it
/// intercepts. Interleaving is safe because MySQL is strictly
/// request/response — `opensrv-mysql` flushes its writer after every command
/// and is then blocked in `poll_read`, so nothing of its is in flight at the
/// moment the filter writes.
///
/// See the module docs for the full rationale.
pub(crate) struct CommandFilter<R, W> {
    /// The connection's read half.
    reader: R,
    /// An independent write handle to the same connection.
    writer: W,
    /// Set when the client asked for a session reset; drained by the handler.
    reset_pending: Arc<AtomicBool>,
    /// Whether `COM_CHANGE_USER` may be accepted as a no-op.
    ///
    /// True on the single-backend path, where the "new" user reaches exactly
    /// the same database as the old one and accepting is harmless. False on
    /// the authenticating path: there, saying `OK` would tell the client it
    /// is now somebody else while the connection keeps talking to the
    /// original tenant's backend, so the command is refused instead.
    allow_change_user: bool,
    /// Progress through the packet currently being filtered.
    state: State,
}

impl<R, W> CommandFilter<R, W> {
    /// Wrap `reader`, answering intercepted commands on `writer`.
    pub(crate) fn new(
        reader: R,
        writer: W,
        reset_pending: Arc<AtomicBool>,
        allow_change_user: bool,
    ) -> Self {
        Self {
            reader,
            writer,
            reset_pending,
            allow_change_user,
            state: State::head(),
        }
    }

    /// Decide what to do with a command packet.
    ///
    /// `seq` is the packet's sequence id and `body` the payload bytes that
    /// follow the command byte.
    fn classify(&mut self, header: [u8; HEADER_AND_COMMAND], body: usize) -> State {
        let command = header[HEADER_LEN];
        let seq = header[3];

        if is_handled_upstream(command) {
            return State::Emit {
                buf: header,
                pos: 0,
                len: HEADER_AND_COMMAND,
                body,
            };
        }

        let no_op = match command {
            COM_STMT_RESET => true,
            COM_RESET_CONNECTION => {
                self.reset_pending.store(true, Ordering::Relaxed);
                true
            }
            COM_CHANGE_USER if self.allow_change_user => {
                self.reset_pending.store(true, Ordering::Relaxed);
                true
            }
            _ => false,
        };

        if no_op {
            let name = format!("0x{command:02x}");
            debug!(command = %name, "MySQL housekeeping command answered OK");
            return State::Drain {
                remaining: body,
                then: Action::Substitute(ping_packet(seq)),
            };
        }

        // Everything else: say so, out loud, and answer the way a real server
        // would. Silently OK-ing this is what desynchronises the connection.
        let name = format!("0x{command:02x}");
        warn!(
            command = %name,
            "unsupported MySQL command; replying ER_UNKNOWN_COM_ERROR (1047)"
        );
        let message = format!("Unknown command {name}");
        State::Drain {
            remaining: body,
            then: Action::Reply(err_packet(
                seq.wrapping_add(1),
                ER_UNKNOWN_COM_ERROR,
                UNKNOWN_COM_SQLSTATE,
                &message,
            )),
        }
    }
}

impl<R, W> AsyncRead for CommandFilter<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    #[allow(clippy::too_many_lines)]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                // A filled length of zero is how `AsyncRead` spells EOF.
                State::Eof => return Poll::Ready(Ok(())),

                State::Write { packet, pos } => {
                    while *pos < packet.len() {
                        let n = ready!(Pin::new(&mut this.writer).poll_write(cx, &packet[*pos..]))?;
                        if n == 0 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "MySQL client stopped accepting bytes mid-reply",
                            )));
                        }
                        *pos += n;
                    }
                    ready!(Pin::new(&mut this.writer).poll_flush(cx))?;
                    this.state = State::head();
                }

                State::Head { buf, have } => {
                    // Read the 4-byte header first; only once the payload is
                    // known to be non-empty is there a command byte to read.
                    let need = if *have < HEADER_LEN {
                        HEADER_LEN
                    } else {
                        HEADER_AND_COMMAND
                    };
                    let mut scratch = [0u8; HEADER_AND_COMMAND];
                    let mut got = ReadBuf::new(&mut scratch[..need - *have]);
                    ready!(Pin::new(&mut this.reader).poll_read(cx, &mut got))?;
                    let n = got.filled().len();
                    if n == 0 {
                        this.state = State::Eof;
                        return Poll::Ready(Ok(()));
                    }
                    buf[*have..*have + n].copy_from_slice(&scratch[..n]);
                    *have += n;

                    let header = *buf;
                    let have = *have;
                    if have < HEADER_LEN {
                        continue;
                    }
                    let payload_len =
                        u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;

                    if payload_len == 0 {
                        // The empty packet that terminates a payload of
                        // exactly 16 MiB. Nothing to classify.
                        this.state = State::Emit {
                            buf: header,
                            pos: 0,
                            len: HEADER_LEN,
                            body: 0,
                        };
                        continue;
                    }
                    if have < HEADER_AND_COMMAND {
                        continue;
                    }

                    let body = payload_len - 1;
                    // Only sequence 0 starts a command. Anything else belongs
                    // to an exchange already under way (the handshake
                    // response, an auth-switch response, or the continuation
                    // of a >16 MiB payload) and is not ours to interpret.
                    this.state = if header[3] == 0 {
                        this.classify(header, body)
                    } else {
                        State::Emit {
                            buf: header,
                            pos: 0,
                            len: HEADER_AND_COMMAND,
                            body,
                        }
                    };
                }

                State::Emit {
                    buf,
                    pos,
                    len,
                    body,
                } => {
                    if out.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let n = (*len - *pos).min(out.remaining());
                    out.put_slice(&buf[*pos..*pos + n]);
                    *pos += n;
                    if *pos == *len {
                        let body = *body;
                        this.state = if body == 0 {
                            State::head()
                        } else {
                            State::Pass { remaining: body }
                        };
                    }
                    return Poll::Ready(Ok(()));
                }

                State::Pass { remaining } => {
                    let cap = (*remaining).min(out.remaining());
                    if cap == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    // `take` hands the inner reader a view over the same
                    // memory but its own bookkeeping, so `out` never learns
                    // that the bytes became initialized. Initializing them
                    // up front is what makes the later `advance` legal
                    // without `unsafe`; it is a no-op for the callers that
                    // matter, which pass an already-initialized buffer.
                    out.initialize_unfilled_to(cap);
                    let n = {
                        let mut sub = out.take(cap);
                        ready!(Pin::new(&mut this.reader).poll_read(cx, &mut sub))?;
                        sub.filled().len()
                    };
                    if n == 0 {
                        this.state = State::Eof;
                        return Poll::Ready(Ok(()));
                    }
                    out.advance(n);
                    *remaining -= n;
                    if *remaining == 0 {
                        this.state = State::head();
                    }
                    return Poll::Ready(Ok(()));
                }

                State::Drain { remaining, then } => {
                    if *remaining > 0 {
                        // Intercepted commands carry tiny payloads
                        // (`COM_STMT_RESET` four bytes, `COM_CHANGE_USER` a
                        // few dozen), so a small scratch buffer is enough.
                        let mut scratch = [0u8; 256];
                        let take = (*remaining).min(scratch.len());
                        let mut got = ReadBuf::new(&mut scratch[..take]);
                        ready!(Pin::new(&mut this.reader).poll_read(cx, &mut got))?;
                        let n = got.filled().len();
                        if n == 0 {
                            this.state = State::Eof;
                            return Poll::Ready(Ok(()));
                        }
                        *remaining -= n;
                        continue;
                    }
                    this.state = match then.clone() {
                        Action::Substitute(buf) => State::Emit {
                            buf,
                            pos: 0,
                            len: HEADER_AND_COMMAND,
                            body: 0,
                        },
                        Action::Reply(packet) => State::Write { packet, pos: 0 },
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// Build a client command packet: header + command byte + payload.
    #[allow(clippy::cast_possible_truncation)]
    fn packet(seq: u8, command: u8, payload: &[u8]) -> Vec<u8> {
        let len = 1 + payload.len();
        let mut p = vec![
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
            seq,
            command,
        ];
        p.extend_from_slice(payload);
        p
    }

    /// Feed `input` through a filter and collect what `opensrv-mysql` would
    /// see and what the client would be sent.
    async fn run(input: &[u8], allow_change_user: bool) -> (Vec<u8>, Vec<u8>, bool) {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut client_read, mut client_write) = tokio::io::split(client);

        // Feed the filter from a task: an input larger than the duplex
        // buffer would otherwise block before anything started reading.
        let data = input.to_vec();
        let feeder = tokio::spawn(async move {
            client_write.write_all(&data).await.unwrap();
            client_write.shutdown().await.unwrap();
        });

        let (server_read, server_write) = tokio::io::split(server);
        let reset = Arc::new(AtomicBool::new(false));
        let mut filter = CommandFilter::new(
            server_read,
            server_write,
            Arc::clone(&reset),
            allow_change_user,
        );

        let mut forwarded = Vec::new();
        filter.read_to_end(&mut forwarded).await.unwrap();
        // Closes the filter's end of the duplex, so the read below sees EOF.
        drop(filter);
        feeder.await.unwrap();

        let mut replied = Vec::new();
        client_read.read_to_end(&mut replied).await.unwrap();

        (forwarded, replied, reset.load(Ordering::Relaxed))
    }

    #[tokio::test]
    async fn query_is_forwarded_verbatim() {
        let input = packet(0, COM_QUERY, b"SELECT 1");
        let (forwarded, replied, reset) = run(&input, true).await;
        assert_eq!(forwarded, input);
        assert!(replied.is_empty(), "no reply should be synthesized");
        assert!(!reset);
    }

    #[tokio::test]
    async fn several_packets_are_forwarded_in_order() {
        let mut input = packet(0, COM_QUERY, b"SELECT 1");
        input.extend(packet(0, COM_STMT_PREPARE, b"SELECT ?"));
        input.extend(packet(0, COM_STMT_CLOSE, &[1, 0, 0, 0]));
        let (forwarded, replied, _) = run(&input, true).await;
        assert_eq!(forwarded, input);
        assert!(replied.is_empty());
    }

    #[tokio::test]
    async fn stmt_reset_becomes_a_ping() {
        let input = packet(0, COM_STMT_RESET, &[7, 0, 0, 0]);
        let (forwarded, replied, reset) = run(&input, true).await;
        assert_eq!(forwarded, ping_packet(0).to_vec());
        assert!(replied.is_empty());
        assert!(
            !reset,
            "COM_STMT_RESET resets one statement, not the session"
        );
    }

    #[tokio::test]
    async fn reset_connection_becomes_a_ping_and_flags_a_session_reset() {
        let input = packet(0, COM_RESET_CONNECTION, &[]);
        let (forwarded, replied, reset) = run(&input, true).await;
        assert_eq!(forwarded, ping_packet(0).to_vec());
        assert!(replied.is_empty());
        assert!(reset);
    }

    #[tokio::test]
    async fn change_user_is_a_ping_on_the_single_backend_path() {
        let input = packet(0, COM_CHANGE_USER, b"root\0\0test\0");
        let (forwarded, replied, reset) = run(&input, true).await;
        assert_eq!(forwarded, ping_packet(0).to_vec());
        assert!(replied.is_empty());
        assert!(reset);
    }

    #[tokio::test]
    async fn change_user_is_refused_on_the_authenticating_path() {
        let input = packet(0, COM_CHANGE_USER, b"root\0\0test\0");
        let (forwarded, replied, reset) = run(&input, false).await;
        assert!(
            forwarded.is_empty(),
            "the command must not reach opensrv-mysql"
        );
        assert_eq!(replied[4], 0xff, "an ERR packet");
        assert_eq!(u16::from_le_bytes([replied[5], replied[6]]), 1047);
        assert!(!reset, "a refused change-user must not reset the session");
    }

    #[tokio::test]
    async fn unknown_command_gets_an_err_not_an_ok() {
        // COM_STATISTICS (0x09): `opensrv-mysql` would answer this with a
        // 7-byte OK packet, which is not the reply shape the client expects.
        let input = packet(0, 0x09, &[]);
        let (forwarded, replied, _) = run(&input, true).await;
        assert!(
            forwarded.is_empty(),
            "the command must not reach opensrv-mysql"
        );

        let payload_len = u32::from_le_bytes([replied[0], replied[1], replied[2], 0]) as usize;
        assert_eq!(replied.len(), 4 + payload_len);
        assert_eq!(replied[3], 1, "reply carries the command's sequence + 1");
        assert_eq!(replied[4], 0xff, "an ERR packet");
        assert_eq!(u16::from_le_bytes([replied[5], replied[6]]), 1047);
        assert_eq!(&replied[7..13], b"#08S01");
        assert_eq!(&replied[13..], b"Unknown command 0x09");
    }

    #[tokio::test]
    async fn a_command_after_an_unknown_one_still_works() {
        let mut input = packet(0, 0x1b, &[1]); // COM_SET_OPTION
        input.extend(packet(0, COM_QUERY, b"SELECT 1"));
        let (forwarded, replied, _) = run(&input, true).await;
        assert_eq!(forwarded, packet(0, COM_QUERY, b"SELECT 1"));
        assert_eq!(replied[4], 0xff);
    }

    #[tokio::test]
    async fn non_zero_sequence_is_never_treated_as_a_command() {
        // A handshake response whose first byte happens to equal
        // COM_RESET_CONNECTION must be forwarded, not intercepted.
        let input = packet(1, COM_RESET_CONNECTION, b"rest of the handshake");
        let (forwarded, replied, reset) = run(&input, true).await;
        assert_eq!(forwarded, input);
        assert!(replied.is_empty());
        assert!(!reset);
    }

    #[tokio::test]
    async fn an_empty_packet_is_forwarded() {
        let input = vec![0, 0, 0, 3];
        let (forwarded, replied, _) = run(&input, true).await;
        assert_eq!(forwarded, input);
        assert!(replied.is_empty());
    }

    #[tokio::test]
    async fn a_large_payload_survives_being_split_across_reads() {
        let big = vec![b'x'; 200_000];
        let input = packet(0, COM_QUERY, &big);
        let (forwarded, replied, _) = run(&input, true).await;
        assert_eq!(forwarded, input);
        assert!(replied.is_empty());
    }

    #[tokio::test]
    async fn a_header_split_across_reads_is_reassembled() {
        let input = packet(0, COM_STMT_RESET, &[7, 0, 0, 0]);
        let (mut client, server) = tokio::io::duplex(64);
        let (server_read, server_write) = tokio::io::split(server);
        let reset = Arc::new(AtomicBool::new(false));
        let mut filter = CommandFilter::new(server_read, server_write, Arc::clone(&reset), true);

        let writer = tokio::spawn(async move {
            for byte in input {
                client.write_all(&[byte]).await.unwrap();
                client.flush().await.unwrap();
                tokio::task::yield_now().await;
            }
            client.shutdown().await.unwrap();
            client
        });

        let mut forwarded = Vec::new();
        filter.read_to_end(&mut forwarded).await.unwrap();
        writer.await.unwrap();

        assert_eq!(forwarded, ping_packet(0).to_vec());
    }
}
