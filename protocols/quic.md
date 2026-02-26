# QUIC Protocol Overview

QUIC (Quick UDP Internet Connections) is a multiplexed, secure transport protocol designed to address the performance limitations of TCP while maintaining reliability guarantees. Built on UDP, QUIC integrates transport and cryptographic handshakes, provides native stream multiplexing, and supports connection migration across network changes.

**Note**: QUIC is a proper name, not an acronym, despite its origin as "Quick UDP Internet Connections."

## Protocol Fundamentals

### Design Goals

QUIC was designed to overcome specific limitations in TCP-based protocols:

1. **Eliminate head-of-line blocking**: TCP's byte-stream abstraction causes packet loss to block all multiplexed application streams. QUIC provides independent stream delivery.

2. **Reduce connection establishment latency**: TCP and TLS negotiate separately, requiring multiple round trips. QUIC combines transport and cryptographic handshakes into a single exchange.

3. **Enable connection migration**: TCP connections are bound to a 4-tuple (source IP, source port, destination IP, destination port). Network changes break connections. QUIC uses connection identifiers to survive NAT rebinding and network transitions.

4. **Modernize congestion control**: QUIC's design allows rapid deployment of improved congestion control algorithms without requiring operating system updates.

5. **Provide encrypted transport by default**: All QUIC payload data is encrypted using TLS 1.3, with only minimal connection metadata exposed.

### Transport Architecture

QUIC operates as a connection-oriented protocol over UDP datagrams. Each UDP packet carries a QUIC packet containing one or more frames. Frames represent protocol operations: stream data transfer, acknowledgments, flow control updates, and connection management.

Key architectural components:

- **Connections**: Stateful communication contexts identified by connection IDs
- **Packets**: UDP datagram payloads containing encrypted frames
- **Frames**: Typed protocol messages (stream data, control signals, etc.)
- **Streams**: Ordered byte-stream channels multiplexed within a connection

## Connection Lifecycle

### Connection Establishment

QUIC connections are established through a handshake that combines transport-layer and cryptographic negotiation. The process involves:

**1-RTT Handshake (First Connection):**

```
Client                                      Server
  |                                            |
  |-- Initial[CRYPTO, PADDING] -------------->|
  |   (ClientHello, Transport Params)         |
  |                                            |
  |<-- Initial[CRYPTO, ACK] -------------------|
  |<-- Handshake[CRYPTO] ----------------------|
  |   (ServerHello, EncryptedExtensions,       |
  |    Certificate, CertificateVerify,         |
  |    Finished, Transport Params)             |
  |                                            |
  |-- Handshake[CRYPTO, ACK] ---------------->|
  |   (Finished)                               |
  |                                            |
  |<== Application Data ======================|
  |==> Application Data ======================|
```

After the handshake completes, both endpoints can send application data. The entire process requires one round trip.

**0-RTT Handshake (Resumption):**

For resumed connections, clients can send application data in the first flight alongside the ClientHello, reducing latency to zero round trips at the cost of replay protection:

```
Client                                      Server
  |                                            |
  |-- Initial[CRYPTO] + 0-RTT[Stream Data] -->|
  |   (ClientHello, Application Data)          |
  |                                            |
  |<-- Initial[CRYPTO] + Handshake[CRYPTO] ----|
  |   (ServerHello, Finished)                  |
  |                                            |
  |==> 1-RTT[Stream Data] =====================|
  |<== 1-RTT[Stream Data] =====================|
```

0-RTT data must be idempotent and replay-safe, as attackers can capture and replay the initial packet.

### Transport Parameters

During connection establishment, endpoints exchange transport parameters that define connection behavior:

- `initial_max_data`: Connection-level flow control limit
- `initial_max_stream_data_bidi_local/remote`: Per-stream flow control limits
- `initial_max_streams_bidi/uni`: Maximum concurrent streams allowed
- `max_idle_timeout`: Connection idle timeout duration
- `max_udp_payload_size`: Maximum UDP payload the endpoint can receive
- `active_connection_id_limit`: Number of connection IDs the peer can provide

These parameters are immutable for the connection lifetime and must not change during resumption.

### Connection Migration

QUIC connections are identified by connection IDs rather than network 4-tuples, enabling migration across network paths. Use cases include:

- **NAT rebinding**: UDP port changes due to NAT timeout
- **Network transitions**: Mobile device switching from Wi-Fi to cellular
- **Load balancing**: Server-initiated migration for capacity management

Migration process:

1. Client sends a packet from a new network path with a new source address
2. Server validates the new path using a PATH_CHALLENGE frame
3. Client responds with PATH_RESPONSE to prove path ownership
4. Server begins sending packets to the new address

Connection migration is client-initiated by default. Servers can only migrate by providing new connection IDs for clients to use.

### Connection Termination

QUIC supports three termination mechanisms:

**1. Graceful Shutdown (CONNECTION_CLOSE):**

Either endpoint can send a CONNECTION_CLOSE frame to cleanly terminate the connection. The frame includes an error code and reason phrase. The peer acknowledges termination and both sides enter the closing state.

**2. Idle Timeout:**

If no packets are exchanged for the negotiated `max_idle_timeout` duration, the connection is silently closed without sending frames. This prevents resource exhaustion from abandoned connections.

**3. Stateless Reset:**

If an endpoint loses connection state (due to restart or migration), it cannot process incoming packets. The endpoint sends a stateless reset token to force the peer to abandon the connection immediately without further handshake.

## Stream Multiplexing

### Stream Types

QUIC provides four stream types based on directionality and initiator:

| Type                     | Initiated By | Direction      | Use Case                              |
|--------------------------|--------------|----------------|---------------------------------------|
| Bidirectional (client)   | Client       | Both           | HTTP request/response                 |
| Bidirectional (server)   | Server       | Both           | Server-initiated interactions         |
| Unidirectional (client)  | Client       | Client→Server  | Control streams, telemetry            |
| Unidirectional (server)  | Server       | Server→Client  | Server push, control streams          |

Stream IDs encode the type and initiator in the two least-significant bits:

- `0x00, 0x04, 0x08...`: Client-initiated bidirectional
- `0x01, 0x05, 0x09...`: Server-initiated bidirectional
- `0x02, 0x06, 0x0A...`: Client-initiated unidirectional
- `0x03, 0x07, 0x0B...`: Server-initiated unidirectional

### Stream Lifecycle

Streams are created implicitly by sending a frame with the corresponding stream ID. No explicit open signal is required.

**State transitions:**

1. **Idle**: Stream ID has not been used
2. **Open**: Data is being transmitted
3. **Half-closed (local)**: Local endpoint finished sending (sent FIN bit)
4. **Half-closed (remote)**: Remote endpoint finished sending (received FIN bit)
5. **Closed**: Both sides finished sending

Streams can be terminated early using RESET_STREAM (sending direction) and STOP_SENDING (receiving direction) frames.

### Flow Control

QUIC implements credit-based flow control at two levels:

**Stream-level flow control:**

Each stream has an independent flow control window. The receiver advertises the maximum byte offset it is willing to receive via MAX_STREAM_DATA frames. The sender must not exceed this limit.

**Connection-level flow control:**

The total bytes across all streams are subject to connection-level limits advertised via MAX_DATA frames. This prevents a single stream from consuming all connection resources.

Flow control updates are sent asynchronously as data is consumed, allowing the sender to continue transmission without blocking.

## Reliability and Loss Recovery

### Reliable Delivery

QUIC guarantees ordered, reliable delivery within each stream. Frames are assigned packet numbers (monotonically increasing per packet). The receiver acknowledges received packets using ACK frames, which include:

- Largest acknowledged packet number
- ACK delay (time between packet receipt and ACK generation)
- ACK ranges (compressed representation of received packets)

### Loss Detection

QUIC uses acknowledgment-based loss detection. Packets are considered lost if:

1. **Reordering threshold**: Three or more packets sent later have been acknowledged
2. **Time threshold**: Sufficient time has elapsed since transmission without acknowledgment (typically 9/8 * smoothed RTT)

Declared lost packets trigger retransmission of their frames.

### Retransmission

Lost frames are retransmitted in new packets with new packet numbers. QUIC does not retransmit packets verbatim; only the frames within them are resent. This decoupling allows acknowledgments to unambiguously identify which transmission was received.

### Congestion Control

QUIC mandates congestion control but does not prescribe a specific algorithm. Implementations commonly use:

- **NewReno**: TCP NewReno adapted for QUIC (RFC 9002 default)
- **CUBIC**: More aggressive window growth for high-bandwidth paths
- **BBR**: Bottleneck bandwidth and round-trip propagation time estimation

Congestion control operates per connection, not per stream. All streams share the congestion window and pacing budget.

## Packet Structure

QUIC packets consist of a header and payload. Two header types exist:

### Long Header (Handshake Packets)

Used during connection establishment. Contains:

- Version field (4 bytes)
- Destination Connection ID Length + Destination Connection ID
- Source Connection ID Length + Source Connection ID
- Type-specific fields (token, length)
- Packet Number
- Encrypted payload

Long header packets have types: Initial, 0-RTT, Handshake, and Retry.

### Short Header (1-RTT Packets)

Used after handshake completion. Contains:

- Spin bit (latency measurement)
- Destination Connection ID
- Packet Number
- Encrypted payload

Short headers minimize overhead for long-lived connections.

### Frame Types

Common frame types:

- **PADDING**: Increases packet size for PMTU probing
- **PING**: Elicits acknowledgment without sending data
- **ACK**: Acknowledges received packets
- **STREAM**: Carries stream data with offset and length
- **MAX_DATA / MAX_STREAM_DATA**: Advertises flow control updates
- **RESET_STREAM / STOP_SENDING**: Aborts stream transmission
- **CONNECTION_CLOSE**: Terminates the connection
- **PATH_CHALLENGE / PATH_RESPONSE**: Validates network path during migration

## Security Properties

### Encryption

QUIC uses TLS 1.3 for key derivation and encryption. Packet payloads are encrypted using AEAD algorithms (typically AES-128-GCM or ChaCha20-Poly1305). Packet headers are protected to prevent ossification by middleboxes.

Encryption levels:

- **Initial**: Derived from well-known salt and client-chosen Destination Connection ID
- **0-RTT**: Derived from resumed session keys
- **Handshake**: Derived from ephemeral handshake keys
- **1-RTT**: Derived from final application traffic keys

### Header Protection

Packet numbers are encrypted to prevent middleboxes from tracking flows and inferring loss patterns. A sample from the encrypted payload is used to mask the packet number and flags.

### Replay Protection

1-RTT data is protected by TLS's anti-replay mechanisms. 0-RTT data is inherently replayable and must be used only for idempotent operations. Servers can optionally reject 0-RTT to enforce strict replay protection.

## QUIC Implementation in Spooky

Spooky uses the `quiche` QUIC library for connection management and HTTP/3 framing. The implementation handles:

### Connection Management

- **Endpoint creation**: Spooky binds to the configured UDP port and initializes the QUIC endpoint with TLS configuration
- **Connection acceptance**: Incoming QUIC connections are validated against TLS certificate requirements
- **Idle timeout**: Connections are closed after the configured idle period to reclaim resources
- **Graceful shutdown**: Spooky sends CONNECTION_CLOSE frames during shutdown to inform clients

### TLS Integration

TLS 1.3 is mandatory for QUIC. Spooky loads certificate chains and private keys at startup:

```rust
let tls_config = rustls::ServerConfig::builder()
    .with_safe_defaults()
    .with_no_client_auth()
    .with_single_cert(certs, private_key)?;
```

ALPN (Application-Layer Protocol Negotiation) is configured to advertise "h3" for HTTP/3:

```rust
tls_config.alpn_protocols = vec![b"h3".to_vec()];
```

### Stream Handling

HTTP/3 streams are mapped to QUIC streams:

- **Request streams**: Client-initiated bidirectional streams carry HTTP requests and responses
- **Control stream**: Unidirectional stream for HTTP/3 settings and control frames
- **QPACK streams**: Separate unidirectional streams for QPACK encoder/decoder communication

Spooky's edge component accepts incoming streams and passes them to the bridge for HTTP/3 processing.

### Flow Control

Spooky configures QUIC transport parameters to balance memory usage and throughput:

- Connection-level flow control prevents excessive buffering across all streams
- Stream-level flow control limits individual stream memory consumption
- Defaults are tuned for typical web traffic patterns but can be adjusted for specific workloads

### Congestion Control

Spooky uses the default NewReno congestion control provided by `quiche`. This choice balances fairness with TCP flows and predictable behavior across network conditions.

## Performance Considerations

### UDP Socket Optimization

QUIC performance depends on efficient UDP socket handling:

- **Receive buffer size**: Increase `SO_RCVBUF` to reduce packet loss during bursts
- **Send buffer size**: Increase `SO_SNDBUF` to support large congestion windows
- **GRO/GSO**: Generic Receive/Send Offload reduces per-packet processing overhead on Linux

### Packet Pacing

Sending packets in bursts can trigger packet loss due to buffer overflow. QUIC implementations pace packet transmission based on the congestion window and RTT to smooth traffic.

### CPU Usage

QUIC's cryptographic operations (AEAD encryption/decryption, header protection) consume CPU. Performance tuning:

- Use hardware-accelerated cryptographic instructions (AES-NI)
- Batch packet processing to amortize per-packet overhead
- Offload TLS operations to dedicated threads if necessary

### Memory Footprint

Each QUIC connection maintains state for:

- Send and receive buffers for each stream
- Loss recovery metadata (sent packet information)
- Congestion control state (RTT estimates, congestion window)

Spooky limits concurrent connections and streams to bound memory usage.

## Comparison with TCP

| Feature                  | TCP                                   | QUIC                                  |
|--------------------------|---------------------------------------|---------------------------------------|
| Transport Layer          | Kernel-space implementation           | User-space implementation             |
| Connection Establishment | 3-way handshake + TLS (2-3 RTT)       | Combined handshake (1-RTT or 0-RTT)   |
| Stream Multiplexing      | Not supported (requires app layer)    | Native transport-level multiplexing   |
| Head-of-Line Blocking    | All streams blocked by packet loss    | Independent per-stream delivery       |
| Connection Migration     | Not supported                         | Supported via connection IDs          |
| Congestion Control       | OS-dependent, slow to update          | User-space, rapid algorithm evolution |
| Encryption               | Optional (TLS on top)                 | Mandatory (integrated TLS 1.3)        |
| NAT Traversal            | Requires keep-alive mechanisms        | Built-in connection migration support |
| Deployment               | Requires OS updates                   | Application-level deployment          |

## References

- RFC 9000: QUIC: A UDP-Based Multiplexed and Secure Transport
- RFC 9001: Using TLS to Secure QUIC
- RFC 9002: QUIC Loss Detection and Congestion Control
- RFC 8999: Version-Independent Properties of QUIC
- RFC 9221: An Unreliable Datagram Extension to QUIC
- RFC 9287: Greasing the QUIC Bit
