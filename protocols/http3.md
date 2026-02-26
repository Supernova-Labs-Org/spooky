# HTTP/3 Protocol Overview

HTTP/3 is the third major version of the Hypertext Transfer Protocol, designed to improve performance and reliability by running over QUIC instead of TCP. It represents a fundamental shift in how HTTP traffic is transported while maintaining compatibility with existing HTTP semantics.

## Evolution of HTTP Transport

### HTTP/1.1 Limitations

HTTP/1.1 uses text-based, whitespace-delimited message framing that is human-readable but computationally expensive to parse. The protocol lacks native multiplexing support, requiring multiple TCP connections to achieve parallelism. This approach degrades congestion control effectiveness and increases connection overhead.

Key characteristics:
- Plain-text message format with verbose headers
- One request per connection without pipelining support
- Multiple connections required for concurrent requests
- No header compression mechanism

### HTTP/2 Improvements and Remaining Issues

HTTP/2 introduced binary framing and stream multiplexing over a single TCP connection, significantly improving efficiency. However, it inherited TCP's head-of-line blocking problem: packet loss on any stream stalls all multiplexed streams until retransmission occurs.

Improvements over HTTP/1.1:
- Binary framing layer for efficient parsing
- Stream multiplexing with priority signaling
- Header compression using HPACK
- Server push capability

Limitations:
- TCP head-of-line blocking affects all streams
- Connection establishment requires separate TCP and TLS handshakes
- Limited resilience to network path changes

### QUIC as the Transport Foundation

QUIC addresses HTTP/2's limitations by providing stream multiplexing at the transport layer with independent stream reliability. Built on UDP, QUIC integrates TLS 1.3 directly into the protocol, enabling faster connection establishment and 0-RTT resumption.

QUIC features leveraged by HTTP/3:
- Per-stream flow control and independent loss recovery
- Connection-level congestion control
- Integrated TLS 1.3 with single handshake
- Connection migration support via connection identifiers
- Reduced latency with 0-RTT data transmission

HTTP/3 maps HTTP semantics onto QUIC streams, inheriting these transport-layer improvements while maintaining HTTP/2's design principles.

## HTTP/3 Protocol Architecture

### Transport Layer

HTTP/3 operates exclusively over QUIC (RFC 9000), utilizing QUIC's stream multiplexing and flow control mechanisms. Each HTTP request/response pair maps to a single bidirectional QUIC stream, providing stream-level isolation and independent delivery guarantees.

### Frame Structure

HTTP/3 communication uses typed frames transmitted within QUIC streams. Common frame types include:

- **HEADERS**: Carries HTTP header fields (compressed using QPACK)
- **DATA**: Conveys request or response payload
- **SETTINGS**: Communicates connection-level parameters
- **PUSH_PROMISE**: Initiates server push
- **GOAWAY**: Initiates graceful connection shutdown

Frame types are variable-length encoded, with the frame type and length fields preceding the payload.

### Stream Types

HTTP/3 defines three stream types:

1. **Request streams** (client-initiated bidirectional): Carry HTTP request/response exchanges
2. **Control stream** (unidirectional): Manages connection-wide settings and parameters
3. **Push streams** (server-initiated unidirectional): Deliver server-pushed responses

### Request/Response Model

Each HTTP request/response cycle consumes one client-initiated bidirectional QUIC stream. The client sends HEADERS and optional DATA frames, then half-closes the stream. The server responds with HEADERS and DATA frames before closing its sending side.

Stream independence ensures that packet loss on one stream does not block others, eliminating TCP-layer head-of-line blocking.

### Server Push

HTTP/3 supports server push through the PUSH_PROMISE frame, which reserves a push stream identifier. The server then sends the pushed response on the corresponding push stream. Clients can limit or disable push using MAX_PUSH_ID and CANCEL_PUSH frames.

### Header Compression with QPACK

HTTP/3 replaces HTTP/2's HPACK compression with QPACK (RFC 9204), designed to handle QUIC's unordered stream delivery. QPACK maintains compression efficiency while allowing headers to be decoded independently when streams arrive out of order.

QPACK features:
- Dynamic table updates on a dedicated unidirectional stream
- Reference tracking to prevent decoding dependencies
- Configurable risk/compression tradeoff
- Backward compatibility with HPACK concepts

## Header Format Examples

### HTTP/1.1 Header Format

```http
GET /index.html HTTP/1.1
Host: example.com
User-Agent: Mozilla/5.0
Accept: text/html,application/xhtml+xml
Accept-Encoding: gzip, deflate
Connection: keep-alive
```

HTTP/1.1 uses plain-text headers with CRLF delimiters. Each header field is a name-value pair separated by a colon.

### HTTP/2 Header Format

```text
HEADERS Frame (Stream 1):
  :method = GET
  :path = /index.html
  :scheme = https
  :authority = example.com
  user-agent = Mozilla/5.0
  accept = text/html,application/xhtml+xml
  accept-encoding = gzip, deflate
```

HTTP/2 introduces pseudo-header fields (prefixed with `:`) for request metadata. Headers are encoded using HPACK and transmitted in a HEADERS frame.

### HTTP/3 Header Format

```text
HEADERS Frame (Stream 0):
  :method = GET
  :path = /index.html
  :scheme = https
  :authority = example.com
  user-agent = Mozilla/5.0
  accept = text/html,application/xhtml+xml
  accept-encoding = gzip, deflate
```

HTTP/3 maintains HTTP/2's pseudo-header syntax but uses QPACK encoding instead of HPACK. The semantic meaning and structure are identical to HTTP/2.

## HTTP/2 vs HTTP/3 Comparison

| Feature                  | HTTP/2                                         | HTTP/3                                    |
|--------------------------|------------------------------------------------|-------------------------------------------|
| Transport Protocol       | TCP                                            | QUIC (over UDP)                           |
| Connection Establishment | Separate TCP and TLS handshakes (2-3 RTT)      | Combined QUIC+TLS handshake (1-RTT)       |
| Header Compression       | HPACK                                          | QPACK                                     |
| Stream Multiplexing      | Application layer (within TCP connection)      | Transport layer (native QUIC streams)     |
| Head-of-Line Blocking    | TCP-layer blocking affects all streams         | Per-stream delivery (no cross-stream HoL) |
| Connection Migration     | Not supported (TCP 4-tuple bound)              | Supported via connection IDs              |
| 0-RTT Support            | Limited (TLS 1.3 early data with constraints)  | Native 0-RTT with replay protection       |
| Header Field Syntax      | Pseudo-headers (`:method`, `:path`, `:scheme`) | Same as HTTP/2                            |
| Server Push              | PUSH_PROMISE frame                             | PUSH_PROMISE frame (same semantics)       |
| Flow Control             | Stream and connection level                    | Stream and connection level               |

## HTTP/3 Implementation in Spooky

Spooky implements HTTP/3 termination at the edge, accepting HTTP/3 client connections and translating them to HTTP/2 for backend forwarding. This approach enables HTTP/3 client support without requiring backend infrastructure changes.

### Protocol Conversion

The Spooky bridge component handles bidirectional translation between HTTP/3 and HTTP/2:

1. **Request path**: HTTP/3 HEADERS and DATA frames are parsed, decompressed via QPACK, and re-encoded as HTTP/2 frames with HPACK compression for backend transmission.

2. **Response path**: HTTP/2 responses from backends are decoded, headers are converted to QPACK format, and frames are transmitted over the client's HTTP/3 stream.

### Stream Mapping

Each HTTP/3 client stream maps to a corresponding HTTP/2 stream on a backend connection. Stream lifecycle events (creation, data transfer, closure) are synchronized between protocols.

### Connection Management

Spooky maintains separate connection pools for client-facing HTTP/3 sessions and backend HTTP/2 connections. QUIC connection state is managed by the edge component, while HTTP/2 connection pooling is handled by the transport layer.

### Feature Support

- **Header compression**: Full QPACK support for clients, HPACK for backends
- **Flow control**: QUIC stream and connection flow control enforced
- **Server push**: Not currently implemented (client-to-proxy only)
- **Priority signaling**: HTTP/3 priority frames are mapped to HTTP/2 priority when supported by backends

## Performance Characteristics

HTTP/3 offers several performance advantages over HTTP/2:

**Reduced latency**: Combined QUIC+TLS handshake reduces connection establishment time. 0-RTT resumption enables immediate data transmission for repeat connections.

**Improved loss resilience**: Independent stream delivery prevents packet loss on one stream from blocking others. TCP's retransmission delays do not propagate across streams.

**Better mobile performance**: Connection migration allows QUIC connections to survive network changes (Wi-Fi to cellular transitions) without interruption.

**Optimal congestion control**: QUIC's congestion control operates at the connection level without TCP's limitations, enabling more accurate RTT estimation and loss detection.

## References

- RFC 9114: HTTP/3
- RFC 9000: QUIC Transport Protocol
- RFC 9204: QPACK Header Compression
- RFC 9218: Extensible Prioritization Scheme for HTTP
