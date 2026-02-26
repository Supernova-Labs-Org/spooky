# References

Technical references and external resources for Spooky development.

## Protocol Specifications

### QUIC and HTTP/3
- [RFC 9000 - QUIC: A UDP-Based Multiplexed and Secure Transport](https://www.rfc-editor.org/rfc/rfc9000.html)
- [RFC 9001 - Using TLS to Secure QUIC](https://www.rfc-editor.org/rfc/rfc9001.html)
- [RFC 9002 - QUIC Loss Detection and Congestion Control](https://www.rfc-editor.org/rfc/rfc9002.html)
- [RFC 9114 - HTTP/3](https://www.rfc-editor.org/rfc/rfc9114.html)
- [RFC 9204 - QPACK: Field Compression for HTTP/3](https://www.rfc-editor.org/rfc/rfc9204.html)

### HTTP/2
- [RFC 9113 - HTTP/2](https://www.rfc-editor.org/rfc/rfc9113.html)
- [RFC 7541 - HPACK: Header Compression for HTTP/2](https://www.rfc-editor.org/rfc/rfc7541.html)

### TLS
- [RFC 8446 - The Transport Layer Security (TLS) Protocol Version 1.3](https://www.rfc-editor.org/rfc/rfc8446.html)
- [RFC 7301 - Transport Layer Security (TLS) Application-Layer Protocol Negotiation Extension](https://www.rfc-editor.org/rfc/rfc7301.html)

## Core Dependencies

### QUIC and HTTP/3
- [quiche](https://docs.rs/quiche/) - Cloudflare's QUIC and HTTP/3 implementation
- [quinn](https://docs.rs/quinn/) - Alternative async QUIC implementation

### HTTP/2
- [hyper](https://docs.rs/hyper/) - HTTP client and server library
- [h2](https://docs.rs/h2/) - HTTP/2 implementation

### Async Runtime
- [tokio](https://docs.rs/tokio/) - Asynchronous runtime for Rust
- [futures](https://docs.rs/futures/) - Async utilities

### Serialization
- [serde](https://docs.rs/serde/) - Serialization framework
- [serde_yaml](https://docs.rs/serde_yaml/) - YAML support for serde

### CLI and Configuration
- [clap](https://docs.rs/clap/) - Command-line argument parser

### Utilities
- [bytes](https://docs.rs/bytes/) - Efficient byte buffer types
- [http](https://docs.rs/http/) - HTTP types
- [log](https://docs.rs/log/) - Logging facade
- [env_logger](https://docs.rs/env_logger/) - Logger implementation
- [rand](https://docs.rs/rand/) - Random number generation

### TLS
- [rustls](https://docs.rs/rustls/) - Modern TLS library
- [rustls-pki-types](https://docs.rs/rustls-pki-types/) - TLS certificate types

## Load Balancing Resources

### Algorithms
- [Consistent Hashing and Random Trees](https://www.akamai.com/site/en/documents/research-paper/consistent-hashing-and-random-trees-distributed-caching-protocols-for-relieving-hot-spots-on-the-world-wide-web-technical-publication.pdf) - Original consistent hashing paper
- [The Power of Two Random Choices](https://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf) - Random selection strategy analysis

### Health Checking
- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html) - Martin Fowler
- [Health Checks for gRPC](https://github.com/grpc/grpc/blob/master/doc/health-checking.md) - gRPC health check protocol

## Performance and Optimization

### QUIC Performance
- [QUIC at Cloudflare](https://blog.cloudflare.com/the-road-to-quic/) - Production QUIC deployment insights
- [QUIC at Google](https://www.chromium.org/quic/) - Chrome QUIC implementation notes

### HTTP/3 Optimization
- [HTTP/3 Explained](https://http3-explained.haxx.se/) - Daniel Stenberg's HTTP/3 guide
- [HTTP/3 Performance](https://www.fastly.com/blog/measuring-http3-performance) - Real-world performance analysis

### System Tuning
- [Linux Network Stack](https://www.kernel.org/doc/html/latest/networking/index.html) - Kernel networking documentation
- [UDP Performance](https://blog.cloudflare.com/how-to-receive-a-million-packets/) - High-performance UDP handling

## Security

### TLS Best Practices
- [Mozilla SSL Configuration Generator](https://ssl-config.mozilla.org/) - TLS configuration recommendations
- [Certificate Transparency](https://certificate.transparency.dev/) - CT log monitoring

### QUIC Security
- [QUIC Crypto](https://www.chromium.org/quic/quic-crypto) - QUIC cryptographic design
- [QUIC Security Considerations](https://www.rfc-editor.org/rfc/rfc9000.html#section-21) - RFC 9000 Section 21

## Testing and Debugging

### Tools
- [curl with HTTP/3](https://github.com/curl/curl/blob/master/docs/HTTP3.md) - Testing HTTP/3 endpoints
- [h3i](https://github.com/cloudflare/quiche/tree/master/tools/apps) - Interactive HTTP/3 client
- [Wireshark QUIC](https://wiki.wireshark.org/QUIC) - Packet capture and analysis

### Load Testing
- [h2load](https://nghttp2.org/documentation/h2load.1.html) - HTTP/2 load testing tool
- [wrk2](https://github.com/giltene/wrk2) - HTTP benchmarking tool

## Monitoring and Observability

### Metrics
- [Prometheus Documentation](https://prometheus.io/docs/introduction/overview/) - Metrics collection
- [OpenTelemetry](https://opentelemetry.io/) - Observability framework

### Tracing
- [Tokio Tracing](https://docs.rs/tracing/) - Application-level tracing
- [Jaeger](https://www.jaegertracing.io/) - Distributed tracing

## Related Projects

### HTTP/3 Proxies
- [nghttpx](https://nghttp2.org/documentation/nghttpx.1.html) - HTTP/2 and HTTP/3 proxy
- [h2o](https://h2o.dev/) - HTTP/1.x, HTTP/2, HTTP/3 server

### QUIC Implementations
- [quic-go](https://github.com/quic-go/quic-go) - Go QUIC implementation
- [msquic](https://github.com/microsoft/msquic) - Microsoft QUIC implementation
- [ngtcp2](https://github.com/ngtcp2/ngtcp2) - C QUIC library

### Load Balancers
- [HAProxy](https://www.haproxy.org/) - Traditional TCP/HTTP load balancer
- [Envoy](https://www.envoyproxy.io/) - Modern L7 proxy and load balancer
- [Traefik](https://traefik.io/) - Cloud-native edge router

## Community Resources

### Rust
- [Rust Programming Language Book](https://doc.rust-lang.org/book/)
- [Rust Async Book](https://rust-lang.github.io/async-book/)
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial)

### QUIC and HTTP/3
- [QUIC Working Group](https://quicwg.org/) - IETF QUIC standardization
- [HTTP/3 Implementations](https://github.com/quicwg/base-drafts/wiki/Implementations) - List of QUIC/HTTP3 implementations

## Academic Papers

- [QUIC: A UDP-Based Multiplexed and Secure Transport](https://dl.acm.org/doi/10.1145/3098822.3098842) - SIGCOMM 2017
- [The QUIC Transport Protocol: Design and Internet-Scale Deployment](https://dl.acm.org/doi/10.1145/3098822.3098842) - SIGCOMM 2017
- [An Analysis of QUIC in the Wild](https://conferences.sigcomm.org/imc/2019/papers/imc19-final240.pdf) - IMC 2019

## Contributing

To add a reference:

1. Verify the resource is authoritative and current
2. Add to the appropriate section
3. Include a brief description
4. Test all links

See [contributing guide](development/contributing.md) for more details.
