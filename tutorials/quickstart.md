# Quickstart Guide

This guide demonstrates how to deploy a working Spooky HTTP/3 proxy in under 10 minutes. You will set up a basic proxy configuration, generate self-signed certificates, and verify HTTP/3 connectivity to a backend service.

## Prerequisites

- Rust 1.85 or later installed (edition 2024)
- Basic familiarity with command-line tools
- An HTTP/2 backend service running locally (or use the example backend provided)
- UDP port 9889 available for QUIC traffic

## Step 1: Build Spooky

Clone the repository and build the release binary:

```bash
git clone https://github.com/nishujangra/spooky.git
cd spooky
cargo build --release
```

The compiled binary will be located at `target/release/spooky`. Build time is typically 2-5 minutes depending on your system.

## Step 2: Generate Self-Signed Certificates

QUIC requires TLS 1.3, so you need certificate and key files. For testing purposes, generate a self-signed certificate:

```bash
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout certs/key.pem \
  -out certs/cert.pem \
  -days 365 \
  -subj "/CN=localhost"
```

This creates:
- `certs/cert.pem`: The TLS certificate
- `certs/key.pem`: The private key

**Note:** For production deployments, use certificates from a trusted Certificate Authority (CA). See [TLS Configuration](../configuration/tls.md) for production certificate setup.

## Step 3: Start a Test Backend Server

You need an HTTP/2 backend for Spooky to forward traffic to. If you don't have one running, use the provided HTTP/2 test backend:

```bash
# Using Spooky's built-in HTTP/2 test backend
cargo run --bin h2_backend -- --port 8080
```

This starts an HTTP/2-only server on `127.0.0.1:8080`. Spooky requires HTTP/2 backends - HTTP/1.1 backends are not supported.

## Step 4: Create Configuration File

Create a minimal configuration file named `config.yaml`:

```yaml
version: 1

listen:
  protocol: http3
  port: 9889
  address: "0.0.0.0"
  tls:
    cert: "certs/cert.pem"
    key: "certs/key.pem"

upstream:
  default:
    load_balancing:
      type: "random"
    route:
      path_prefix: "/"
    backends:
      - id: "local-backend"
        address: "127.0.0.1:8080"
        weight: 100
        health_check:
          path: "/"
          interval: 5000
          timeout_ms: 2000
          success_threshold: 2
          failure_threshold: 3

log:
  level: info
```

This configuration:
- Listens for HTTP/3 connections on UDP port 9889
- Uses the generated self-signed certificates
- Forwards all requests to `127.0.0.1:8080` using random load balancing
- Performs health checks every 5 seconds on the backend

## Step 5: Start Spooky

Launch the proxy with the configuration file:

```bash
./target/release/spooky --config config.yaml
```

Expected output:

```
[INFO] Loading configuration from config.yaml
[INFO] Starting Spooky HTTP/3 proxy
[INFO] Listening on 0.0.0.0:9889 (HTTP/3)
[INFO] Backend local-backend (127.0.0.1:8080) marked healthy
[INFO] Proxy started successfully
```

The proxy is now accepting HTTP/3 connections on port 9889 and forwarding them to the backend on port 8080.

## Step 6: Test Connectivity

Verify that HTTP/3 requests are being proxied correctly. You will need an HTTP/3-capable client such as curl with HTTP/3 support.

### Using curl with HTTP/3

If you have curl compiled with HTTP/3 support:

```bash
curl --http3-only -k https://localhost:9889/
```

The `-k` flag bypasses certificate validation for self-signed certificates. You should see the response from your backend server.

### Using curl with Alt-Svc Discovery

For a more realistic test that mimics browser behavior:

```bash
curl -k \
  --resolve localhost:9889:127.0.0.1 \
  https://localhost:9889/
```

Verify HTTP/3 connectivity by forcing HTTP/3-only requests:

```bash
curl -k --http3-only https://localhost:9889/
```

If successful, you should receive a response from your backend. HTTP/3 connectivity is confirmed when the request succeeds (Spooky doesn't advertise Alt-Svc headers).

### Using a Custom HTTP/3 Client

If you don't have HTTP/3 support in curl, you can use other clients:

**Using h3i (HTTP/3 interactive client):**

```bash
cargo install h3i
h3i https://localhost:9889/ --insecure
```

**Using qh3 (QUIC HTTP/3 client):**

```bash
git clone https://github.com/cloudflare/quiche.git
cd quiche/tools/apps
cargo build --release
./target/release/quiche-client https://localhost:9889/ --no-verify
```

## Step 7: Verify Backend Forwarding

Check that requests are being forwarded to the backend. In the terminal running Spooky, you should see log entries indicating request handling:

```
[INFO] QUIC connection established from 127.0.0.1:55420
[INFO] HTTP/3 stream 0: GET /
[INFO] Forwarding to backend local-backend (127.0.0.1:8080)
[INFO] Response 200 OK forwarded to client
```

In the backend server terminal, verify that HTTP requests are being received.

## Step 8: Test Path-Based Routing (Optional)

To demonstrate routing capabilities, modify the configuration to add multiple upstream pools:

```yaml
upstream:
  api_backend:
    load_balancing:
      type: "round-robin"
    route:
      path_prefix: "/api"
    backends:
      - id: "api-server"
        address: "127.0.0.1:8001"
        weight: 100

  default_backend:
    load_balancing:
      type: "random"
    route:
      path_prefix: "/"
    backends:
      - id: "default-server"
        address: "127.0.0.1:8080"
        weight: 100
```

Restart Spooky with the updated configuration. Requests to `/api/*` will route to port 8001, while all other requests route to port 8080.

Test the routing:

```bash
# Routes to default backend (port 8080)
curl --http3-only -k https://localhost:9889/

# Routes to API backend (port 8001)
curl --http3-only -k https://localhost:9889/api/users
```

## Common Issues and Solutions

### Port Already in Use

If port 9889 is already bound:

```
Error: Address already in use (os error 98)
```

Solution: Either stop the conflicting process or change the port in `config.yaml`.

### Backend Connection Refused

If Spooky cannot connect to the backend:

```
[ERROR] Failed to connect to backend local-backend: Connection refused
```

Solution: Ensure the backend service is running on the configured address and port.

### Certificate Errors

If the certificate is not found:

```
[ERROR] Failed to load TLS certificate: No such file or directory
```

Solution: Verify that the certificate paths in `config.yaml` are correct and the files exist.

### Health Check Failures

If backends are marked unhealthy:

```
[WARN] Backend local-backend health check failed: timeout
```

Solution: Ensure the health check path exists on the backend and responds within the timeout period (default 2 seconds).

## Next Steps

You now have a working HTTP/3 to HTTP/2 proxy. To further configure and optimize Spooky:

- **[Configuration Reference](../configuration/reference.md)** - Complete configuration options including advanced load balancing and routing
- **[TLS Setup](../configuration/tls.md)** - Configure production TLS certificates with Let's Encrypt or other CAs
- **[Load Balancing Guide](../user-guide/load-balancing.md)** - Understand different load balancing algorithms and when to use them
- **[Production Deployment](../deployment/production.md)** - Best practices for production deployment including systemd integration and monitoring
- **[Troubleshooting](../troubleshooting/common-issues.md)** - Solutions to common operational issues

For HTTP/3 and QUIC protocol details:

- **[HTTP/3 Overview](../protocols/http3.md)** - HTTP/3 protocol implementation and differences from HTTP/2
- **[QUIC Overview](../protocols/quic.md)** - QUIC transport protocol details and how Spooky uses it
