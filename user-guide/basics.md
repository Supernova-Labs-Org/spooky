# Spooky Basics

This guide covers fundamental concepts and basic usage of Spooky, an HTTP/3 to HTTP/2 edge proxy.

## Architecture Overview

Spooky operates as a protocol translation layer between HTTP/3 clients and HTTP/2 backend services:

1. **QUIC Connection Termination**: Accepts incoming HTTP/3 requests over QUIC
2. **Protocol Translation**: Converts HTTP/3 streams to HTTP/2 requests
3. **Load Balancing**: Routes requests to backend servers based on configured algorithms
4. **Response Conversion**: Translates backend HTTP/2 responses back to HTTP/3
5. **Client Delivery**: Returns responses to the client over QUIC

```
Client (HTTP/3/QUIC) → Spooky Edge → Backend (HTTP/2)
                            ↓
                    Route Matching
                    Load Balancing
                    Health Checking
```

## Configuration Structure

### Minimal Configuration

```yaml
version: 1

listen:
  protocol: http3
  port: 9889
  address: "0.0.0.0"
  tls:
    cert: "server.crt"
    key: "server.key"

upstream:
  default_pool:
    load_balancing:
      type: "random"

    route:
      path_prefix: "/"

    backends:
      - id: "backend1"
        address: "127.0.0.1:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

log:
  level: info
```

### Configuration Sections

**listen**: Defines the edge server configuration
- `protocol`: Must be "http3"
- `port`: UDP port for QUIC connections (default: 9889)
- `address`: Bind address (default: "0.0.0.0")
- `tls.cert`: Path to TLS certificate
- `tls.key`: Path to TLS private key

**upstream**: Named pools of backend servers with routing rules and load balancing configuration
- Key: Arbitrary pool name for identification
- `load_balancing`: Load balancing algorithm for this pool (`random`, `round-robin`, `consistent-hash`)
- `route`: Routing criteria to match requests
- `backends`: List of backend servers

**log**: Logging configuration
- `level`: Log verbosity (trace, debug, info, warn, error)
- `file.enabled`: Write logs to a file instead of stderr (default: false)
- `file.path`: Log file path (default: `/var/log/spooky/spooky.log`)

## Upstream Pools and Routing

Spooky supports multiple upstream pools with independent routing rules. Requests are matched using longest-prefix matching across all configured routes.

### Path-Based Routing

```yaml
upstream:
  api_pool:
    route:
      path_prefix: "/api"
    backends:
      - id: "api1"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

  auth_pool:
    route:
      path_prefix: "/auth"
    backends:
      - id: "auth1"
        address: "10.0.2.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

  default_pool:
    route:
      path_prefix: "/"
    backends:
      - id: "web1"
        address: "10.0.3.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
```

**Routing Behavior**:
- Requests to `/api/*` → api_pool
- Requests to `/auth/*` → auth_pool
- All other requests → default_pool

### Host-Based Routing

```yaml
upstream:
  api_backend:
    route:
      host: "api.example.com"
    backends:
      - id: "api1"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

  www_backend:
    route:
      host: "www.example.com"
    backends:
      - id: "web1"
        address: "10.0.2.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
```

### Combined Routing

```yaml
upstream:
  api_v2:
    route:
      host: "api.example.com"
      path_prefix: "/v2"
    backends:
      - id: "api-v2-1"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
```

Matches requests to `api.example.com/v2/*`.

## Backend Configuration

### Backend Parameters

```yaml
backends:
  - id: "backend1"              # Unique identifier
    address: "127.0.0.1:8080"   # Backend address (IP:port)
    weight: 100                 # Relative weight for load balancing
    health_check:
      path: "/health"           # Health check endpoint
      interval: 5000            # Check interval (milliseconds)
      timeout_ms: 2000          # Request timeout (milliseconds)
      failure_threshold: 3      # Consecutive failures before marking unhealthy
      success_threshold: 2      # Consecutive successes before marking healthy
      cooldown_ms: 5000         # Cooldown period after marking unhealthy
```

### Health Check Configuration

**interval**: Time between health checks in milliseconds (default: 5000)

**timeout_ms**: Maximum time to wait for health check response (default: 1000)

**failure_threshold**: Number of consecutive failures required to mark backend unhealthy (default: 3)

**success_threshold**: Number of consecutive successes required to mark backend healthy after being unhealthy (default: 2)

**cooldown_ms**: Time to wait before attempting recovery after marking unhealthy (default: 5000)

### Health Check Implementation

Backend services must implement health check endpoints that return 2xx status codes when healthy:

```javascript
// Node.js example
app.get('/health', (req, res) => {
  // Verify critical dependencies
  if (database.isConnected() && cache.isReady()) {
    res.status(200).json({ status: 'healthy' });
  } else {
    res.status(503).json({ status: 'unhealthy' });
  }
});
```

```python
# Python example
@app.route('/health')
def health():
    if check_database() and check_cache():
        return {'status': 'healthy'}, 200
    return {'status': 'unhealthy'}, 503
```

## Command Line Interface

### Starting Spooky

```bash
# Start with configuration file
spooky --config config.yaml

# Display version
spooky --version
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--config` | Path to configuration file | Required |
| `--version` | Display version information | - |
| `--help` | Display help information | - |

**Note:** Log level is configured in `config.yaml` (`log.level`) or via `RUST_LOG` environment variable. Configuration validation happens automatically during startup.

## Testing and Verification

### Testing with curl

Requires curl built with HTTP/3 support (nghttp3 and ngtcp2):

```bash
# Basic HTTP/3 request
curl --http3-only -k https://localhost:9889/

# Test with custom host resolution
curl --http3-only -k \
  --resolve example.com:9889:127.0.0.1 \
  https://example.com:9889/api/users

# Test with headers
curl --http3-only -k \
  -H "Authorization: Bearer token" \
  https://localhost:9889/protected

# Verbose output for debugging
curl --http3-only -k -v https://localhost:9889/
```

### Load Balancing Verification

```bash
# Generate concurrent requests
for i in {1..20}; do
  curl --http3-only -k https://localhost:9889/ &
done
wait

# Monitor backend request distribution (systemd)
sudo journalctl -u spooky.service | grep "routing to backend" | \
  awk '{print $NF}' | sort | uniq -c

# Monitor backend request distribution (if redirected to file)
grep "routing to backend" /var/log/spooky/spooky.log | \
  awk '{print $NF}' | sort | uniq -c
```

### Health Check Monitoring

```bash
# Monitor health check activity (systemd)
sudo journalctl -u spooky.service -f | grep -i health

# Monitor health check activity (direct process)
spooky --config config.yaml 2>&1 | grep -i health

# Check backend status (systemd)
sudo journalctl -u spooky.service | grep "backend.*healthy" | tail -20

# Check backend status (if redirected to file)
grep "backend.*healthy" /var/log/spooky/spooky.log | tail -20
```

## Logging

### Log Levels

| Level | Description | Use Case |
|-------|-------------|----------|
| `trace` | Extremely verbose, includes protocol details | Protocol debugging |
| `debug` | Detailed operational information | Development, troubleshooting |
| `info` | General operational messages | Production (default) |
| `warn` | Warning conditions | Production |
| `error` | Error conditions | Production |

### Log Configuration

```yaml
# stderr (default)
log:
  level: info

# write to file
log:
  level: info
  file:
    enabled: true
    path: /var/log/spooky/spooky.log
```

### Log Analysis

**systemd (log.file.enabled: false)**

```bash
# Follow logs in real-time
sudo journalctl -u spooky.service -f

# Filter by severity
sudo journalctl -u spooky.service | grep ERROR

# Search for specific requests
sudo journalctl -u spooky.service | grep "GET /api/users"

# Monitor backend selection
sudo journalctl -u spooky.service | grep "routing to backend"

# Track health check failures
sudo journalctl -u spooky.service | grep "health check failed"
```

**File (log.file.enabled: true)**

```bash
# Follow logs in real-time
tail -f /var/log/spooky/spooky.log

# Filter by severity
grep ERROR /var/log/spooky/spooky.log

# Search for specific requests
grep "GET /api/users" /var/log/spooky/spooky.log

# Monitor backend selection
grep "routing to backend" /var/log/spooky/spooky.log

# Track health check failures
grep "health check failed" /var/log/spooky/spooky.log
```

## Common Deployment Patterns

### Development Setup

```bash
# Generate self-signed certificate
openssl req -x509 -newkey rsa:2048 \
  -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/CN=localhost"

# Create development configuration
cat > dev-config.yaml <<EOF
version: 1

listen:
  protocol: http3
  port: 9889
  address: "127.0.0.1"
  tls:
    cert: "server.crt"
    key: "server.key"

upstream:
  dev_pool:
    load_balancing:
      type: "random"
    route:
      path_prefix: "/"
    backends:
      - id: "dev-backend"
        address: "127.0.0.1:3000"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

log:
  level: debug
EOF

# Start Spooky
spooky --config dev-config.yaml
```

### Example Multi-Backend Setup

> **Note:** Spooky is experimental. The configuration below shows how a multi-backend setup would look, but is not a production deployment recommendation.

```bash
# Obtain certificates (Let's Encrypt)
certbot certonly --standalone -d example.com

# Create production configuration
cat > prod-config.yaml <<EOF
version: 1

listen:
  protocol: http3
  port: 443
  address: "0.0.0.0"
  tls:
    cert: "/etc/letsencrypt/live/example.com/fullchain.pem"
    key: "/etc/letsencrypt/live/example.com/privkey.pem"

upstream:
  prod_pool:
    load_balancing:
      type: "round-robin"
    route:
      path_prefix: "/"
    backends:
      - id: "web-01"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 10000
          timeout_ms: 3000
          failure_threshold: 3
          success_threshold: 2
          cooldown_ms: 30000
      - id: "web-02"
        address: "10.0.1.11:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 10000
          timeout_ms: 3000
          failure_threshold: 3
          success_threshold: 2
          cooldown_ms: 30000

log:
  level: info
EOF

# Deploy as systemd service
sudo systemctl start spooky
sudo systemctl enable spooky
```

## Troubleshooting

### Connection Issues

```bash
# Verify Spooky is listening on UDP
sudo netstat -uln | grep 9889
# or
sudo ss -uln | grep 9889

# Check firewall configuration
sudo ufw status
sudo iptables -L -n -v | grep 9889

# Test UDP connectivity
nc -u -v -z localhost 9889

# Verify TLS certificate
openssl s_client -connect localhost:9889 -showcerts
```

### Backend Connectivity

```bash
# Test backend directly
curl -v http://127.0.0.1:8080/health

# Test through Spooky
curl --http3-only -k -v https://localhost:9889/health

# Check backend reachability from Spooky host
telnet 10.0.1.10 8080
```

### Configuration Validation

```bash
# Validate configuration syntax (startup validation happens before serving)
spooky --config config.yaml

# Check for YAML syntax errors
yamllint config.yaml

# Verify routing configuration
grep -A 10 "route:" config.yaml
```

### Performance Debugging

```bash
# Monitor system resources
top -p $(pgrep spooky)
htop -p $(pgrep spooky)

# Check UDP buffer statistics
netstat -su | grep Udp

# Monitor QUIC connections
ss -u -a | grep 9889

# Check for packet loss
netstat -s | grep -i lost
```

## Next Steps

- Review [Load Balancing](load-balancing.md) for detailed algorithm documentation
- Refer to Configuration Reference for complete parameter documentation
- See Deployment Guide for production deployment best practices
