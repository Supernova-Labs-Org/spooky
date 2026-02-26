# Common Issues and Solutions

Technical reference for diagnosing and resolving operational issues in Spooky HTTP/3 to HTTP/2 gateway deployments.

## Configuration Errors

### Invalid Configuration Schema

**Error Messages**:
```
Invalid version: expected '1', found '2'
Invalid protocol: expected 'http3', found 'http2'
Invalid log level: debug-verbose
Invalid load balancing type: 'weighted' for upstream 'api'
```

**Root Causes**:
- Configuration schema version mismatch
- Unsupported protocol specification
- Invalid log level (valid: `whisper`, `haunt`, `spooky`, `scream`, `poltergeist`, `silence`, `trace`, `debug`, `info`, `warn`, `error`, `off`)
- Unsupported load balancing algorithm (valid: `random`, `round-robin`, `round_robin`, `rr`, `consistent-hash`, `consistent_hash`, `ch`)

**Diagnostic Commands**:
```bash
# Validate YAML syntax
python3 -c "import yaml; yaml.safe_load(open('config.yaml'))" 2>&1

# Check configuration structure
grep -E "^version:|^listen:|^upstream:" config.yaml

# Verify log level
grep "log:" -A 2 config.yaml

# Check load balancing configuration
grep "load_balancing:" -A 2 config.yaml
```

**Resolution**:
- Set `version: 1` in configuration file
- Use `protocol: http3` for listen configuration
- Correct log levels according to valid options
- Update load balancing type to supported algorithms

### Listen Configuration Errors

**Error Messages**:
```
Listen address is empty
Invalid listen port: 0 (must be between 1 and 65535)
Invalid listen port: 70000 (must be between 1 and 65535)
Failed to bind UDP socket
```

**Root Causes**:
- Missing or empty listen address
- Port number outside valid range (1-65535)
- Port already in use by another process
- Insufficient privileges for privileged ports (<1024)

**Diagnostic Commands**:
```bash
# Check port availability (UDP)
sudo ss -ulnp | grep :443

# Identify process using port
sudo lsof -i UDP:443

# Check socket permissions
sudo setcap -v 'cap_net_bind_service=+ep' /usr/local/bin/spooky

# Verify listen configuration
grep -A 5 "^listen:" config.yaml
```

**Resolution**:
```bash
# Grant capability for privileged port binding
sudo setcap 'cap_net_bind_service=+ep' /usr/local/bin/spooky

# Or bind to non-privileged port
sed -i 's/port: 443/port: 8443/' config.yaml

# Kill conflicting process
sudo fuser -k 443/udp
```

### Upstream Pool Configuration Errors

**Error Messages**:
```
No upstreams configured
Upstream name is empty
Upstream 'api' has no backends configured
Upstream 'api' must have either 'host' or 'path_prefix' route matcher
Route path_prefix cannot be empty for upstream 'api'
Route path_prefix must start with '/' for upstream 'api': api/v1
```

**Root Causes**:
- Empty upstream map in configuration
- Missing route matching criteria (no `host` or `path_prefix`)
- Invalid path prefix format (must start with `/`)
- Empty backend list for upstream pool

**Diagnostic Commands**:
```bash
# List configured upstreams
grep "^upstream:" -A 50 config.yaml | grep -E "^  [a-z]"

# Check route configuration
yq '.upstream[].route' config.yaml

# Validate path prefixes
grep "path_prefix:" config.yaml
```

**Resolution**:
```yaml
# Correct upstream configuration
upstream:
  api:
    route:
      host: "api.example.com"      # Host-based routing
      path_prefix: "/api"          # Must start with /
    load_balancing:
      type: round-robin
    backends:
      - id: backend1
        address: "10.0.1.10:8080"
        weight: 100
```

### Backend Configuration Errors

**Error Messages**:
```
Backend ID is empty in upstream 'api'
Backend address is empty for backend 'backend1' in upstream 'api'
Backend address '10.0.1.10' in upstream 'api' must be in host:port format
Backend 'backend1' in upstream 'api' has invalid weight (0)
Health check interval is invalid (0) for backend 'backend1' in upstream 'api'
Health check timeout is invalid (0) for backend 'backend1' in upstream 'api'
Health check failure threshold is invalid (0) for backend 'backend1' in upstream 'api'
Health check success threshold is invalid (0) for backend 'backend1' in upstream 'api'
Health check cooldown is invalid (0) for backend 'backend1' in upstream 'api'
```

**Root Causes**:
- Missing or malformed backend address (must be `host:port`)
- Zero values for weight or health check parameters
- Invalid health check configuration

**Diagnostic Commands**:
```bash
# Validate backend addresses
grep "address:" config.yaml | grep -v ":"

# Check health check configuration
yq '.upstream[].backends[].health_check' config.yaml

# Verify backend weights
yq '.upstream[].backends[].weight' config.yaml
```

**Resolution**:
```yaml
# Correct backend configuration
backends:
  - id: backend1
    address: "10.0.1.10:8080"     # Must include port
    weight: 100                    # Must be > 0
    health_check:
      path: "/health"
      interval: 5000               # Must be > 0 (milliseconds)
      timeout_ms: 2000             # Must be > 0
      failure_threshold: 3         # Must be > 0
      success_threshold: 2         # Must be > 0
      cooldown_ms: 10000           # Must be > 0
```

## TLS Certificate Problems

### Certificate File Access Errors

**Error Messages**:
```
TLS certificate file does not exist: /etc/spooky/certs/server.crt
TLS private key file does not exist: /etc/spooky/certs/server.key
Cannot read TLS certificate file '/etc/spooky/certs/server.crt': Permission denied
Cannot read TLS private key file '/etc/spooky/certs/server.key': Permission denied
Failed to load certificate '/etc/spooky/certs/server.crt': No such file or directory
Failed to load key '/etc/spooky/certs/server.key': error:02001002:system library:fopen:No such file or directory
```

**Root Causes**:
- Certificate or key file path does not exist
- Insufficient file permissions for Spooky process
- Invalid PEM format
- File ownership prevents access

**Diagnostic Commands**:
```bash
# Verify file existence and permissions
ls -la /etc/spooky/certs/server.{crt,key}

# Check file ownership
stat /etc/spooky/certs/server.crt

# Test read access
sudo -u spooky cat /etc/spooky/certs/server.crt > /dev/null

# Validate PEM format
openssl x509 -in /etc/spooky/certs/server.crt -text -noout
openssl rsa -in /etc/spooky/certs/server.key -check -noout
```

**Resolution**:
```bash
# Fix file permissions
sudo chown spooky:spooky /etc/spooky/certs/server.{crt,key}
sudo chmod 644 /etc/spooky/certs/server.crt
sudo chmod 600 /etc/spooky/certs/server.key

# Verify certificate chain
openssl verify -CAfile ca.crt /etc/spooky/certs/server.crt

# Test certificate-key pair match
diff <(openssl x509 -in server.crt -noout -modulus | openssl md5) \
     <(openssl rsa -in server.key -noout -modulus | openssl md5)
```

### TLS Handshake Failures

**Error Messages**:
```
TLS configuration error during request processing: handshake failure
Failed to load certificate: invalid certificate format
QUIC recv failed: TlsFail
```

**Root Causes**:
- Certificate-key mismatch
- Expired certificate
- Incomplete certificate chain
- Unsupported TLS version
- Client does not support required ALPN protocols (`h3`, `h3-29`)

**Diagnostic Commands**:
```bash
# Check certificate expiration
openssl x509 -in server.crt -noout -dates

# Verify certificate chain
openssl s_client -connect localhost:443 -showcerts < /dev/null

# Check ALPN negotiation (requires curl with HTTP/3 support)
curl --http3 -v https://localhost:443 2>&1 | grep -i alpn

# Monitor TLS handshake packets
sudo tcpdump -i any -n udp port 443 -X | grep -A 20 "Initial"
```

**Resolution**:
```bash
# Regenerate certificate with proper SAN
openssl req -new -x509 -days 365 -key server.key -out server.crt \
  -subj "/CN=example.com" \
  -addext "subjectAltName=DNS:example.com,DNS:*.example.com"

# Ensure certificate chain is complete
cat server.crt intermediate.crt > fullchain.crt

# Update configuration
sed -i 's|cert: .*|cert: /etc/spooky/certs/fullchain.crt|' config.yaml
```

## QUIC Connection Issues

### Connection ID Mismatch

**Error Messages**:
```
Wrong QUIC HEADER
Non-Initial packet for unknown connection, ignoring
Dropping packet for unknown connection from 192.168.1.10:52341 (DCID: a3f2...)
```

**Root Causes**:
- Client using stale connection ID after server restart
- Connection ID collision or corruption
- NAT rebinding without proper migration support
- Packet reordering or duplication

**Diagnostic Commands**:
```bash
# Monitor connection IDs in logs
journalctl -u spooky -f | grep -E "DCID|SCID"

# Check active QUIC connections
ss -u -a | grep :443

# Capture QUIC packets for analysis
sudo tcpdump -i any -w quic.pcap udp port 443
tshark -r quic.pcap -Y quic

# Count connection errors
journalctl -u spooky --since "1 hour ago" | grep -c "Wrong QUIC HEADER"
```

**Resolution**:
- Issue is typically transient; clients will establish new connections
- Ensure `set_disable_active_migration(true)` is set in QUIC config
- Check for network middleboxes modifying UDP payloads
- Increase `max_idle_timeout` if connections drop prematurely

### Version Negotiation Failures

**Error Messages**:
```
Version negotiation failed: buffer too short
Failed to send version negotiation: Network unreachable
```

**Root Causes**:
- Client requesting unsupported QUIC version
- MTU constraints preventing version negotiation packet transmission
- Network path blocking UDP packets
- Firewall stateful inspection interfering with QUIC

**Diagnostic Commands**:
```bash
# Check supported QUIC version
journalctl -u spooky | grep "PROTOCOL_VERSION"

# Test MTU path
tracepath -n -b 443 target-host

# Verify UDP egress
nc -u -v -w 1 target-host 443 < /dev/null

# Monitor version negotiation packets
sudo tcpdump -i any udp port 443 -v | grep -i version
```

**Resolution**:
```bash
# Configure smaller UDP payload size
# Edit config or quiche parameters:
# set_max_recv_udp_payload_size(1200)
# set_max_send_udp_payload_size(1200)

# Adjust firewall rules
sudo iptables -I INPUT -p udp --dport 443 -j ACCEPT
sudo iptables -I OUTPUT -p udp --sport 443 -j ACCEPT
```

### QUIC Timeout and Idle Connections

**Error Messages**:
```
QUIC recv failed: Done
Connection closed, not storing
```

**Root Causes**:
- Idle timeout exceeded (default 5000ms in Spooky)
- Network path timeout
- Client terminated connection without proper close
- NAT binding expired

**Diagnostic Commands**:
```bash
# Check connection lifetimes
journalctl -u spooky | grep -E "Creating new connection|Connection closed" | tail -20

# Monitor timeout events
journalctl -u spooky -f | grep "on_timeout"

# Analyze connection duration distribution
journalctl -u spooky --since "1 hour ago" | \
  grep "Creating new connection" | wc -l

# Check NAT timeout settings (if behind NAT)
cat /proc/sys/net/netfilter/nf_conntrack_udp_timeout
```

**Resolution**:
```rust
// Adjust idle timeout in quiche configuration
quic_config.set_max_idle_timeout(10000);  // Increase to 10 seconds

// Tune UDP stream limits
quic_config.set_initial_max_streams_bidi(200);
quic_config.set_initial_max_streams_uni(200);
```

## Backend Connectivity Failures

### Unknown Backend Errors

**Error Messages**:
```
No route found for path: /api/users (host: Some("api.example.com"))
Upstream pool not found for: api
unknown backend: 10.0.1.10:8080
```

**Root Causes**:
- Request path/host does not match any configured upstream route
- Upstream pool not properly initialized
- Backend not registered in H2 connection pool
- Route matching logic precedence issue

**Diagnostic Commands**:
```bash
# List configured routes
yq '.upstream[] | {route}' config.yaml

# Test route matching
journalctl -u spooky -f | grep "No route found"

# Verify H2 pool initialization
journalctl -u spooky --since "10 minutes ago" | grep -i "pool"

# Check backend registration
ss -t | grep :8080 | wc -l
```

**Resolution**:
```yaml
# Ensure proper route specificity (longest prefix matching)
upstream:
  api_v2:
    route:
      host: "api.example.com"
      path_prefix: "/api/v2"   # More specific
    backends: [...]

  api_v1:
    route:
      host: "api.example.com"
      path_prefix: "/api"      # Less specific
    backends: [...]

  default:
    route:
      path_prefix: "/"         # Catch-all
    backends: [...]
```

### HTTP/2 Connection Pool Errors

**Error Messages**:
```
Transport error: send: connection error detected: frame with invalid size
Transport error: send: connection closed
Transport error: body: stream error received: stream no longer needed
Backend timeout
```

**Root Causes**:
- Backend closed HTTP/2 connection unexpectedly
- H2 frame size violation
- Backend service crashed or restarted
- Connection pool exhaustion (>64 inflight requests per backend)
- Network timeout (2s default in Spooky)

**Diagnostic Commands**:
```bash
# Monitor H2 connection errors
journalctl -u spooky -f | grep "Transport error"

# Check backend H2 support
curl -I --http2 http://10.0.1.10:8080/

# Test backend health endpoint
curl -v http://10.0.1.10:8080/health

# Monitor connection pool saturation
journalctl -u spooky | grep "semaphore closed"

# Check backend service status
systemctl status backend-service
```

**Resolution**:
```bash
# Increase backend timeout if needed
# Edit quic_listener.rs: BACKEND_TIMEOUT = Duration::from_secs(5);

# Adjust max inflight requests per backend
# Edit quic_listener.rs: MAX_INFLIGHT_PER_BACKEND = 128;

# Restart backend service
sudo systemctl restart backend-service

# Monitor backend connection states
watch -n 1 'ss -t -a | grep :8080'
```

### Backend Health Check Failures

**Error Messages**:
```
Backend 10.0.1.10:8080 became unhealthy
Health checks disabled: no Tokio runtime available
```

**Root Causes**:
- Backend failing health check endpoint
- Health check timeout too aggressive
- Backend intermittently unavailable
- Network path to backend unreliable
- Health check threshold too sensitive

**Diagnostic Commands**:
```bash
# Monitor health transitions
journalctl -u spooky -f | grep -E "became healthy|became unhealthy"

# Manual health check
curl -w "@-" -o /dev/null -s http://10.0.1.10:8080/health <<< \
  'time_total: %{time_total}s\nhttp_code: %{http_code}\n'

# Check health check configuration
yq '.upstream[].backends[].health_check' config.yaml

# Monitor backend response times
httping -c 10 http://10.0.1.10:8080/health
```

**Resolution**:
```yaml
# Adjust health check parameters for stability
backends:
  - id: backend1
    address: "10.0.1.10:8080"
    health_check:
      path: "/health"
      interval: 10000           # Increase interval
      timeout_ms: 5000          # Increase timeout
      failure_threshold: 5      # Require more failures
      success_threshold: 2      # Require consecutive successes
      cooldown_ms: 30000        # Longer cooldown
```

## Load Balancing Issues

### No Healthy Backends Available

**Error Messages**:
```
no healthy servers
no servers configured for upstream
```

**Root Causes**:
- All backends failed health checks
- Empty backend list for upstream
- Backends in cooldown period after failures
- Circuit breaker triggered

**Diagnostic Commands**:
```bash
# Check backend health status
journalctl -u spooky | grep -E "became healthy|became unhealthy" | tail -20

# Monitor 503 Service Unavailable responses
journalctl -u spooky | grep "status 503"

# Count healthy vs total backends per upstream
yq '.upstream[].backends | length' config.yaml

# Check recent health transitions
journalctl -u spooky --since "5 minutes ago" | grep "Backend"
```

**Resolution**:
```bash
# Verify backend services are running
for backend in 10.0.1.10:8080 10.0.1.11:8080; do
  echo -n "$backend: "
  curl -s -o /dev/null -w "%{http_code}" http://$backend/health || echo "FAIL"
  echo
done

# Temporarily disable health checks for debugging
# Set failure_threshold very high in config.yaml

# Restart Spooky to reset health state
sudo systemctl restart spooky
```

### Uneven Load Distribution

**Symptoms**:
- One backend receives disproportionate traffic
- Round-robin not cycling through backends
- Consistent hash not distributing evenly

**Root Causes**:
- Backend weight misconfiguration
- Inconsistent hash key selection (always same key)
- Some backends marked unhealthy
- Hash ring replica count too low for consistent-hash

**Diagnostic Commands**:
```bash
# Analyze backend selection distribution
journalctl -u spooky | grep "Selected backend" | \
  awk '{print $(NF-2)}' | sort | uniq -c

# Check backend weights
yq '.upstream[].backends[] | "\(.id): \(.weight)"' config.yaml

# Monitor load balancing algorithm
journalctl -u spooky | grep "via round-robin\|via consistent-hash\|via random"

# Check hash key consistency
journalctl -u spooky | grep "request_hash_key"
```

**Resolution**:
```yaml
# Ensure proper weight distribution
backends:
  - id: backend1
    address: "10.0.1.10:8080"
    weight: 100
  - id: backend2
    address: "10.0.1.11:8080"
    weight: 100    # Equal weight for even distribution

# For consistent-hash, increase replica count
# Edit lb/src/lib.rs: DEFAULT_REPLICAS = 128;
```

## Performance Problems

### High Latency

**Symptoms**:
- `latency_ms` in logs consistently >1000ms
- Slow response times reported by clients
- Backend timeout errors (503 status)

**Root Causes**:
- Backend processing delay
- Network congestion
- Connection pool saturation
- CPU saturation on Spooky host
- Inefficient load balancing

**Diagnostic Commands**:
```bash
# Analyze latency distribution
journalctl -u spooky --since "1 hour ago" | \
  grep "latency_ms" | \
  awk '{print $(NF)}' | \
  sort -n | \
  awk '{sum+=$1; arr[NR]=$1} END {
    print "min:", arr[1];
    print "p50:", arr[int(NR*0.5)];
    print "p95:", arr[int(NR*0.95)];
    print "p99:", arr[int(NR*0.99)];
    print "max:", arr[NR];
    print "avg:", sum/NR;
  }'

# Monitor CPU usage
top -b -n 1 | grep spooky

# Check connection pool contention
journalctl -u spooky | grep "semaphore" | tail -20

# Measure backend response time directly
time curl http://10.0.1.10:8080/api/test
```

**Resolution**:
```bash
# Increase backend timeout if backends are slow but reliable
# Edit BACKEND_TIMEOUT in quic_listener.rs

# Scale backend capacity
# Add more backends to upstream pool

# Increase connection pool size
# Edit MAX_INFLIGHT_PER_BACKEND in quic_listener.rs

# Optimize backend application
# Profile and optimize backend code
```

### Memory Growth

**Symptoms**:
- RSS memory continuously increasing
- Out of memory errors
- System swapping

**Root Causes**:
- Connection leak (connections not properly closed)
- Request/response body buffering
- Metrics accumulation
- QUIC connection state not cleaned up

**Diagnostic Commands**:
```bash
# Monitor memory usage over time
while true; do
  ps -p $(pgrep spooky) -o pid,vsz,rss,cmd | tail -1
  sleep 10
done

# Check connection count
ss -u | grep -c :443

# Analyze memory map
sudo pmap -x $(pgrep spooky)

# Check for file descriptor leaks
ls -l /proc/$(pgrep spooky)/fd | wc -l
```

**Resolution**:
```bash
# Restart Spooky periodically (temporary mitigation)
sudo systemctl restart spooky

# Monitor connection cleanup
journalctl -u spooky -f | grep "Connection closed"

# Reduce max idle timeout
# Edit quic_config.set_max_idle_timeout(3000);

# Limit connection count
# Implement connection limit in accept logic
```

### UDP Packet Loss

**Symptoms**:
- Retransmissions in QUIC logs
- Client timeout errors
- Degraded throughput

**Root Causes**:
- Network congestion
- UDP buffer overflow (receive buffer too small)
- Firewall dropping packets
- MTU fragmentation

**Diagnostic Commands**:
```bash
# Check UDP buffer sizes
sysctl net.core.rmem_max net.core.rmem_default
sysctl net.core.wmem_max net.core.wmem_default

# Monitor UDP statistics
netstat -su | grep -E "packet receive errors|receive buffer errors"

# Capture packet loss
sudo tcpdump -i any -c 1000 udp port 443 -w capture.pcap
tshark -r capture.pcap -q -z io,stat,1

# Check interface statistics
ip -s link show eth0
```

**Resolution**:
```bash
# Increase UDP buffer sizes
sudo sysctl -w net.core.rmem_max=26214400
sudo sysctl -w net.core.wmem_max=26214400
sudo sysctl -w net.core.rmem_default=26214400
sudo sysctl -w net.core.wmem_default=26214400

# Make permanent
echo "net.core.rmem_max=26214400" | sudo tee -a /etc/sysctl.conf
echo "net.core.wmem_max=26214400" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p

# Reduce UDP payload size
# Edit quic_config.set_max_recv_udp_payload_size(1350);
```

## Debugging Techniques

### Enable Debug Logging

```yaml
# config.yaml
log:
  level: haunt  # debug level
```

```bash
# Restart to apply
sudo systemctl restart spooky

# Monitor debug logs
journalctl -u spooky -f --output=cat
```

### Analyze Request Flow

```bash
# Trace specific request path
journalctl -u spooky | grep -E "HTTP/3 request|Selected backend|Upstream.*status" | \
  grep "/api/users"

# Monitor complete request lifecycle
journalctl -u spooky -f | \
  grep -E "Creating new connection|HTTP/3 request|Selected backend|status.*latency_ms|Connection closed"
```

### Packet Capture Analysis

```bash
# Capture QUIC traffic
sudo tcpdump -i any -w spooky.pcap udp port 443

# Analyze with tshark
tshark -r spooky.pcap -Y quic -T fields \
  -e frame.time -e ip.src -e ip.dst -e quic.header_form

# Decrypt QUIC (requires SSLKEYLOGFILE)
SSLKEYLOGFILE=/tmp/keys.log curl --http3 https://localhost:443/
tshark -r spooky.pcap -o tls.keylog_file:/tmp/keys.log -Y http3
```

### Performance Profiling

```bash
# CPU profiling with perf
sudo perf record -F 99 -p $(pgrep spooky) -g -- sleep 30
sudo perf report --stdio | head -50

# Flamegraph generation
sudo perf record -F 99 -p $(pgrep spooky) -g -- sleep 30
sudo perf script | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg

# Memory profiling (if built with jemalloc)
export MALLOC_CONF=prof:true,prof_prefix:/tmp/jeprof
sudo systemctl restart spooky
# Send traffic, then analyze with jeprof
```

## Common Error Reference

| Error Message | HTTP Status | Cause | Resolution |
|--------------|-------------|-------|------------|
| `invalid request` | 400 | Malformed HTTP/3 headers | Check client request format |
| `no servers configured for upstream` | 503 | Empty backend list | Add backends to upstream config |
| `no healthy servers` | 503 | All backends unhealthy | Check backend health, adjust thresholds |
| `invalid server` | 503 | Backend index out of bounds | Configuration reload race condition |
| `upstream error` | 502 | Backend connection failed | Verify backend connectivity |
| `upstream timeout` | 503 | Backend exceeded 2s timeout | Increase BACKEND_TIMEOUT or optimize backend |
| `internal server error` | 500 | TLS configuration error | Check certificate/key files |
| `Wrong QUIC HEADER` | (dropped) | Malformed QUIC packet | Check for network corruption |
| `No route found for path` | (internal) | No matching upstream route | Add route configuration |
| `Upstream pool not found` | (internal) | Pool initialization failure | Check logs for startup errors |

## Support and Escalation

When reporting issues, include:

```bash
# 1. Version information
spooky --version

# 2. Configuration (sanitized)
yq eval 'del(.listen.tls.key, .listen.tls.cert)' config.yaml

# 3. System information
uname -a
cat /etc/os-release

# 4. Error logs (last 100 lines)
journalctl -u spooky --no-pager -n 100 --since "1 hour ago"

# 5. Resource utilization
ps aux | grep spooky
ss -u | grep -c :443
free -h

# 6. Network diagnostics
ss -ulnp | grep spooky
sudo iptables -L -n -v | grep 443
```

For production incidents, capture diagnostic bundle:

```bash
#!/bin/bash
mkdir -p spooky-diagnostics
cd spooky-diagnostics

spooky --version > version.txt
uname -a > system.txt
journalctl -u spooky --no-pager -n 500 > logs.txt
yq eval 'del(.listen.tls.key, .listen.tls.cert)' ../config.yaml > config.yaml
ps aux | grep spooky > processes.txt
ss -tulnp > sockets.txt
free -h > memory.txt
sudo tcpdump -i any -c 100 -w capture.pcap udp port 443

cd ..
tar czf spooky-diagnostics-$(date +%Y%m%d-%H%M%S).tar.gz spooky-diagnostics/
```