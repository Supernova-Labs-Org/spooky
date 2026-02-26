# Load Balancing

Comprehensive guide to load balancing algorithms, health checking, and backend management in Spooky.

## Load Balancing Algorithms

Spooky implements three load balancing algorithms, each optimized for different use cases. Each upstream pool configures its own algorithm independently via `load_balancing.type`.

### Round Robin

**Algorithm**: Sequential distribution across healthy backends in a circular pattern.

**Configuration**:
```yaml
upstream:
  api_pool:
    load_balancing:
      type: "round-robin"  # Accepts: round-robin, round_robin, rr
    route:
      path_prefix: "/api"
    backends:
      - id: "backend1"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
      - id: "backend2"
        address: "10.0.1.11:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
```

**Characteristics**:
- Sequential, predictable distribution pattern
- State maintained across requests (counter increments per request)
- Equal distribution when all backends have equal weight
- Automatically skips unhealthy backends
- Counter wraps on overflow (no reset on restart)

**Use Cases**:
- Stateless applications requiring even distribution
- Backends with equal capacity and performance
- Scenarios where predictable patterns are acceptable
- General purpose load balancing

**Performance**: Very low overhead (simple counter increment)

### Consistent Hashing

**Algorithm**: Hash-based routing using a consistent hash ring with virtual nodes.

**Configuration**:
```yaml
upstream:
  api_pool:
    load_balancing:
      type: "consistent-hash"  # Accepts: consistent-hash, consistent_hash, ch
      # key: "header:x-user-id"  # Planned feature: configurable hash key source
    route:
      path_prefix: "/api"
    backends:
      - id: "backend1"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
      - id: "backend2"
        address: "10.0.1.11:8080"
        weight: 200  # 2x virtual nodes = 2x traffic share
        health_check:
          path: "/health"
          interval: 5000
```

**Characteristics**:
- Deterministic routing based on hash key
- Same key always routes to same backend (session affinity)
- Uses 64 virtual replicas per backend per weight unit
- Minimal request redistribution when backends change
- FNV-1a hash function for distribution
- Automatically skips unhealthy backends

**Hash Key Sources**:

Currently, the key parameter is configured but hash key extraction must be implemented in the proxy layer. The algorithm accepts any string key.

```yaml
# Current behavior: fixed key derivation from request
load_balancing:
  type: "consistent-hash"
# Planned configurable key sources (not currently implemented):
# key: "header:x-user-id"       # User ID from header
# key: "header:x-session-id"    # Session ID from header
# key: "cookie:session_id"      # Session cookie
# key: "query:user_id"          # Query parameter
# key: "path"                   # Request path
```

**Use Cases**:
- Applications requiring session affinity
- Cache locality optimization (same keys hit same backend caches)
- Stateful applications without external session store
- Minimizing cache misses during backend changes

**Performance**: Low overhead (hash computation + BTreeMap lookup)

### Random

**Algorithm**: Random selection from healthy backends.

**Configuration**:
```yaml
upstream:
  api_pool:
    load_balancing:
      type: "random"
    route:
      path_prefix: "/api"
    backends:
      - id: "backend1"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
      - id: "backend2"
        address: "10.0.1.11:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
```

**Characteristics**:
- Non-deterministic selection using thread-local RNG
- No state maintained between requests
- Statistically even distribution over time
- No session affinity
- Automatically skips unhealthy backends

**Use Cases**:
- Stateless applications
- High-throughput scenarios where simplicity matters
- Testing and development
- Avoiding predictable patterns for security

**Performance**: Very low overhead (random number generation)

## Algorithm Comparison

| Algorithm | Complexity | Session Affinity | State | Distribution | Use Case |
|-----------|-----------|------------------|-------|--------------|----------|
| Round Robin | O(1) | No | Counter | Even | General purpose, predictable load |
| Consistent Hash | O(log n) | Yes | Hash ring | Even (with weight) | Session affinity, cache locality |
| Random | O(1) | No | None | Statistically even | Stateless, high throughput |

## Backend Weighting

Only consistent hashing respects backend weights. Round-robin and random algorithms currently ignore weights (weighted versions are planned for future release).

### Weight Configuration

```yaml
backends:
  - id: "small-instance"
    address: "10.0.1.10:8080"
    weight: 50      # Receives 1x traffic
    health_check:
      path: "/health"
      interval: 5000

  - id: "medium-instance"
    address: "10.0.1.11:8080"
    weight: 100     # Receives 2x traffic (2x the first)
    health_check:
      path: "/health"
      interval: 5000

  - id: "large-instance"
    address: "10.0.1.12:8080"
    weight: 200     # Receives 4x traffic (4x the first)
    health_check:
      path: "/health"
      interval: 5000
```

**Weight Behavior**:
- **Round Robin**: Weight values are currently ignored (weighted round-robin planned for future release)
- **Consistent Hash**: Number of virtual nodes = replicas × weight (64 replicas per weight unit)
- **Random**: Weight values are currently ignored (weighted random planned for future release)
- **Minimum**: Weight values below 1 are clamped to 1

## Health Checking

Spooky performs active health checks on all backends. Unhealthy backends are automatically removed from rotation.

### Health Check Mechanism

```yaml
backends:
  - id: "backend1"
    address: "10.0.1.10:8080"
    weight: 100
    health_check:
      path: "/health"              # Health check endpoint path
      interval: 5000               # Check every 5 seconds
      timeout_ms: 2000             # 2 second timeout per check
      failure_threshold: 3         # 3 failures → mark unhealthy
      success_threshold: 2         # 2 successes → mark healthy
      cooldown_ms: 10000           # 10 second cooldown before recovery
```

### Health Check Parameters

**path**: Endpoint to check (default: "/health")
- Must return 2xx status code when healthy
- Should be lightweight and fast

**interval**: Time between checks in milliseconds (default: 5000)
- Lower values = faster failure detection
- Higher values = lower overhead

**timeout_ms**: Maximum wait time for response (default: 1000)
- Should be less than interval
- Failed on timeout

**failure_threshold**: Consecutive failures to mark unhealthy (default: 3)
- Higher values = more tolerance for transient failures
- Lower values = faster failure detection

**success_threshold**: Consecutive successes to mark healthy (default: 2)
- Higher values = more confidence before recovery
- Lower values = faster recovery

**cooldown_ms**: Minimum time to stay unhealthy (default: 5000)
- Prevents flapping
- Gives backend time to recover

### Health State Machine

```
Initial State: Healthy
              |
              | failure_threshold consecutive failures
              v
         Unhealthy (cooldown period)
              |
              | cooldown expires
              v
         Unhealthy (testing)
              |
              | success_threshold consecutive successes
              v
           Healthy
```

**Healthy**: Backend receives traffic, failures are counted

**Unhealthy (cooldown)**: Backend removed from rotation, health checks continue but successes are ignored until cooldown expires

**Unhealthy (testing)**: Backend still removed from rotation, consecutive successes are counted toward success_threshold

### Backend State Tracking

The load balancer tracks per-backend state:

```rust
struct BackendState {
    address: String,
    weight: u32,
    health_check: HealthCheck,
    consecutive_failures: u32,
    health_state: HealthState,  // Healthy | Unhealthy { until, successes }
}
```

State transitions:
- **record_success()**: Increments success counter, transitions to Healthy if threshold met
- **record_failure()**: Increments failure counter, transitions to Unhealthy if threshold met

## Multiple Upstream Pools

Spooky supports multiple upstream pools with independent routing and load balancing configuration. Each pool specifies its own algorithm.

### Configuration Example

```yaml
upstream:
  api_pool:
    load_balancing:
      type: "consistent-hash"  # Session affinity for API requests
    route:
      path_prefix: "/api"
    backends:
      - id: "api1"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
      - id: "api2"
        address: "10.0.1.11:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

  auth_pool:
    load_balancing:
      type: "round-robin"  # Even spread across auth backends
    route:
      path_prefix: "/auth"
    backends:
      - id: "auth1"
        address: "10.0.2.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

  static_pool:
    load_balancing:
      type: "random"  # Stateless static assets, any backend is fine
    route:
      path_prefix: "/static"
    backends:
      - id: "cdn1"
        address: "10.0.3.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 10000
```

**Route Matching**:
- Routes are evaluated by longest-prefix matching
- Route with most specific (longest) path prefix wins
- For equal-length prefixes, selection depends on HashMap iteration order
- **Important**: Unmatched routes return an error - configure a catch-all upstream (e.g., `path_prefix: "/"`) to handle all other requests

## Monitoring and Observability

### Logging

Health check events are logged:

```
[INFO] Backend backend1 health check passed
[WARN] Backend backend2 health check failed: connection timeout
[INFO] Backend backend2 marked unhealthy after 3 consecutive failures
[INFO] Backend backend2 marked healthy after 2 consecutive successes
[DEBUG] Routing request to backend backend1 (round-robin)
[DEBUG] Routing request to backend backend2 (consistent-hash, key=user:123)
```

### Health Check Monitoring

```bash
# Monitor health check activity (systemd)
sudo journalctl -u spooky.service -f | grep -i health

# Monitor health check activity (direct process)
spooky --config config.yaml 2>&1 | grep -i health

# Count backend state changes (systemd)
sudo journalctl -u spooky.service | grep "marked unhealthy\|marked healthy" | tail -20

# Count backend state changes (if redirected to file)
grep "marked unhealthy\|marked healthy" /var/log/spooky/spooky.log | tail -20

# Track specific backend (systemd)
sudo journalctl -u spooky.service | grep "backend1" | grep health

# Track specific backend (if redirected to file)
grep "backend1" /var/log/spooky/spooky.log | grep health
```

### Load Distribution Analysis

```bash
# Extract backend selection counts (systemd)
sudo journalctl -u spooky.service | grep "routing to backend" | \
  awk '{print $NF}' | sort | uniq -c | sort -rn

# Monitor routing decisions in real-time (systemd)
sudo journalctl -u spooky.service -f | grep "routing to backend"

# If redirected to file
grep "routing to backend" /var/log/spooky/spooky.log | \
  awk '{print $NF}' | sort | uniq -c | sort -rn
```

## Performance Considerations

### Algorithm Performance

| Algorithm | Time Complexity | Memory per Backend | Per-Request Cost |
|-----------|----------------|-------------------|------------------|
| Round Robin | O(1) | ~8 bytes | Counter increment |
| Consistent Hash | O(log n) | ~4 KB (64 replicas × weight) | Hash + BTreeMap lookup |
| Random | O(1) | ~0 bytes | RNG call |

**n** = number of healthy backends

### Backend Pool Operations

```rust
// All operations filter to healthy backends first
healthy_indices()              // O(n) - scans all backends
pick_backend(algorithm)        // O(1) or O(log n) depending on algorithm
mark_success(index)            // O(1) - direct index access
mark_failure(index)            // O(1) - direct index access
```

### Scalability

- **Backend Count**: Algorithms scale to hundreds of backends
- **Health Checks**: Run asynchronously, do not block request path
- **Memory**: ~8-12 KB per backend (including health state and hash ring)
- **CPU**: Minimal overhead for all algorithms

## Troubleshooting

### Uneven Load Distribution

**Symptoms**: Some backends receive disproportionate traffic

**Diagnosis**:
```bash
# Check backend weights
grep -A 10 "backends:" config.yaml | grep -E "id|weight"

# Monitor actual distribution (systemd)
sudo journalctl -u spooky.service | grep "routing to backend" | \
  awk '{print $NF}' | sort | uniq -c

# If redirected to file
grep "routing to backend" /var/log/spooky/spooky.log | \
  awk '{print $NF}' | sort | uniq -c

# Verify all backends are healthy (systemd)
sudo journalctl -u spooky.service | grep "healthy" | tail -20

# If redirected to file
grep "healthy" /var/log/spooky/spooky.log | tail -20
```

**Solutions**:
- Verify backend weights are configured correctly
- Check for unhealthy backends (temporarily removed from rotation)
- For consistent-hash, verify hash keys are well-distributed
- For round-robin, ensure sufficient request volume for even distribution

### Session Affinity Not Working

**Symptoms**: Requests from same user/session hit different backends

**Diagnosis**:
```bash
# Verify consistent-hash configuration
grep -A 5 "load_balancing:" config.yaml | grep -E "type|key"

# Check if hash key is present in requests (systemd)
sudo journalctl -u spooky.service -f | grep "consistent-hash"

# Check if hash key is present in requests (if redirected to file)
tail -f /var/log/spooky/spooky.log | grep "consistent-hash"

# Test with known hash key
curl --http3-only -H "X-User-ID: test123" https://localhost:9889/
```

**Solutions**:
- Ensure load_balancing.type is "consistent-hash"
- Note: Hash key is automatically derived from request (authority → path → method)
- For session affinity, ensure requests include consistent authority or path components
- Configurable key sources are planned for future implementation

### Frequent Health Check Failures

**Symptoms**: Backends repeatedly marked unhealthy despite being functional

**Diagnosis**:
```bash
# Monitor health check failures (systemd)
sudo journalctl -u spooky.service -f | grep "health check failed"

# If redirected to file
tail -f /var/log/spooky/spooky.log | grep "health check failed"

# Test health endpoint directly
curl -v http://10.0.1.10:8080/health

# Check response time
time curl http://10.0.1.10:8080/health

# Verify network connectivity
ping 10.0.1.10
traceroute 10.0.1.10
```

**Solutions**:
- Increase health check timeout (timeout_ms) if endpoint is slow
- Increase failure_threshold to tolerate transient failures
- Optimize backend health endpoint performance
- Check network latency and packet loss
- Verify health check path is correct

### Backends Not Recovering

**Symptoms**: Healthy backends remain marked unhealthy

**Diagnosis**:
```bash
# Check cooldown period
grep -A 10 "health_check:" config.yaml | grep cooldown_ms

# Monitor recovery attempts (systemd)
sudo journalctl -u spooky.service -f | grep -E "marked healthy|success"

# If redirected to file
tail -f /var/log/spooky/spooky.log | grep -E "marked healthy|success"

# Verify backend is actually healthy
curl http://10.0.1.10:8080/health
```

**Solutions**:
- Reduce cooldown_ms for faster recovery
- Reduce success_threshold if being too conservative
- Verify backend health endpoint returns 2xx status
- Check for health check timeout issues

### No Backends Available

**Symptoms**: All backends marked unhealthy, requests fail

**Diagnosis**:
```bash
# List all backend states (systemd)
sudo journalctl -u spooky.service | grep -i "backend.*health" | tail -20

# If redirected to file
grep -i "backend.*health" /var/log/spooky/spooky.log | tail -20

# Check configuration
grep -A 15 "backends:" config.yaml

# Test each backend directly
for backend in 10.0.1.10:8080 10.0.1.11:8080; do
  echo "Testing $backend"
  curl -v http://$backend/health
done
```

**Solutions**:
- Fix backend health endpoints
- Adjust health check parameters (increase timeout, threshold)
- Verify backends are actually running and accessible
- Check firewall rules between Spooky and backends

## Best Practices

### Health Check Configuration

- Set timeout_ms < interval to prevent check pileup
- Use failure_threshold ≥ 3 to avoid false positives
- Set cooldown_ms ≥ 10000 to prevent flapping
- Keep health endpoints lightweight (< 100ms response time)

### Algorithm Selection

- **Use Round Robin** for simple, even distribution with equal backends
- **Use Consistent Hash** when session affinity or cache locality matters
- **Use Random** for stateless, high-throughput scenarios

### Weight Configuration

- Base weights on backend capacity (CPU, memory, network)
- Start with equal weights, adjust based on monitoring
- Use relative weights (100, 200, 400) rather than absolute
- For consistent-hash, remember: weight × 64 = number of virtual nodes

### Upstream Pool Design

- Create separate pools for different services (API, auth, static)
- Order routes from most specific to least specific
- Use path_prefix for path-based routing
- Use host for virtual host routing
- Provide a catch-all default pool

## Advanced Configuration Examples

### Multi-Tier Application

```yaml
# Global load balancing strategy (applies to all upstream pools)
# Per-upstream load_balancing is planned but not currently active
load_balancing:
  type: "round-robin"

upstream:
  api_tier:
    route:
      path_prefix: "/api"
    backends:
      - id: "api1"
        address: "10.0.1.10:8080"
        weight: 200
        health_check:
          path: "/health"
          interval: 5000
          timeout_ms: 2000
          failure_threshold: 3
          success_threshold: 2
          cooldown_ms: 15000

  auth_tier:
    route:
      path_prefix: "/auth"
    backends:
      - id: "auth1"
        address: "10.0.2.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 3000
          timeout_ms: 1000
          failure_threshold: 2
          success_threshold: 2
          cooldown_ms: 10000

  static_tier:
    route:
      path_prefix: "/static"
    backends:
      - id: "cdn1"
        address: "10.0.3.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 30000
          timeout_ms: 5000
          failure_threshold: 5
          success_threshold: 1
          cooldown_ms: 60000
```

### Heterogeneous Backend Capacities

```yaml
upstream:
  prod_pool:
    load_balancing:
      type: "round-robin"
    route:
      path_prefix: "/"
    backends:
      - id: "small-1"
        address: "10.0.1.10:8080"
        weight: 50       # 2 vCPU, 4 GB RAM
        health_check:
          path: "/health"
          interval: 5000

      - id: "medium-1"
        address: "10.0.1.11:8080"
        weight: 100      # 4 vCPU, 8 GB RAM
        health_check:
          path: "/health"
          interval: 5000

      - id: "large-1"
        address: "10.0.1.12:8080"
        weight: 200      # 8 vCPU, 16 GB RAM
        health_check:
          path: "/health"
          interval: 5000
```

## Related Documentation

- See [Basics](basics.md) for general configuration and deployment
- Refer to Configuration Reference for complete parameter documentation
- See Architecture Guide for internal implementation details
