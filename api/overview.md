# API Reference

Spooky's programmatic interfaces and configuration APIs.

## Command Line Interface

### Basic Usage

```bash
spooky --config <CONFIG>
```

### Options

| Option | Short | Type | Required | Description |
|--------|-------|------|----------|-------------|
| `--config` | `-c` | string | Yes | Path to configuration file |
| `--version` | `-V` | boolean | No | Show version information |
| `--help` | `-h` | boolean | No | Show help information |

### Examples

```bash
# Start Spooky with configuration
spooky --config /etc/spooky/config.yaml

# Show version
spooky --version
```

## Configuration API

### Configuration File Format

Spooky uses YAML for configuration with the following structure:

```yaml
# Top-level configuration schema
version: 1                    # Configuration format version
listen:                       # Listener configuration (required)
upstream:                     # Named upstream pools (required)
load_balancing?:              # Global load balancing (optional)
log:                          # Logging configuration (optional, defaults applied)
```

### Type Definitions

#### ListenConfig

```typescript
interface ListenConfig {
  protocol: "http3";     // Only HTTP/3 is supported
  port: number;
  address: string;
  tls: TLSConfig;        // TLS is required for HTTP/3
}
```

#### TLSConfig

```typescript
interface TLSConfig {
  cert: string;        // Path to certificate file
  key: string;         // Path to private key file
  ca?: string;         // Path to CA certificate (client auth)
}
```

#### UpstreamConfig

```typescript
interface UpstreamConfig {
  route: RouteConfig;             // Routing rules (required)
  load_balancing?: LoadBalancingConfig;  // Per-upstream LB (planned, not implemented)
  backends: BackendConfig[];      // Backend servers (required, at least 1)
}
```

#### RouteConfig

```typescript
interface RouteConfig {
  host?: string;         // Host header to match (optional)
  path_prefix?: string;  // Path prefix to match (optional, but at least one of host/path_prefix required)
}
```

#### BackendConfig

```typescript
interface BackendConfig {
  id: string;          // Unique backend identifier
  address: string;     // Backend address (host:port)
  weight?: number;     // Load balancing weight (default: 100)
  health_check?: HealthCheckConfig;
}
```

#### HealthCheckConfig

```typescript
interface HealthCheckConfig {
  path?: string;           // Health check endpoint (default: "/health")
  interval?: number;       // Check interval in ms (default: 5000)
  timeout_ms?: number;     // Request timeout in ms (default: 1000)
  success_threshold?: number;   // Successes to mark healthy (default: 2)
  failure_threshold?: number; // Failures to mark unhealthy (default: 3)
  method?: string;         // HTTP method (default: "GET")
}
```

#### LoadBalancingConfig

```typescript
interface LoadBalancingConfig {
  type: "random" | "round-robin" | "consistent-hash";
  key?: string;  // Hash key source (planned: header:name, cookie:name, query:name, path)
}
```


#### LogConfig

```typescript
interface LogConfig {
  level?: string;  // Log level (default: "info")
}
```

Supported log levels (in order of verbosity):
- `whisper` - Trace-level logging (most verbose)
- `haunt` - Debug-level logging
- `spooky` - Info-level logging (default)
- `scream` - Warning-level logging
- `poltergeist` - Error-level logging
- `silence` - Logging disabled

Standard log levels are also supported:
- `trace`, `debug`, `info`, `warn`, `error`, `off`

## Metrics System

Spooky maintains internal performance and operational metrics tracked via atomic counters.

### Current Available Metrics

The following metrics are currently tracked in-memory within the `Metrics` structure:

#### Request Metrics

- `requests_total` (AtomicU64) - Total number of requests received and processed
- `requests_success` (AtomicU64) - Number of requests completed successfully with 2xx responses
- `requests_failure` (AtomicU64) - Number of requests that failed or returned error responses

#### Backend Metrics

- `backend_timeouts` (AtomicU64) - Number of requests that timed out waiting for backend response
- `backend_errors` (AtomicU64) - Number of backend connection or communication errors

### Metrics Implementation Details

All metrics use atomic operations with relaxed ordering for high-performance lock-free increment operations. Metrics are incremented through dedicated methods:

- `inc_total()` - Increment total request counter
- `inc_success()` - Increment successful request counter
- `inc_failure()` - Increment failed request counter
- `inc_timeout()` - Increment backend timeout counter
- `inc_backend_error()` - Increment backend error counter

### Future: Metrics API Endpoint

**Status**: Planned feature

A metrics exposition endpoint is planned for future implementation that will expose collected metrics for monitoring and observability systems.

#### Planned Endpoint

```
GET /metrics
```

The endpoint will provide Prometheus-compatible metric exposition format, making Spooky metrics accessible to standard monitoring and alerting infrastructure.

#### Planned Metric Categories

Future implementations may include:

- **Request metrics**: Total requests, success/failure rates, request duration histograms
- **Connection metrics**: Active QUIC connections, HTTP/2 connection pool statistics
- **Backend health metrics**: Backend availability, health check results, response times
- **Load balancing metrics**: Backend selection distribution, algorithm performance
- **System metrics**: Process resource usage, runtime statistics

### Admin API

**Status**: Future capability

Administrative API endpoints are planned for runtime management and observability:

#### Planned Admin Capabilities

- **Health endpoint**: Spooky instance health status and backend health aggregation
- **Metrics endpoint**: Real-time operational metrics exposition
- **Configuration reload**: Dynamic configuration updates without restart
- **Connection management**: View active connections, drain connections gracefully
- **Backend management**: Enable/disable backends, adjust weights dynamically

## Health Check API

### Backend Health Checks

Spooky performs HTTP health checks against configured backends.

#### Request Format

```http
GET /health HTTP/1.1
Host: backend.example.com:8080
User-Agent: spooky/0.1.0
```

#### Expected Response

**Healthy Response** (2xx status code):
```http
HTTP/1.1 200 OK
Content-Type: application/json

{"status": "healthy", "timestamp": "2024-01-01T12:00:00Z"}
```

**Unhealthy Response** (non-2xx status code):
```http
HTTP/1.1 503 Service Unavailable
Content-Type: application/json

{"status": "unhealthy", "reason": "database connection failed"}
```

### Spooky Health Endpoint

**Status**: Future feature

A dedicated health endpoint for the Spooky instance is planned for future implementation:

```http
GET /health HTTP/1.1
```

Planned response format:
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "uptime": 3600,
  "backends": {
    "web-01": "healthy",
    "web-02": "healthy",
    "api-01": "unhealthy"
  }
}
```

## Configuration Validation

### Startup Validation

Configuration validation is performed automatically at startup before the QUIC listener is initialized. The validation process verifies:

- Configuration file format and syntax
- Required field presence
- Value type correctness
- File path existence (certificates, keys)
- Network address format validity

### Exit Codes

- `0`: Configuration validated successfully, normal operation
- `1`: Configuration validation failed or runtime error occurred

### Validation Output

**Valid Configuration**:
```
Configuration validation successful
Spooky is starting
Listening on 0.0.0.0:9889
```

**Invalid Configuration**:
```
Error loading config: <error details>
```

or

```
Configuration validation failed. Exiting...
```

## Error Codes

### HTTP Status Codes

Spooky may return the following HTTP status codes to clients:

- `200 OK`: Request successful (forwarded from backend)
- `400 Bad Request`: Malformed or invalid request
- `500 Internal Server Error`: Internal proxy error (e.g., TLS configuration issues)
- `502 Bad Gateway`: Backend server error
- `503 Service Unavailable`: Backend timeout or no healthy backends available

## Logging

### Log Format

Spooky uses the `env_logger` logging implementation with timestamped output. All log messages are written to standard output (stdout) with the following format:

```
[YYYY-MM-DD HH:MM:SS] [LEVEL] [module::path] message
```

### Log Output Examples

```
[2026-02-18 14:23:45] [INFO] [spooky] Spooky is starting
[2026-02-18 14:23:45] [DEBUG] [spooky_edge::quic_listener] Listening on 0.0.0.0:9889
[2026-02-18 14:23:45] [DEBUG] [spooky_edge::quic_listener] Certificate loaded successfully
[2026-02-18 14:23:50] [INFO] [spooky_edge::quic_listener] Length of data received: 1200
[2026-02-18 14:23:50] [DEBUG] [spooky_edge::quic_listener] Packet DCID (len=8): [00 01 02 03 04 05 06 07], type: Initial, active connections: 1
[2026-02-18 14:25:30] [INFO] [spooky_edge::quic_listener] Draining connections
[2026-02-18 14:25:35] [INFO] [spooky] Spooky shutdown complete
```

### Log Levels

Log verbosity is configured via the `log.level` configuration parameter. The following levels are available (ordered from most to least verbose):

| Level | Standard Equivalent | Use Case |
|-------|-------------------|----------|
| `whisper` | trace | Extremely detailed diagnostic information including packet hex dumps |
| `haunt` | debug | Detailed diagnostic information for troubleshooting |
| `spooky` | info | General informational messages about normal operation |
| `scream` | warn | Warning messages for potentially problematic situations |
| `poltergeist` | error | Error messages for failures and exceptions |
| `silence` | off | Disable all logging output |

Standard log level names (`trace`, `debug`, `info`, `warn`, `error`, `off`) are also supported for compatibility.

### Log Configuration

Configure logging in the configuration file:

```yaml
log:
  level: "spooky"  # or "haunt", "whisper", etc.
```

### Environment Variable Control

The `env_logger` implementation respects the `RUST_LOG` environment variable, which can be used to override configuration file settings or enable module-specific logging:

```bash
# Override global log level
RUST_LOG=debug spooky --config config.yaml

# Enable debug logging for specific modules
RUST_LOG=spooky_edge=debug,spooky_transport=info spooky --config config.yaml

# Trace all modules
RUST_LOG=trace spooky --config config.yaml
```

## Environment Variables

**Note**: Environment variable interpolation in configuration files is not currently supported. Configuration values must be provided literally in the YAML file.

For dynamic configuration, consider using external configuration management tools or templating the configuration file before loading.