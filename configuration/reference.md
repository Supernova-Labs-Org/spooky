# Configuration Reference

Complete reference for all Spooky configuration options.

## Configuration File Format

Spooky uses YAML for configuration. Specify the configuration file using the `--config` flag:

```bash
spooky --config /path/to/config.yaml
```

## Complete Configuration Example

```yaml
version: 1

listen:
  protocol: http3
  address: "0.0.0.0"
  port: 9889
  tls:
    cert: "/etc/spooky/certs/fullchain.pem"
    key: "/etc/spooky/certs/privkey.pem"

upstream:
  api_pool:
    load_balancing:
      type: "consistent-hash"
      # key: "header:x-user-id"  # Planned feature, not currently supported

    route:
      host: "api.example.com"
      path_prefix: "/api"

    backends:
      - id: "api-01"
        address: "10.0.1.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
          timeout_ms: 2000
          failure_threshold: 3
          success_threshold: 2
          cooldown_ms: 5000

      - id: "api-02"
        address: "10.0.1.11:8080"
        weight: 150
        health_check:
          path: "/health"
          interval: 5000

  default_pool:
    load_balancing:
      type: "round-robin"

    route:
      path_prefix: "/"

    backends:
      - id: "web-01"
        address: "10.0.2.10:8080"
        weight: 100
        health_check:
          path: "/status"
          interval: 10000

log:
  level: info
```

## Top-Level Configuration

### version

Configuration schema version.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `version` | integer | No | 1 | Configuration schema version |

### listen

Server listening configuration. Defines the protocol, address, and port for incoming client connections.

### upstream

Named upstream pool definitions. Each key represents a unique upstream pool with its own routing rules, load balancing strategy, and backend servers.

### load_balancing

**Deprecated.** This top-level field is accepted by the parser for backward compatibility but has no effect at runtime. Configure load balancing strategy per upstream pool via `upstream.<name>.load_balancing` instead.

### log

Logging configuration. Controls log level and output formatting.

## Default Values

The following table lists all default configuration values used when properties are not explicitly specified:

| Property | Default Value | Description |
|----------|---------------|-------------|
| `version` | `1` | Configuration format version |
| `listen.protocol` | `"http3"` | Network protocol |
| `listen.port` | `9889` | Listening port |
| `listen.address` | `"0.0.0.0"` | Listening address |
| `listen.tls.cert_file` | Required | TLS certificate file path |
| `listen.tls.key_file` | Required | TLS private key file path |
| `upstream[].route.path_prefix` | `"/"` | Path prefix for routing |
| `upstream[].backends[].weight` | `100` | Backend weight for load balancing |
| `upstream[].backends[].health_check.path` | `"/health"` | Health check endpoint |
| `upstream[].backends[].health_check.interval` | `5000` | Health check interval (ms) |
| `upstream[].backends[].health_check.timeout_ms` | `1000` | Health check timeout (ms) |
| `upstream[].backends[].health_check.failure_threshold` | `3` | Failures to mark unhealthy |
| `upstream[].backends[].health_check.success_threshold` | `2` | Successes to mark healthy |
| `upstream[].backends[].health_check.cooldown_ms` | `5000` | Cooldown after failure (ms) |
| `upstream[].load_balancing.type` | `"round-robin"` | Per-upstream load balancing algorithm |
| `log.level` | `"info"` | Logging verbosity level |
| `log.file.enabled` | `false` | Write logs to file instead of stderr |
| `log.file.path` | `"/var/log/spooky/spooky.log"` | Log file path (used when `log.file.enabled` is true) |

## Listen Configuration

Configures the listening interface for incoming client connections. HTTP/3 requires TLS configuration.

### Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `protocol` | string | No | `http3` | Protocol to listen on |
| `address` | string | No | `0.0.0.0` | IP address to bind to |
| `port` | integer | No | `9889` | Port to bind to |
| `tls` | object | Yes | - | TLS configuration (required for HTTP/3) |

### Protocol Values

- `http3`: HTTP/3 over QUIC (recommended)

### TLS Configuration

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `cert` | string | Yes | Path to TLS certificate file (PEM format) |
| `key` | string | Yes | Path to TLS private key file (PEM format, PKCS#8 recommended) |

### Examples

```yaml
# Standard HTTP/3 configuration
listen:
  protocol: http3
  address: "0.0.0.0"
  port: 9889
  tls:
    cert: "/etc/spooky/certs/server.crt"
    key: "/etc/spooky/certs/server.key"

# Localhost-only development
listen:
  protocol: http3
  address: "127.0.0.1"
  port: 9889
  tls:
    cert: "certs/localhost.crt"
    key: "certs/localhost.key"
```

## Upstream Configuration

Upstream pools define groups of backend servers with routing rules and load balancing strategies. Each upstream pool is identified by a unique name and contains routing criteria, load balancing configuration, and backend definitions.

### Structure

```yaml
upstream:
  pool_name:
    load_balancing: <LoadBalancing>
    route: <RouteMatch>
    backends: [<Backend>]
```

### Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `load_balancing` | object | No | round-robin | Per-upstream load balancing algorithm configuration |
| `route` | object | Yes | - | Route matching criteria |
| `backends` | array | Yes | - | List of backend servers |

### Route Matching

Route matching determines which upstream pool handles a request. Routes are evaluated by longest-prefix matching across all configured upstreams, selecting the route with the most specific (longest) path prefix.

#### RouteMatch Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `host` | string | No | - | Host header to match (e.g., `api.example.com`) |
| `path_prefix` | string | No | - | Path prefix to match (e.g., `/api`) |
| `method` | string | No | - | HTTP method to match (reserved for future use) |

Route matching rules:

1. If `host` is specified, the request Host header must match exactly
2. If `path_prefix` is specified, the request path must start with the prefix
3. If both are specified, both conditions must match
4. Routes are evaluated by longest-prefix matching - the route with the most specific (longest) path prefix is selected
5. For routes with equal-length prefixes, selection depends on HashMap iteration order (not deterministic by configuration order)

#### Route Examples

```yaml
# Host-based routing
upstream:
  api_pool:
    route:
      host: "api.example.com"
    backends: [...]

  web_pool:
    route:
      host: "www.example.com"
    backends: [...]

# Path-based routing
upstream:
  api_pool:
    route:
      path_prefix: "/api"
    backends: [...]

  admin_pool:
    route:
      path_prefix: "/admin"
    backends: [...]

  default_pool:
    route:
      path_prefix: "/"
    backends: [...]

# Combined host and path routing
upstream:
  api_v2_pool:
    route:
      host: "api.example.com"
      path_prefix: "/v2"
    backends: [...]

  api_v1_pool:
    route:
      host: "api.example.com"
      path_prefix: "/v1"
    backends: [...]
```

### Backend Configuration

Each backend represents an upstream server that can handle requests.

#### Backend Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `id` | string | Yes | - | Unique identifier for the backend |
| `address` | string | Yes | - | Backend server address in `host:port` format |
| `weight` | integer | No | `100` | Load balancing weight (higher values receive more traffic) |
| `health_check` | object | Yes | - | Health check configuration |

#### Health Check Configuration

Health checks monitor backend availability and automatically remove unhealthy backends from the pool.

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `path` | string | No | `/health` | HTTP path for health check requests |
| `interval` | integer | No | `5000` | Health check interval in milliseconds |
| `timeout_ms` | integer | No | `1000` | Health check timeout in milliseconds |
| `failure_threshold` | integer | No | `3` | Consecutive failures before marking unhealthy |
| `success_threshold` | integer | No | `2` | Consecutive successes before marking healthy |
| `cooldown_ms` | integer | No | `5000` | Cooldown period after marking unhealthy (milliseconds) |

Health check behavior:

1. Health checks are performed at the specified `interval`
2. A backend is marked unhealthy after `failure_threshold` consecutive failures
3. An unhealthy backend enters cooldown for `cooldown_ms` milliseconds
4. After cooldown, health checks resume
5. A backend is marked healthy after `success_threshold` consecutive successes

#### Backend Examples

```yaml
# Minimal backend configuration
backends:
  - id: "backend1"
    address: "10.0.1.10:8080"
    health_check:
      path: "/health"

# Weighted backend with custom health checks
backends:
  - id: "backend1"
    address: "10.0.1.10:8080"
    weight: 100
    health_check:
      path: "/api/health"
      interval: 10000
      timeout_ms: 2000
      failure_threshold: 5
      success_threshold: 3
      cooldown_ms: 10000

  - id: "backend2"
    address: "10.0.1.11:8080"
    weight: 200
    health_check:
      path: "/api/health"
      interval: 10000

# Multiple backends with different health endpoints
backends:
  - id: "primary"
    address: "10.0.1.10:8080"
    weight: 150
    health_check:
      path: "/status"
      interval: 5000

  - id: "secondary"
    address: "10.0.1.11:8080"
    weight: 100
    health_check:
      path: "/healthz"
      interval: 5000
```

## Load Balancing Configuration

Load balancing determines how requests are distributed across healthy backends within an upstream pool. Each pool configures its own strategy independently.

### Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `type` | string | Yes | - | Load balancing algorithm |
| `key` | string | No | - | Hash key source for consistent hashing (planned feature) |

### Supported Algorithms

#### random

Selects a backend randomly from all healthy backends. Weight values are currently ignored (weighted random is planned for future release).

```yaml
upstream:
  my_pool:
    load_balancing:
      type: "random"
```

#### round-robin

Distributes requests evenly across all healthy backends in sequential order. Weight values are currently ignored (weighted round-robin is planned for future release).

```yaml
upstream:
  my_pool:
    load_balancing:
      type: "round-robin"
```

#### consistent-hash

Routes requests using consistent hashing based on a fixed key derived from the request. Currently uses request authority (if present), otherwise request path, otherwise HTTP method.

**Note**: Configurable key sources (headers, cookies, query parameters) are planned for future implementation.

```yaml
upstream:
  my_pool:
    load_balancing:
      type: "consistent-hash"
      # key: "header:x-user-id"  # Planned feature, not currently supported
```

### Algorithm Selection

- Use `random` for simple stateless load distribution
- Use `round-robin` for even distribution across backends
- Use `consistent-hash` when session affinity or request consistency is required

### Examples

```yaml
upstream:
  api_pool:
    load_balancing:
      type: "consistent-hash"
    route:
      path_prefix: "/api"
    backends: [...]

  default_pool:
    load_balancing:
      type: "round-robin"
    route:
      path_prefix: "/"
    backends: [...]
```

## Logging Configuration

Controls logging output, verbosity, and destination.

### Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `level` | string | No | `info` | Log level |
| `file.enabled` | bool | No | `false` | Write logs to a file instead of stderr |
| `file.path` | string | No | `/var/log/spooky/spooky.log` | Log file path (used when `file.enabled` is `true`) |

### Log Levels

Log levels in order of increasing verbosity:

- `silence`: No logging output
- `poltergeist`: Error messages only
- `scream`: Warnings and errors
- `spooky`: Informational messages, warnings, and errors
- `haunt`: Debug information
- `whisper`: Trace-level debugging

Standard log level mapping:

- `silence` = off
- `poltergeist` = error
- `scream` = warn
- `spooky` = info
- `haunt` = debug
- `whisper` = trace

### Examples

```yaml
# stderr only (default)
log:
  level: info

# Write to file
log:
  level: info
  file:
    enabled: true
    path: /var/log/spooky/spooky.log

# Development — debug to stderr
log:
  level: haunt  # debug level

# Troubleshooting — trace to file
log:
  level: whisper  # trace level
  file:
    enabled: true
    path: /tmp/spooky-trace.log
```

## Configuration Validation

Spooky validates configuration at startup and reports errors before attempting to start the server.

### Common Validation Errors

1. **Missing required fields**
   - TLS certificate or key paths not specified
   - Backend address or ID missing
   - Route configuration empty

2. **Invalid file paths**
   - TLS certificate file not found or not readable
   - TLS key file not found or not readable
   - Incorrect file permissions

3. **Invalid values**
   - Port number out of range (1-65535)
   - Invalid IP address format
   - Invalid backend address format (must be `host:port`)
   - Duplicate backend IDs within a pool

4. **Configuration conflicts**
   - Port already in use
   - Duplicate upstream pool names
   - Overlapping or ambiguous route definitions

### Testing Configuration

Validate configuration without starting the server:

```bash
spooky --config <path>
```

The command exits with status 0 if configuration is valid, or prints detailed error messages and exits with non-zero status if invalid.

## Complete Working Example

```yaml
version: 1

listen:
  protocol: http3
  address: "0.0.0.0"
  port: 9889
  tls:
    cert: "certs/proxy-fullchain.pem"
    key: "certs/proxy-key-pkcs8.pem"

upstream:
  api_pool:
    load_balancing:
      type: "consistent-hash"

    route:
      path_prefix: "/api"

    backends:
      - id: "backend1"
        address: "127.0.0.1:7001"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

      - id: "backend2"
        address: "127.0.0.1:7002"
        weight: 50
        health_check:
          path: "/status"
          interval: 10000

  default_pool:
    load_balancing:
      type: "round-robin"

    route:
      path_prefix: "/"

    backends:
      - id: "auth1"
        address: "127.0.0.1:8001"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

log:
  level: debug
```