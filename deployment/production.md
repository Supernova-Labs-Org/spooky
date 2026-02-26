# Production Deployment

> **Warning:** Spooky is experimental software. It is not production-ready. This guide documents deployment procedures for evaluation and staging environments. Do not use Spooky in production without thoroughly understanding its current limitations (see [Roadmap](../roadmap.md) for known issues).

This guide covers deployment procedures, system configuration, and operational considerations for Spooky HTTP/3 load balancer deployments.

## Pre-Deployment Checklist

### Infrastructure Requirements

**Compute Resources**
- CPU: 4 cores minimum (8+ for high-throughput deployments)
- Memory: 4GB minimum (8GB+ recommended, ~1-2KB per concurrent connection)
- Disk: 10GB minimum (configuration, logs, and binary storage)
- OS: Linux kernel 5.0+ (Ubuntu 20.04 LTS, RHEL 8+, or equivalent)

**Network Requirements**
- UDP ingress on designated QUIC port (typically 443)
- HTTP/2 egress to backend pool networks
- Low-latency connectivity between proxy tier and backends (<5ms RTT preferred)
- MTU considerations: 1500 byte minimum, jumbo frames (9000 bytes) beneficial for high-throughput scenarios

**Certificate Infrastructure**
- Valid TLS certificates with full chain
- Automated renewal mechanism (Let's Encrypt, internal PKI, or certificate management platform)
- Certificate rotation procedures documented and tested

### Pre-Deployment Validation

Before deploying to production, verify the following:

1. Configuration validated with `spooky --config <path>` (startup validation happens before serving)
2. Backend health check endpoints operational and returning expected responses
3. TLS certificates valid with appropriate SANs and expiration dates
4. Firewall rules permit required traffic flows
5. Service account and filesystem permissions configured
6. Monitoring and alerting infrastructure ready to receive metrics
7. Runbooks prepared for common failure scenarios

## System Configuration

### Binary Installation

Production deployments should use compiled release binaries:

```bash
# Download release binary
VERSION="0.1.0"
ARCH="x86_64"
wget "https://github.com/nishujangra/spooky/releases/download/v${VERSION}/spooky-linux-${ARCH}.tar.gz"
tar xzf "spooky-linux-${ARCH}.tar.gz"

# Verify checksum
sha256sum -c "spooky-linux-${ARCH}.tar.gz.sha256"

# Install to system path
sudo install -m 755 -o root -g root spooky /usr/local/bin/spooky

# Create dedicated service account
sudo useradd --system --shell /usr/sbin/nologin \
  --home-dir /var/lib/spooky --create-home spooky

# Initialize directory structure
sudo mkdir -p /etc/spooky/certs /var/log/spooky
sudo chown -R root:spooky /etc/spooky
sudo chmod 750 /etc/spooky
sudo chown spooky:spooky /var/log/spooky
sudo chmod 750 /var/log/spooky

# Note: Spooky logs to stdout/stderr by default (collected by journald).
# The /var/log/spooky directory is for optional file-based logging.
```

### Kernel Parameter Tuning

UDP and QUIC workloads benefit from increased buffer sizes and connection tracking limits:

```bash
# /etc/sysctl.d/99-spooky.conf
# UDP receive/send buffer tuning
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864
net.core.rmem_default = 16777216
net.core.wmem_default = 16777216

# Network device backlog
net.core.netdev_max_backlog = 65536
net.core.netdev_budget = 50000
net.core.netdev_budget_usecs = 5000

# Connection tracking (if using conntrack)
net.netfilter.nf_conntrack_max = 2097152
net.netfilter.nf_conntrack_tcp_timeout_established = 7200
net.netfilter.nf_conntrack_udp_timeout = 60
net.netfilter.nf_conntrack_udp_timeout_stream = 120

# TCP tuning for HTTP/2 backend connections
net.ipv4.tcp_rmem = 8192 262144 33554432
net.ipv4.tcp_wmem = 8192 262144 33554432
net.ipv4.tcp_max_syn_backlog = 8192
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_mtu_probing = 1

# File descriptor limits
fs.file-max = 2097152

# Apply configuration
sudo sysctl -p /etc/sysctl.d/99-spooky.conf
```

### Resource Limits

Configure ulimits for the spooky service account:

```bash
# /etc/security/limits.d/spooky.conf
spooky soft nofile 1048576
spooky hard nofile 1048576
spooky soft nproc 16384
spooky hard nproc 16384
spooky soft memlock unlimited
spooky hard memlock unlimited
```

### Production Configuration

```yaml
# /etc/spooky/config.yaml
version: 1

listen:
  protocol: http3
  address: "0.0.0.0"
  port: 443
  tls:
    cert: "/etc/spooky/certs/fullchain.pem"
    key: "/etc/spooky/certs/privkey.pem"

# Define upstream pools with health checking
upstream:
  # API backend pool with consistent hashing for session affinity
  api_pool:
    load_balancing:
      type: "consistent-hash"
    route:
      path_prefix: "/api"
    backends:
      - id: "api-01"
        address: "10.0.10.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
          timeout_ms: 2000
          failure_threshold: 3
          success_threshold: 2

      - id: "api-02"
        address: "10.0.10.11:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000
          timeout_ms: 2000
          failure_threshold: 3
          success_threshold: 2

  # Static content pool with round-robin
  static_pool:
    load_balancing:
      type: "round-robin"
    route:
      path_prefix: "/static"
    backends:
      - id: "static-01"
        address: "10.0.20.10:8080"
        weight: 100
        health_check:
          path: "/"
          interval: 10000

      - id: "static-02"
        address: "10.0.20.11:8080"
        weight: 100
        health_check:
          path: "/"
          interval: 10000

  # Default backend pool
  default_pool:
    load_balancing:
      type: "random"
    route:
      path_prefix: "/"
    backends:
      - id: "web-01"
        address: "10.0.30.10:8080"
        weight: 100
        health_check:
          path: "/health"
          interval: 5000

# Logging configuration
log:
  level: info  # Use 'warn' for production to reduce I/O

# Connection tuning (if supported by configuration schema)
# Adjust based on backend capacity and expected load
# max_concurrent_connections: 10000
# backend_connection_pool_size: 100
```

**Configuration Notes:**
- Route matching uses longest-prefix: more specific paths take precedence
- Health check intervals balance detection speed vs. backend load
- Adjust `failure_threshold` and `success_threshold` based on backend stability
- Weight distribution should reflect backend capacity
- Consistent hashing is appropriate for stateful backends requiring session affinity

## TLS Certificate Management

### Certificate Acquisition

#### Let's Encrypt (ACME)

```bash
# Install certbot
sudo apt-get install -y certbot

# Obtain certificate (HTTP-01 challenge, requires port 80)
sudo certbot certonly --standalone \
  --preferred-challenges http \
  --email ops@example.com \
  --agree-tos \
  --non-interactive \
  -d proxy.example.com

# Copy to spooky directory
sudo cp /etc/letsencrypt/live/proxy.example.com/fullchain.pem /etc/spooky/certs/
sudo cp /etc/letsencrypt/live/proxy.example.com/privkey.pem /etc/spooky/certs/
sudo chown root:spooky /etc/spooky/certs/*.pem
sudo chmod 640 /etc/spooky/certs/privkey.pem
sudo chmod 644 /etc/spooky/certs/fullchain.pem
```

#### Automated Renewal

```bash
# Create renewal hook
sudo tee /etc/letsencrypt/renewal-hooks/deploy/spooky-reload.sh << 'EOF'
#!/bin/bash
set -e

CERT_DOMAIN="proxy.example.com"
SPOOKY_CERT_DIR="/etc/spooky/certs"

# Copy renewed certificates
cp "/etc/letsencrypt/live/${CERT_DOMAIN}/fullchain.pem" "${SPOOKY_CERT_DIR}/"
cp "/etc/letsencrypt/live/${CERT_DOMAIN}/privkey.pem" "${SPOOKY_CERT_DIR}/"

# Set permissions
chown root:spooky "${SPOOKY_CERT_DIR}"/*.pem
chmod 640 "${SPOOKY_CERT_DIR}/privkey.pem"
chmod 644 "${SPOOKY_CERT_DIR}/fullchain.pem"

# Reload spooky (graceful reload if supported, otherwise restart)
systemctl reload-or-restart spooky

logger -t spooky-cert-renewal "TLS certificates renewed and spooky reloaded"
EOF

sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/spooky-reload.sh

# Test renewal process
sudo certbot renew --dry-run
```

### Certificate Validation

Before deploying new certificates:

```bash
# Verify certificate and key match
openssl x509 -noout -modulus -in /etc/spooky/certs/fullchain.pem | openssl md5
openssl rsa -noout -modulus -in /etc/spooky/certs/privkey.pem | openssl md5

# Verify certificate chain
openssl verify -CAfile /etc/spooky/certs/fullchain.pem /etc/spooky/certs/fullchain.pem

# Check expiration
openssl x509 -noout -dates -in /etc/spooky/certs/fullchain.pem

# Verify SAN entries
openssl x509 -noout -text -in /etc/spooky/certs/fullchain.pem | grep -A1 "Subject Alternative Name"
```

## Systemd Service Configuration

### Service Unit

```ini
# /etc/systemd/system/spooky.service
[Unit]
Description=Spooky HTTP/3 to HTTP/2 Proxy
Documentation=https://github.com/nishujangra/spooky
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=spooky
Group=spooky

# Binary and configuration
ExecStart=/usr/local/bin/spooky --config /etc/spooky/config.yaml
# Note: Hot reload not currently supported, use restart instead
# ExecReload=/bin/kill -HUP $MAINPID

# Restart policy
Restart=always
RestartSec=5s
StartLimitBurst=3
StartLimitIntervalSec=60s

# Resource limits
LimitNOFILE=1048576
LimitNPROC=16384
TasksMax=16384

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log/spooky
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectKernelLogs=true
ProtectControlGroups=true
ProtectProc=invisible
ProcSubset=pid
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictNamespaces=true
RestrictRealtime=true
RestrictSUIDSGID=true
LockPersonality=true
SystemCallArchitectures=native
SystemCallFilter=@system-service
SystemCallFilter=~@privileged @resources

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=spooky

[Install]
WantedBy=multi-user.target
```

### Service Management

```bash
# Install and enable service
sudo systemctl daemon-reload
sudo systemctl enable spooky.service

# Start service
sudo systemctl start spooky.service

# Verify status
sudo systemctl status spooky.service

# View logs
sudo journalctl -u spooky.service -f

# Restart for configuration changes (hot reload planned)
sudo systemctl restart spooky.service

# Full restart
sudo systemctl restart spooky.service
```

## Security Hardening

### Network Security

**Firewall Configuration (nftables)**

```bash
# /etc/nftables.conf (example rules)
table inet filter {
    chain input {
        type filter hook input priority filter; policy drop;

        # Allow established/related connections
        ct state established,related accept
        ct state invalid drop

        # Allow loopback
        iif lo accept

        # Allow SSH (restrict to management network)
        ip saddr 10.0.0.0/24 tcp dport 22 ct state new accept

        # Allow QUIC/HTTP3
        udp dport 443 accept

        # Allow health checks from monitoring (optional)
        ip saddr 10.0.0.0/24 tcp dport 8080 ct state new accept

        # Rate limiting for new connections
        ct state new limit rate over 1000/second burst 2000 packets drop
    }

    chain forward {
        type filter hook forward priority filter; policy drop;
    }

    chain output {
        type filter hook output priority filter; policy accept;
    }
}
```

### Application Security

**Filesystem Permissions**

```bash
# Configuration immutable after validation
sudo chown root:spooky /etc/spooky/config.yaml
sudo chmod 640 /etc/spooky/config.yaml
sudo chattr +i /etc/spooky/config.yaml  # Immutable (remove with -i for updates)

# Certificate protection
sudo chmod 640 /etc/spooky/certs/privkey.pem
sudo chmod 644 /etc/spooky/certs/fullchain.pem
```

**TLS Configuration**

Ensure TLS 1.3 is enforced with strong cipher suites. Note: cipher suite configuration may be limited by the underlying QUIC library (quiche). Verify supported options in the Spooky documentation.

### SELinux / AppArmor

For environments requiring mandatory access control, create appropriate policies. Example AppArmor profile skeleton:

```bash
# /etc/apparmor.d/usr.local.bin.spooky
#include <tunables/global>

/usr/local/bin/spooky {
  #include <abstractions/base>
  #include <abstractions/nameservice>

  capability net_bind_service,
  capability setuid,
  capability setgid,

  /usr/local/bin/spooky mr,
  /etc/spooky/** r,
  /var/log/spooky/** rw,

  network inet dgram,
  network inet6 dgram,
  network inet stream,
  network inet6 stream,
}
```

## Monitoring and Observability

### Metrics Exposition

**Note**: Metrics exposition is planned for future releases but not currently implemented. Spooky currently maintains internal counters only.

When metrics are implemented, they will follow Prometheus exposition format for easy integration with monitoring systems. Example configuration (for future reference):

```yaml
# prometheus.yml (planned)
scrape_configs:
  - job_name: 'spooky'
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: '/metrics'  # To be implemented
    static_configs:
      - targets: ['spooky-01.internal:9090', 'spooky-02.internal:9090']
        labels:
          environment: 'production'
          service: 'proxy'
```

### Key Metrics to Monitor

**Throughput Metrics**
- Requests per second (by route, backend, status code)
- Bytes transferred (ingress/egress)
- Active connections (QUIC, HTTP/2)

**Latency Metrics**
- Request duration percentiles (p50, p95, p99)
- Backend response time
- Connection establishment time
- TLS handshake duration

**Error Metrics**
- HTTP 5xx error rate
- Backend connection failures
- Health check failure count
- TLS handshake failures

**Resource Metrics**
- CPU utilization
- Memory usage (RSS, heap)
- File descriptor usage
- Network buffer utilization

### Alerting Rules

```yaml
# prometheus-alerts.yml
groups:
  - name: spooky-availability
    rules:
      - alert: SpookyInstanceDown
        expr: up{job="spooky"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Spooky instance {{ $labels.instance }} is down"
          description: "Instance has been unreachable for 1 minute"

      - alert: SpookyHighErrorRate
        expr: |
          (
            sum(rate(http_requests_total{job="spooky",status=~"5.."}[5m]))
            /
            sum(rate(http_requests_total{job="spooky"}[5m]))
          ) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High 5xx error rate on Spooky"
          description: "Error rate is {{ $value | humanizePercentage }}"

      - alert: SpookyBackendAllDown
        expr: |
          sum by (upstream_pool) (backend_healthy{job="spooky"}) == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "All backends down for pool {{ $labels.upstream_pool }}"

      - alert: SpookyLatencyHigh
        expr: |
          histogram_quantile(0.95,
            sum(rate(http_request_duration_seconds_bucket{job="spooky"}[5m])) by (le)
          ) > 1.0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High request latency (p95 > 1s)"

      - alert: SpookyFileDescriptorExhaustion
        expr: process_open_fds{job="spooky"} / process_max_fds{job="spooky"} > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "File descriptor usage high on {{ $labels.instance }}"

  - name: spooky-capacity
    rules:
      - alert: SpookyCPUSaturation
        expr: rate(process_cpu_seconds_total{job="spooky"}[5m]) > 0.8
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "CPU saturation on {{ $labels.instance }}"

      - alert: SpookyMemoryPressure
        expr: |
          process_resident_memory_bytes{job="spooky"} / node_memory_MemTotal_bytes > 0.8
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Memory pressure on {{ $labels.instance }}"
```

### Log Management

**Structured Logging**

Configure JSON output for log aggregation:

```yaml
log:
  level: info
  format: json  # If supported
```

**Log Aggregation**

Ship logs to centralized logging (ELK, Loki, Splunk):

```bash
# Example: journald to Loki via Promtail
# /etc/promtail/config.yml
server:
  http_listen_port: 9080

positions:
  filename: /var/lib/promtail/positions.yaml

clients:
  - url: http://loki.internal:3100/loki/api/v1/push

scrape_configs:
  - job_name: systemd-journal
    journal:
      max_age: 12h
      labels:
        job: systemd-journal
    relabel_configs:
      - source_labels: ['__journal__systemd_unit']
        target_label: 'unit'
      - source_labels: ['__journal_syslog_identifier']
        target_label: 'syslog_identifier'
    pipeline_stages:
      - match:
          selector: '{syslog_identifier="spooky"}'
          stages:
            - json:
                expressions:
                  level: level
                  path: path
                  backend: backend
                  duration: duration
            - labels:
                level:
                path:
                backend:
```

**Log Rotation**

Configure log rotation for file-based logging (when stdout/stderr is redirected to files):

```bash
# /etc/logrotate.d/spooky
/var/log/spooky/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 0640 spooky spooky
    sharedscripts
    postrotate
        /bin/systemctl restart spooky.service > /dev/null 2>&1 || true
    endscript
}
```

## High Availability Architecture

### Active-Active Configuration

Deploy multiple Spooky instances behind a UDP-capable load balancer:

```
                    ┌─────────────┐
                    │   DNS/GLB   │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              │                         │
         ┌────▼────┐              ┌────▼────┐
         │ L4 LB 1 │              │ L4 LB 2 │
         └────┬────┘              └────┬────┘
              │                         │
       ┌──────┴──────┬──────────────────┴──────┐
       │             │                          │
  ┌────▼───┐   ┌────▼───┐                ┌────▼───┐
  │Spooky 1│   │Spooky 2│      ...       │Spooky N│
  └────┬───┘   └────┬───┘                └────┬───┘
       │             │                          │
       └──────┬──────┴──────────────────────────┘
              │
      ┌───────▼────────┐
      │ Backend Pool   │
      └────────────────┘
```

**Layer 4 Load Balancer Options:**
- ECMP routing with consistent hashing
- Anycast with BGP
- Cloud provider UDP load balancers (AWS NLB, GCP Network Load Balancer)

### Configuration Synchronization

Maintain consistent configuration across instances:

```bash
# Use configuration management (Ansible example)
# playbooks/deploy-spooky.yml
- hosts: proxy_tier
  become: yes
  tasks:
    - name: Deploy Spooky configuration
      template:
        src: templates/spooky-config.yaml.j2
        dest: /etc/spooky/config.yaml
        owner: root
        group: spooky
        mode: '0640'
        # Note: Configuration validation happens during startup
      notify: restart spooky

    - name: Deploy TLS certificates
      copy:
        src: "{{ item.src }}"
        dest: "{{ item.dest }}"
        owner: root
        group: spooky
        mode: "{{ item.mode }}"
      with_items:
        - { src: 'certs/fullchain.pem', dest: '/etc/spooky/certs/fullchain.pem', mode: '0644' }
        - { src: 'certs/privkey.pem', dest: '/etc/spooky/certs/privkey.pem', mode: '0640' }
      notify: restart spooky

  handlers:
    - name: restart spooky
      systemd:
        name: spooky
        state: restarted
```

### Health Checking

Implement external health checks for load balancer integration:

```bash
# Health check script for L4 LB integration
# /usr/local/bin/spooky-healthcheck.sh
#!/bin/bash
set -euo pipefail

# Check if process is running
if ! pgrep -x spooky > /dev/null; then
    exit 1
fi

# Check if listening on QUIC port (example: port 443)
if ! ss -ulpn | grep -q ":443 "; then
    exit 1
fi

# Optional: Check internal health endpoint if available
# curl -sf http://localhost:8080/health || exit 1

exit 0
```

## Performance Optimization

### Connection Pooling

Optimize HTTP/2 backend connection pool based on backend capacity:

- Typical ratio: 1 backend connection per 50-100 concurrent QUIC connections
- Monitor backend connection state and adjust pool size accordingly
- Consider backend connection limits and TCP socket exhaustion

### QUIC Tuning

QUIC performance depends on UDP buffer sizes and packet processing:

```bash
# Increase UDP receive buffer for high packet rates
# Already covered in kernel tuning section, but worth emphasizing:
# net.core.rmem_max = 67108864
# net.core.rmem_default = 16777216

# Verify current settings
sysctl net.core.rmem_max
sysctl net.core.rmem_default

# Monitor UDP receive buffer overflows
netstat -su | grep "receive errors"
```

### CPU Affinity

For multi-instance deployments on large systems, consider CPU pinning:

```ini
# /etc/systemd/system/spooky@.service (template unit for multiple instances)
[Service]
# Pin instance 0 to CPUs 0-3, instance 1 to CPUs 4-7, etc.
CPUAffinity=%i-$(((%i+1)*4-1))
```

### Memory Management

Monitor heap usage and consider tuning allocator behavior:

```bash
# If using jemalloc (check with ldd /usr/local/bin/spooky)
# Set environment variables for memory profiling
# Environment=/usr/bin/env MALLOC_CONF=prof:true,prof_prefix:/var/log/spooky/jeprof
```

## Operational Procedures

### Deployment Process

1. **Configuration Validation**: Validate new configuration in staging environment
2. **Gradual Rollout**: Deploy to canary instance first, monitor error rates and latency
3. **Progressive Deployment**: Roll out to remaining instances with staggered timing
4. **Rollback Plan**: Keep previous binary version and configuration for rapid rollback

```bash
# Deployment script example
#!/bin/bash
set -euo pipefail

NEW_VERSION="$1"
INSTANCES=("spooky-01" "spooky-02" "spooky-03")

# Deploy to canary
echo "Deploying to canary: ${INSTANCES[0]}"
ssh "${INSTANCES[0]}" "sudo systemctl stop spooky && \
  sudo cp /usr/local/bin/spooky /usr/local/bin/spooky.prev && \
  sudo wget -O /usr/local/bin/spooky https://releases.example.com/spooky-${NEW_VERSION} && \
  sudo systemctl start spooky"

echo "Canary deployed. Monitor metrics for 5 minutes..."
sleep 300

# Check canary health
if ! curl -sf "http://${INSTANCES[0]}:8080/health"; then
    echo "Canary health check failed. Rolling back."
    ssh "${INSTANCES[0]}" "sudo systemctl stop spooky && \
      sudo mv /usr/local/bin/spooky.prev /usr/local/bin/spooky && \
      sudo systemctl start spooky"
    exit 1
fi

# Deploy to remaining instances
for instance in "${INSTANCES[@]:1}"; do
    echo "Deploying to ${instance}"
    ssh "${instance}" "sudo systemctl stop spooky && \
      sudo cp /usr/local/bin/spooky /usr/local/bin/spooky.prev && \
      sudo wget -O /usr/local/bin/spooky https://releases.example.com/spooky-${NEW_VERSION} && \
      sudo systemctl start spooky"
    sleep 30
done

echo "Deployment complete."
```

### Configuration Changes

```bash
# Test configuration before applying (startup validation will happen)
sudo -u spooky spooky --config /etc/spooky/config.yaml.new

# Atomic configuration update
sudo mv /etc/spooky/config.yaml /etc/spooky/config.yaml.backup
sudo mv /etc/spooky/config.yaml.new /etc/spooky/config.yaml

# Restart service (hot reload planned for future release)
sudo systemctl restart spooky.service

# Verify reload success
sudo systemctl status spooky.service
sudo journalctl -u spooky.service -n 50 --no-pager
```

### Incident Response

**High Error Rate**

1. Check backend health: `sudo journalctl -u spooky.service | grep "health check"`
2. Verify backend connectivity: `curl -v http://<backend-ip>:<port>/health`
3. Review recent configuration changes
4. Check for backend capacity issues (CPU, memory, connection limits)
5. If necessary, remove unhealthy backends from pool or rollback configuration

**Connection Exhaustion**

1. Check file descriptor usage: `ls /proc/$(pgrep spooky)/fd | wc -l`
2. Review ulimits: `cat /proc/$(pgrep spooky)/limits`
3. Identify connection leaks: `ss -anp | grep spooky | wc -l`
4. Restart service if connection leak suspected: `sudo systemctl restart spooky.service`

**Memory Leak**

1. Monitor RSS over time: `ps aux | grep spooky`
2. Capture heap profile if using jemalloc
3. Review recent traffic patterns for anomalies
4. Restart service to recover capacity, engage upstream support

### Capacity Planning

Monitor these indicators for scaling decisions:

**Scale Horizontally (Add Instances) When:**
- CPU utilization sustained >70% across all instances
- Network bandwidth saturation
- Request queueing observed (increasing latency at constant RPS)

**Scale Vertically (Increase Resources) When:**
- Memory usage approaching limits
- Context switching rate high with available CPU
- Single-instance throughput below theoretical maximum

**Scaling Methodology:**
1. Baseline current performance metrics
2. Load test with synthetic traffic at 2x current peak
3. Identify bottleneck (CPU, memory, network, backend capacity)
4. Size new deployment for 3x current peak with headroom
5. Implement autoscaling based on CPU/RPS metrics if using cloud infrastructure

## Troubleshooting

### Diagnostic Commands

```bash
# Process information
ps aux | grep spooky
pstree -p $(pgrep spooky)

# Open file descriptors
ls -l /proc/$(pgrep spooky)/fd | wc -l
lsof -p $(pgrep spooky) | head -20

# Network connections
ss -anp | grep spooky | grep ESTABLISHED | wc -l
ss -anp | grep spooky | grep TIME-WAIT | wc -l
ss -su  # UDP socket statistics

# System calls and performance
strace -c -p $(pgrep spooky) -e trace=network  # 10 second sample
perf top -p $(pgrep spooky)

# Memory analysis
cat /proc/$(pgrep spooky)/status | grep -E "Vm|Rss"
pmap -x $(pgrep spooky)

# Configuration verification
sudo -u spooky spooky --config /etc/spooky/config.yaml
```

### Common Issues

**Service Fails to Start**

Symptoms: systemd reports failure, process exits immediately

Diagnosis:
```bash
# Check systemd logs
sudo journalctl -u spooky.service -n 100 --no-pager

# Test configuration manually
sudo -u spooky /usr/local/bin/spooky --config /etc/spooky/config.yaml

# Check certificate validity
openssl x509 -noout -dates -in /etc/spooky/certs/fullchain.pem

# Verify file permissions
ls -la /etc/spooky/certs/
```

Resolution: Address configuration errors, certificate issues, or permission problems identified above.

**High Latency**

Symptoms: Increased p95/p99 request duration

Diagnosis:
```bash
# Check backend latency
curl -w "@curl-format.txt" -o /dev/null -s http://<backend>/<path>

# Network path latency
mtr <backend-ip>

# System resource contention
top -p $(pgrep spooky)
iostat -x 1 10

# Connection state distribution
ss -anp | grep spooky | awk '{print $2}' | sort | uniq -c
```

Resolution: Investigate backend performance, network conditions, or system resource exhaustion.

**Backend Connection Failures**

Symptoms: 502/503 errors, "connection refused" in logs

Diagnosis:
```bash
# Verify backend reachability
nc -zv <backend-ip> <backend-port>

# Check backend process state
ssh <backend> "systemctl status <backend-service>"

# Firewall/security group verification
sudo iptables -L -n -v | grep <backend-ip>

# Review health check logs
sudo journalctl -u spooky.service | grep "health check"
```

Resolution: Restore backend service, fix network connectivity, or adjust health check parameters.

## Backup and Disaster Recovery

### Configuration Backup

```bash
# /usr/local/bin/backup-spooky-config.sh
#!/bin/bash
set -euo pipefail

BACKUP_ROOT="/var/backups/spooky"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BACKUP_DIR="${BACKUP_ROOT}/${TIMESTAMP}"

mkdir -p "${BACKUP_DIR}"

# Backup configuration
cp -a /etc/spooky/config.yaml "${BACKUP_DIR}/"

# Backup certificates (excluding private keys for security)
cp /etc/spooky/certs/fullchain.pem "${BACKUP_DIR}/"

# Store metadata
cat > "${BACKUP_DIR}/metadata.txt" << EOF
Backup Date: $(date -Is)
Hostname: $(hostname -f)
Spooky Version: $(spooky --version 2>&1 || echo "unknown")
EOF

# Compress backup
tar czf "${BACKUP_ROOT}/spooky-config-${TIMESTAMP}.tar.gz" -C "${BACKUP_ROOT}" "${TIMESTAMP}"
rm -rf "${BACKUP_DIR}"

# Rotate old backups (keep 30 days)
find "${BACKUP_ROOT}" -name "spooky-config-*.tar.gz" -mtime +30 -delete

echo "Backup completed: ${BACKUP_ROOT}/spooky-config-${TIMESTAMP}.tar.gz"
```

Schedule via cron:
```bash
# Run daily at 2 AM
0 2 * * * /usr/local/bin/backup-spooky-config.sh >> /var/log/spooky/backup.log 2>&1
```

### Recovery Procedures

**Configuration Restoration**
```bash
# Extract backup
tar xzf /var/backups/spooky/spooky-config-YYYYMMDD-HHMMSS.tar.gz -C /tmp

# Restore configuration
sudo cp /tmp/YYYYMMDD-HHMMSS/config.yaml /etc/spooky/config.yaml
sudo chown root:spooky /etc/spooky/config.yaml
sudo chmod 640 /etc/spooky/config.yaml

# Restart service (hot reload not currently supported)
sudo systemctl restart spooky.service
```

**Complete System Rebuild**
1. Provision new host with OS installation
2. Apply system configuration (kernel tuning, resource limits)
3. Install Spooky binary
4. Restore configuration from backup
5. Install TLS certificates
6. Start service and verify health
7. Update load balancer to include new instance

Recovery Time Objective (RTO): Target <15 minutes with automation
Recovery Point Objective (RPO): Configuration changes backed up daily

## Maintenance Windows

### Planned Maintenance Checklist

**Pre-Maintenance**
- [ ] Notify stakeholders of maintenance window
- [ ] Verify backup procedures completed successfully
- [ ] Review rollback procedures
- [ ] Prepare configuration changes or binary updates
- [ ] Verify staging environment changes successful

**During Maintenance**
- [ ] Remove instance from load balancer (if applicable)
- [ ] Drain existing connections (if graceful shutdown supported)
- [ ] Apply updates (configuration, binary, certificates)
- [ ] Restart service
- [ ] Verify service health and connectivity
- [ ] Monitor error rates and latency for 5 minutes
- [ ] Return instance to load balancer

**Post-Maintenance**
- [ ] Confirm all instances operational
- [ ] Review metrics for anomalies
- [ ] Update change log
- [ ] Close maintenance notification

## Additional Resources

- Spooky Configuration Reference: `/docs/configuration/reference.md`
- Load Balancing Strategies: `/docs/user-guide/load-balancing.md`
- Troubleshooting Guide: `/docs/troubleshooting/common-issues.md`
- Architecture Overview: `/docs/architecture/overview.md`

For issues not covered in this guide, consult the project repository issue tracker or engage with the development team.