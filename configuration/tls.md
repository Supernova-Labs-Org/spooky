# TLS Configuration

Guide for configuring TLS certificates for HTTP/3 connections in Spooky.

## Overview

HTTP/3 uses QUIC as its transport protocol, which requires TLS 1.3 for encryption and authentication. Spooky requires valid TLS certificates to establish secure connections with clients.

## Requirements

### Protocol Requirements

- TLS 1.3 (required for QUIC/HTTP3)
- ALPN (Application-Layer Protocol Negotiation) support
- SNI (Server Name Indication) support

### Supported Formats

- **Certificates**: PEM-encoded X.509 certificates
- **Private Keys**: PEM-encoded PKCS#8 format (recommended) or traditional RSA/ECDSA formats
- **Key Types**: RSA (2048-bit minimum) or ECDSA (P-256, P-384)

## Certificate Generation

### Development: Self-Signed Certificates with mkcert

For local development, mkcert generates locally-trusted certificates:

```bash
# Install mkcert
# Ubuntu/Debian
sudo apt install mkcert

# macOS
brew install mkcert

# Install local CA
mkcert -install

# Generate certificate for localhost
mkdir -p certs
cd certs
mkcert -key-file server.key -cert-file server.crt localhost 127.0.0.1 ::1

# Verify generation
ls -lh server.crt server.key
```

Configuration:

```yaml
listen:
  protocol: http3
  port: 9889
  address: "127.0.0.1"
  tls:
    cert: "certs/server.crt"
    key: "certs/server.key"
```

### Development: Self-Signed Certificates with OpenSSL

For environments where mkcert is not available:

```bash
# Create certificate directory
mkdir -p certs
cd certs

# Generate private key (RSA 2048-bit)
openssl genrsa -out server.key 2048

# Generate certificate signing request
openssl req -new -key server.key -out server.csr \
  -subj "/C=US/ST=State/L=City/O=Development/CN=localhost"

# Generate self-signed certificate (valid 365 days)
openssl x509 -req -in server.csr -signkey server.key \
  -out server.crt -days 365 -sha256

# Convert key to PKCS#8 format (recommended)
openssl pkcs8 -topk8 -nocrypt -in server.key -out server-pkcs8.key

# Verify certificate
openssl x509 -in server.crt -text -noout

# Clean up CSR
rm server.csr
```

Configuration:

```yaml
listen:
  protocol: http3
  port: 9889
  address: "127.0.0.1"
  tls:
    cert: "certs/server.crt"
    key: "certs/server-pkcs8.key"
```

### Production: Let's Encrypt

For production deployments with public domains:

```bash
# Install certbot
sudo apt update
sudo apt install certbot

# Option 1: Standalone mode (requires port 80 available)
sudo certbot certonly --standalone \
  -d example.com \
  -d www.example.com

# Option 2: DNS challenge (no port requirements)
sudo certbot certonly --manual \
  --preferred-challenges dns \
  -d example.com

# Certificates are saved to:
# Certificate: /etc/letsencrypt/live/example.com/fullchain.pem
# Private Key: /etc/letsencrypt/live/example.com/privkey.pem
```

Configuration:

```yaml
listen:
  protocol: http3
  port: 9889
  address: "0.0.0.0"
  tls:
    cert: "/etc/letsencrypt/live/example.com/fullchain.pem"
    key: "/etc/letsencrypt/live/example.com/privkey.pem"
```

### Production: ECDSA Certificates

ECDSA certificates offer better performance than RSA:

```bash
# Generate ECDSA private key (P-256)
openssl ecparam -genkey -name prime256v1 -out server-ec.key

# Convert to PKCS#8 format
openssl pkcs8 -topk8 -nocrypt -in server-ec.key -out server-ec-pkcs8.key

# Generate CSR
openssl req -new -key server-ec-pkcs8.key -out server-ec.csr \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=example.com"

# Generate self-signed certificate (or send CSR to CA)
openssl x509 -req -in server-ec.csr -signkey server-ec-pkcs8.key \
  -out server-ec.crt -days 365 -sha256
```

## Certificate Configuration

### Basic Configuration

Minimal TLS configuration for HTTP/3:

```yaml
listen:
  protocol: http3
  port: 9889
  address: "0.0.0.0"
  tls:
    cert: "/path/to/certificate.pem"
    key: "/path/to/private-key.pem"
```

### Path Specifications

Paths can be absolute or relative:

```yaml
# Absolute paths (recommended for production)
tls:
  cert: "/etc/spooky/certs/fullchain.pem"
  key: "/etc/spooky/certs/privkey.pem"

# Relative paths (relative to working directory)
tls:
  cert: "certs/server.crt"
  key: "certs/server.key"
```

### Multi-Domain Certificates

For certificates covering multiple domains (SAN certificates):

```bash
# Generate certificate with Subject Alternative Names
openssl req -new -x509 -key server.key -out server.crt -days 365 \
  -subj "/CN=example.com" \
  -addext "subjectAltName=DNS:example.com,DNS:www.example.com,DNS:api.example.com"
```

Configuration remains the same:

```yaml
tls:
  cert: "/etc/spooky/certs/multi-domain.crt"
  key: "/etc/spooky/certs/multi-domain.key"
```

## File Permissions and Security

### Recommended Permissions

Restrict access to certificate files:

```bash
# Create dedicated certificate directory
sudo mkdir -p /etc/spooky/certs
sudo chown spooky:spooky /etc/spooky/certs
sudo chmod 700 /etc/spooky/certs

# Set certificate permissions
sudo chmod 644 /etc/spooky/certs/server.crt
sudo chmod 600 /etc/spooky/certs/server.key

# Verify permissions
ls -l /etc/spooky/certs/
```

Expected output:

```
drwx------ 2 spooky spooky 4096 Dec 15 10:00 .
-rw-r--r-- 1 spooky spooky 1234 Dec 15 10:00 server.crt
-rw------- 1 spooky spooky 1704 Dec 15 10:00 server.key
```

### Security Best Practices

1. **Private Key Protection**
   - Never commit private keys to version control
   - Use restrictive file permissions (600)
   - Store keys on encrypted filesystems
   - Consider using hardware security modules (HSM) for production

2. **Certificate Chain Validation**
   - Use complete certificate chains (fullchain.pem with Let's Encrypt)
   - Include intermediate certificates
   - Verify chain with `openssl verify`

3. **Certificate Monitoring**
   - Monitor expiration dates
   - Set up renewal automation for Let's Encrypt
   - Implement alerting for certificates expiring within 30 days

## Certificate Validation

### Verify Certificate and Key Match

Ensure certificate and private key are paired correctly:

```bash
# Extract modulus from certificate
cert_modulus=$(openssl x509 -noout -modulus -in server.crt | md5sum)

# Extract modulus from private key
key_modulus=$(openssl rsa -noout -modulus -in server.key | md5sum)

# Compare (should be identical)
echo "Certificate: $cert_modulus"
echo "Private Key: $key_modulus"
```

For ECDSA keys:

```bash
# Verify ECDSA private key
openssl ec -in server-ec.key -check

# Verify certificate
openssl x509 -in server-ec.crt -text -noout
```

### Verify Certificate Properties

Check certificate details:

```bash
# Display certificate information
openssl x509 -in server.crt -text -noout

# Check expiration date
openssl x509 -in server.crt -noout -enddate

# Check subject and issuer
openssl x509 -in server.crt -noout -subject -issuer

# Verify certificate chain
openssl verify -CAfile ca.crt server.crt
```

### Test Configuration

Verify Spooky can load certificates:

```bash
# Test configuration validity
spooky --config config.yaml

# Run in debug mode to see TLS initialization
# Set log level in config.yaml (log.level) or via RUST_LOG=debug
spooky --config config.yaml
```

## Certificate Rotation and Renewal

### Let's Encrypt Automatic Renewal

Let's Encrypt certificates are valid for 90 days. Set up automatic renewal:

```bash
# Test renewal process
sudo certbot renew --dry-run

# Enable automatic renewal (certbot installs systemd timer)
sudo systemctl status certbot.timer

# Manually renew certificates
sudo certbot renew

# Restart Spooky after renewal (hot reload planned)
sudo systemctl restart spooky
```

### Manual Certificate Rotation

For manually-managed certificates:

```bash
# Backup current certificates
sudo cp /etc/spooky/certs/server.crt /etc/spooky/certs/server.crt.backup
sudo cp /etc/spooky/certs/server.key /etc/spooky/certs/server.key.backup

# Install new certificates
sudo cp new-server.crt /etc/spooky/certs/server.crt
sudo cp new-server.key /etc/spooky/certs/server.key

# Set permissions
sudo chmod 644 /etc/spooky/certs/server.crt
sudo chmod 600 /etc/spooky/certs/server.key

# Restart Spooky (hot reload planned for future release)
sudo systemctl restart spooky

# Verify new certificates are loaded
openssl s_client -connect localhost:9889 -servername localhost < /dev/null 2>/dev/null | openssl x509 -noout -dates
```

### Monitoring Certificate Expiry

Check certificate expiration:

```bash
# Check days until expiry
openssl x509 -in /etc/spooky/certs/server.crt -noout -enddate

# Calculate days remaining
days_left=$(( ($(date -d "$(openssl x509 -in /etc/spooky/certs/server.crt -noout -enddate | cut -d= -f2)" +%s) - $(date +%s)) / 86400 ))
echo "Certificate expires in $days_left days"

# Alert if less than 30 days
if [ $days_left -lt 30 ]; then
  echo "WARNING: Certificate expires soon!"
fi
```

## Troubleshooting

### Common Issues

#### Certificate File Not Found

```
Error: failed to read certificate file: No such file or directory
```

Solution:

```bash
# Verify file exists
ls -l /etc/spooky/certs/server.crt

# Check path in configuration
cat config.yaml | grep -A2 tls

# Use absolute paths
realpath certs/server.crt
```

#### Permission Denied

```
Error: failed to read certificate file: Permission denied
```

Solution:

```bash
# Check file permissions
ls -l /etc/spooky/certs/

# Fix permissions
sudo chown spooky:spooky /etc/spooky/certs/server.{crt,key}
sudo chmod 644 /etc/spooky/certs/server.crt
sudo chmod 600 /etc/spooky/certs/server.key

# Verify Spooky user can read files
sudo -u spooky cat /etc/spooky/certs/server.crt > /dev/null
```

#### Invalid Certificate Format

```
Error: failed to parse certificate: invalid PEM format
```

Solution:

```bash
# Verify PEM format
openssl x509 -in server.crt -text -noout

# Check file encoding
file server.crt

# Convert DER to PEM if needed
openssl x509 -inform DER -in server.der -out server.pem
```

#### Certificate and Key Mismatch

```
Error: certificate and private key do not match
```

Solution:

```bash
# Verify certificate and key match (RSA)
openssl x509 -noout -modulus -in server.crt | md5sum
openssl rsa -noout -modulus -in server.key | md5sum

# Verify ECDSA key
openssl ec -in server.key -pubout -out server-pub.pem
openssl x509 -in server.crt -pubkey -noout -out cert-pub.pem
diff server-pub.pem cert-pub.pem
```

#### PKCS#8 Format Required

Some systems require PKCS#8 format:

```bash
# Convert traditional RSA to PKCS#8
openssl pkcs8 -topk8 -nocrypt -in server.key -out server-pkcs8.key

# Update configuration to use PKCS#8 key
```

### Testing TLS Connections

#### Test with OpenSSL

```bash
# Test TLS 1.3 connection
echo -e "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n" | \
  openssl s_client -connect localhost:9889 -servername localhost -tls1_3

# Display certificate chain
openssl s_client -connect localhost:9889 -servername localhost -showcerts < /dev/null

# Check ALPN negotiation
openssl s_client -connect localhost:9889 -servername localhost -alpn h3 < /dev/null
```

#### Test with cURL (HTTP/3 Support)

If curl is built with HTTP/3 support:

```bash
# Test HTTP/3 connection
curl --http3 https://localhost:9889/

# Verbose output for debugging
curl --http3 -v https://localhost:9889/

# Test with self-signed certificate
curl --http3 -k https://localhost:9889/
```

### Debug Logging

Enable debug logging to troubleshoot TLS issues:

```yaml
log:
  level: debug
```

Look for log entries related to:

- Certificate loading
- TLS handshake
- QUIC connection establishment
- ALPN negotiation

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `certificate has expired` | Certificate validity period ended | Renew certificate |
| `certificate is not yet valid` | System clock incorrect or certificate future-dated | Check system time |
| `unable to get local issuer certificate` | Missing intermediate certificate | Use fullchain.pem |
| `self signed certificate` | Client doesn't trust self-signed cert | Use CA-signed cert or add to client trust store |
| `wrong signature type` | Key algorithm mismatch | Ensure certificate and key use same algorithm |

## Reference

### Configuration Schema

```yaml
listen:
  tls:
    cert: string    # Path to PEM certificate file (required)
    key: string     # Path to PEM private key file (required)
```

### Supported Key Algorithms

- RSA 2048-bit (minimum)
- RSA 4096-bit (recommended for long-term use)
- ECDSA P-256 (secp256r1)
- ECDSA P-384 (secp384r1)

### Certificate Requirements

- PEM encoding
- X.509 format
- Valid date range (not expired, not future-dated)
- Subject Alternative Names (SAN) for multi-domain support
- Complete certificate chain (including intermediates)