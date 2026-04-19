.PHONY: run build build-spooky clean test test-edge test-transport certs certs-selfsigned certs-ca certs-clean certs-verify certs-dir bench-micro bench-macro bench-gate bench-promote-baseline

CERTS_DIR := certs
SAN_CONF := $(CERTS_DIR)/san.conf

run:
	make build
	sudo ./target/release/spooky --config config/config.yaml

build:
	sudo cargo build --release

build-spooky:
	sudo cargo build -p spooky --bin spooky --release

test:
	sudo cargo test --workspace

test-edge:
	sudo cargo test -p spooky-edge

test-transport:
	sudo cargo test -p spooky-transport

# Certificate generation targets
certs-dir:
	mkdir -p $(CERTS_DIR)

$(SAN_CONF): | certs-dir
	@if [ ! -f "$(SAN_CONF)" ]; then \
		echo "[req]" > "$(SAN_CONF)"; \
		echo "default_bits = 2048" >> "$(SAN_CONF)"; \
		echo "prompt = no" >> "$(SAN_CONF)"; \
		echo "default_md = sha256" >> "$(SAN_CONF)"; \
		echo "distinguished_name = req_distinguished_name" >> "$(SAN_CONF)"; \
		echo "req_extensions = v3_req" >> "$(SAN_CONF)"; \
		echo "" >> "$(SAN_CONF)"; \
		echo "[req_distinguished_name]" >> "$(SAN_CONF)"; \
		echo "C = US" >> "$(SAN_CONF)"; \
		echo "ST = California" >> "$(SAN_CONF)"; \
		echo "L = San Francisco" >> "$(SAN_CONF)"; \
		echo "O = Spooky Proxy" >> "$(SAN_CONF)"; \
		echo "OU = Development" >> "$(SAN_CONF)"; \
		echo "CN = localhost" >> "$(SAN_CONF)"; \
		echo "" >> "$(SAN_CONF)"; \
		echo "[v3_req]" >> "$(SAN_CONF)"; \
		echo "subjectAltName = @alt_names" >> "$(SAN_CONF)"; \
		echo "" >> "$(SAN_CONF)"; \
		echo "[alt_names]" >> "$(SAN_CONF)"; \
		echo "DNS.1 = localhost" >> "$(SAN_CONF)"; \
		echo "DNS.2 = proxy.spooky.local" >> "$(SAN_CONF)"; \
		echo "IP.1 = 127.0.0.1" >> "$(SAN_CONF)"; \
		echo "IP.2 = ::1" >> "$(SAN_CONF)"; \
		echo "✅ Created default $(SAN_CONF)"; \
	fi

certs-selfsigned: $(SAN_CONF)
	@echo "🔐 Generating ECC P-256 private key..."
	openssl ecparam -name prime256v1 -genkey -noout -out certs/proxy-key.pem
	@echo "🔄 Converting to PKCS#8 format for rustls compatibility..."
	openssl pkcs8 -topk8 -nocrypt -in certs/proxy-key.pem -out certs/proxy-key-pkcs8.pem
	@echo "📄 Creating Certificate Signing Request (CSR)..."
	openssl req -new -key certs/proxy-key-pkcs8.pem -out certs/proxy.csr -config certs/san.conf -extensions v3_req
	@echo "🏛️ Creating self-signed certificate..."
	openssl x509 -req -in certs/proxy.csr -signkey certs/proxy-key-pkcs8.pem -out certs/proxy-cert.pem -days 365 -extensions v3_req -extfile certs/san.conf -sha256
	@echo "🔄 Converting to DER format..."
	openssl pkcs8 -in certs/proxy-key-pkcs8.pem -topk8 -nocrypt -outform DER -out certs/proxy-key.der
	openssl x509 -in certs/proxy-cert.pem -outform DER -out certs/proxy-cert.der
	@echo "🔒 Setting secure permissions..."
	chmod 600 certs/proxy-key.pem certs/proxy-key-pkcs8.pem certs/proxy-key.der
	chmod 644 certs/proxy-cert.pem certs/proxy-cert.der
	@echo "✅ Self-signed certificates created successfully!"

certs-ca: $(SAN_CONF)
	@echo "🏛️ Creating Certificate Authority..."
	openssl ecparam -name prime256v1 -genkey -noout -out certs/ca-key.pem
	openssl pkcs8 -topk8 -nocrypt -in certs/ca-key.pem -out certs/ca-key-pkcs8.pem
	openssl req -new -x509 -key certs/ca-key-pkcs8.pem -out certs/ca-cert.pem -days 3650 -subj "/C=US/ST=California/L=San Francisco/O=Spooky Proxy CA/OU=Certificate Authority/CN=Spooky Proxy Root CA" -sha256
	@echo "🔐 Generating proxy private key..."
	openssl ecparam -name prime256v1 -genkey -noout -out certs/proxy-key.pem
	openssl pkcs8 -topk8 -nocrypt -in certs/proxy-key.pem -out certs/proxy-key-pkcs8.pem
	@echo "📄 Creating Certificate Signing Request (CSR)..."
	openssl req -new -key certs/proxy-key-pkcs8.pem -out certs/proxy.csr -config certs/san.conf -extensions v3_req
	@echo "🎯 Signing certificate with CA..."
	openssl x509 -req -in certs/proxy.csr -CA certs/ca-cert.pem -CAkey certs/ca-key-pkcs8.pem -CAcreateserial -out certs/proxy-cert.pem -days 365 -extensions v3_req -extfile certs/san.conf -sha256
	@echo "🔄 Converting to DER format..."
	openssl pkcs8 -in certs/proxy-key-pkcs8.pem -topk8 -nocrypt -outform DER -out certs/proxy-key.der
	openssl x509 -in certs/proxy-cert.pem -outform DER -out certs/proxy-cert.der
	@echo "📦 Creating full chain..."
	cat certs/proxy-cert.pem certs/ca-cert.pem > certs/proxy-fullchain.pem
	@echo "🔒 Setting secure permissions..."
	chmod 600 certs/proxy-key.pem certs/proxy-key-pkcs8.pem certs/proxy-key.der certs/ca-key.pem certs/ca-key-pkcs8.pem
	chmod 644 certs/proxy-cert.pem certs/proxy-cert.der certs/ca-cert.pem certs/proxy-fullchain.pem
	@echo "✅ CA-signed certificates created successfully!"

certs: certs-ca

certs-verify:
	@echo "🔍 Verifying certificate details..."
	openssl x509 -in certs/proxy-cert.pem -text -noout | head -20
	@echo ""
	@echo "🔍 Checking SAN extensions..."
	openssl x509 -in certs/proxy-cert.pem -text -noout | grep -A 10 "Subject Alternative Name"
	@echo ""
	@echo "📅 Checking validity period..."
	openssl x509 -in certs/proxy-cert.pem -noout -dates
	@echo ""
	@if [ -f certs/ca-cert.pem ]; then \
		echo "✅ Verifying against CA..."; \
		openssl verify -CAfile certs/ca-cert.pem certs/proxy-cert.pem; \
	fi

certs-clean:
	@echo "🧹 Cleaning certificate files..."
	rm -f certs/*

clean:
	rm -f target/release/spooky

docs-serve:
	mkdocs serve

docs-build:
	mkdocs build

docs-setup:
	pip install -r docs-requirements.txt --break-system-packages
	mkdocs build

bench-micro:
	./scripts/bench-micro.sh

bench-macro:
	./scripts/bench-macro.sh

bench-gate:
	./scripts/bench-gate.sh

bench-promote-baseline:
	@if [ -z "$(RELEASE)" ]; then \
		echo "usage: make bench-promote-baseline RELEASE=vX.Y.Z"; \
		exit 1; \
	fi
	./scripts/bench-promote-baseline.sh "$(RELEASE)"
