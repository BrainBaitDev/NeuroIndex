# Multi-stage build for minimal production image
FROM rust:1.75-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source
COPY . .

# Build release binaries
RUN cargo build --release

# Production stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash neuroindex

# Copy binaries from builder
COPY --from=builder /build/target/release/neuroindex-http /usr/local/bin/
COPY --from=builder /build/target/release/neuroindex-resp-server /usr/local/bin/
COPY --from=builder /build/target/release/neuroindex-cli /usr/local/bin/

# Create data directory
RUN mkdir -p /data && chown neuroindex:neuroindex /data

# Switch to non-root user
USER neuroindex

# Volume for persistence
VOLUME ["/data"]

# Expose ports
EXPOSE 8080 6381

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD neuroindex-cli -p 6381 PING || exit 1

# Default command (HTTP server)
CMD ["neuroindex-http", \
     "--host", "0.0.0.0", \
     "--port", "8080", \
     "--shards", "16", \
     "--persistence-dir", "/data", \
     "--snapshot-interval", "60"]
