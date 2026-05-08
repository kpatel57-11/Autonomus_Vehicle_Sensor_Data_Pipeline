#!/usr/bin/env bash
# scripts/setup_kafka.sh
# Creates all required Kafka topics for the AV Sensor Data Pipeline
# Usage: ./scripts/setup_kafka.sh [bootstrap-server]

set -euo pipefail

BOOTSTRAP="${1:-localhost:9092}"
REPLICATION=1
echo "╔══════════════════════════════════════════════════════════╗"
echo "║    AV Pipeline — Kafka Topic Setup                       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Bootstrap server: $BOOTSTRAP"
echo ""

wait_kafka() {
  echo "⏳ Waiting for Kafka to be ready..."
  for i in $(seq 1 30); do
    if kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" &>/dev/null; then
      echo "✅ Kafka is ready"
      return 0
    fi
    echo "   Attempt $i/30 — sleeping 5s..."
    sleep 5
  done
  echo "❌ Kafka did not become ready"
  exit 1
}

create_topic() {
  local name=$1
  local partitions=$2
  local retention_ms=${3:-604800000}   # 7 days default
  echo -n "  Creating topic '$name' (partitions=$partitions)... "
  kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION" \
    --config retention.ms="$retention_ms" \
    --config max.message.bytes=10485760 \
    && echo "✅" || echo "⚠️  (may already exist)"
}

wait_kafka

echo ""
echo "📦 Creating sensor data topics..."
create_topic "lidar_raw"   12  604800000   # 12 partitions — high throughput
create_topic "camera_meta"  8  604800000
create_topic "gps_stream"   4  259200000   # 3 days — smaller volume
create_topic "radar_raw"    6  604800000
create_topic "can_bus_raw"  4  86400000    # 1 day — high frequency, small msgs
create_topic "ultrasonic"   2  86400000

echo ""
echo "📦 Creating pipeline control topics..."
create_topic "av_dlq"       4  2592000000  # 30 days — dead letter queue
create_topic "av_events"    4  604800000
create_topic "av_checkpoints" 1 -1          # Compact — no expiry

echo ""
echo "📋 Current topics:"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | grep -E "(lidar|camera|gps|radar|can|ultra|av_)" | sort

echo ""
echo "✅ Kafka setup complete!"
