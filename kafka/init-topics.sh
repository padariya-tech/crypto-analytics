#!/bin/bash

# Initialize Kafka topics for Crypto Analytics System
# This script creates all necessary topics with proper configuration

set -e

BOOTSTRAP_SERVER=${1:-"localhost:9092"}

echo "🔧 Initializing Kafka Topics..."
echo "Bootstrap Server: $BOOTSTRAP_SERVER"
echo ""

# Function to create topic
create_topic() {
    local topic=$1
    local partitions=$2
    local replication=$3
    
    echo "📌 Creating topic: $topic"
    echo "   Partitions: $partitions, Replication: $replication"
    
    # Check if topic exists
    if kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --describe --topic $topic &>/dev/null; then
        echo "   ✓ Topic already exists"
    else
        kafka-topics.sh \
            --create \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --topic $topic \
            --partitions $partitions \
            --replication-factor $replication \
            --config retention.ms=86400000 \
            --if-not-exists
        echo "   ✓ Topic created"
    fi
    echo ""
}

# Create topics with appropriate configuration

# 1. Raw trade events (partitioned by symbol)
create_topic "crypto-trades" 3 1

# 2. Processed analytics (partitioned by symbol)
create_topic "processed-market-data" 3 1

# 3. Price alerts (single partition to maintain order)
create_topic "price-alerts" 1 1

# 4. Dead Letter Queue for anomalies/errors
create_topic "anomalies" 1 1

echo "✅ All topics initialized successfully!"
echo ""
echo "Topic Summary:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER --describe