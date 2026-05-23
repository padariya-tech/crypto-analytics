# Real-Time Crypto Market Analytics System

A complete intermediate-level Kafka learning project. Stream live cryptocurrency market data, process it through Apache Kafka, and display real-time analytics on a modern dashboard.

## Architecture Overview

```
Binance WebSocket API
    ↓
[Kafka Producer] - Validates & routes live trades
    ↓
Kafka Topics:
├── crypto-trades (partitioned by symbol)
├── price-alerts
├── processed-market-data
└── anomalies
    ↓
[Multiple Kafka Consumers]
├── Consumer 1: Analytics (moving averages, volume spikes)
├── Consumer 2: Alert Engine (price changes, anomalies)
└── Consumer 3: Storage (Redis + PostgreSQL)
    ↓
[FastAPI Backend] - REST APIs + WebSocket server
    ↓
[React Frontend] - Real-time dashboard with charts
```

## Key Features

✅ **Real-Time Data Streaming** - Live cryptocurrency price feeds via Binance WebSocket API
✅ **Kafka Event Architecture** - Producer, consumer groups, partitions, retry logic, DLQ
✅ **Multiple Consumer Patterns** - Analytics, alerts, storage processors
✅ **WebSocket Real-Time Updates** - Instant frontend updates via async WebSockets
✅ **Modern Dashboard** - Live charts, trade feeds, alerts, Kafka metrics
✅ **Redis Caching** - Fast access to latest prices and analytics
✅ **Docker Compose** - One-command local setup with all services
✅ **Free Cloud Deployment** - Instructions for Render, Railway, Vercel, Netlify
✅ **Testing Suite** - pytest for producers, consumers, APIs
✅ **Production-Ready Code** - Modular, documented, error handling

## Tech Stack

**Backend:**
- Python 3.11+
- FastAPI (REST APIs + WebSockets)
- aiokafka (async Kafka client)
- Redis (caching)
- PostgreSQL (optional data persistence)

**Frontend:**
- React 18 + TypeScript
- Vite (fast development)
- TailwindCSS (styling)
- Recharts (real-time charting)
- WebSockets (live updates)

**Infrastructure:**
- Apache Kafka + Zookeeper
- Docker & Docker Compose
- Free cloud: Render, Railway, Vercel, Netlify

## Quick Start (Local Development)

### Prerequisites

- Docker & Docker Compose installed
- Python 3.11+ (for local backend development)
- Node.js 18+ (for local frontend development)

### 1. Clone and Setup

```bash
# Clone the project
git clone <repo-url>
cd crypto-analytics

# Start all services with Docker Compose
docker-compose up -d

# Wait 30 seconds for Kafka to initialize
sleep 30

# Check services
docker-compose ps
```

### 2. Access the System

- **Frontend:** http://localhost:3000
- **FastAPI Docs:** http://localhost:8000/docs
- **Kafka UI (optional):** http://localhost:8080

### 3. View Logs

```bash
# Backend logs
docker-compose logs -f backend

# Kafka logs
docker-compose logs -f kafka

# Frontend logs
docker-compose logs -f frontend
```

### 4. Stop Everything

```bash
docker-compose down

# Also remove volumes (fresh start)
docker-compose down -v
```

## Local Development (Without Docker)

### Backend Setup

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Kafka & Redis with Docker only
docker-compose -f docker-compose.dev.yml up -d kafka zookeeper redis

# Run backend
python main.py

# Backend runs on http://localhost:8000
```

### Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start dev server
npm run dev

# Frontend runs on http://localhost:5173
```

## Project Structure

```
crypto-analytics/
├── backend/
│   ├── main.py                 # FastAPI entry point
│   ├── requirements.txt         # Python dependencies
│   ├── producer/
│   │   ├── binance_producer.py # Binance WebSocket producer
│   │   └── message_schema.py   # Message validation
│   ├── consumers/
│   │   ├── analytics_consumer.py    # Moving averages, volume
│   │   ├── alert_consumer.py        # Price change alerts
│   │   └── storage_consumer.py      # Redis/PostgreSQL storage
│   ├── websocket/
│   │   ├── manager.py          # WebSocket connection management
│   │   └── handlers.py         # Message broadcasting
│   ├── analytics/
│   │   ├── calculator.py       # Analytics logic
│   │   └── indicators.py       # Technical indicators
│   ├── alerts/
│   │   ├── engine.py           # Alert generation
│   │   └── rules.py            # Alert rules
│   ├── api/
│   │   ├── routes.py           # REST endpoints
│   │   └── schemas.py          # Request/response models
│   ├── db/
│   │   ├── postgres.py         # PostgreSQL client
│   │   └── redis_client.py     # Redis cache
│   ├── config.py               # Configuration
│   ├── tests/
│   │   ├── test_producer.py
│   │   ├── test_consumers.py
│   │   └── test_api.py
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── main.tsx            # React entry point
│   │   ├── App.tsx             # Main app
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx    # Main dashboard
│   │   │   ├── TradeFeed.tsx    # Live trade stream
│   │   │   └── Analytics.tsx    # Analytics dashboard
│   │   ├── components/
│   │   │   ├── PriceCard.tsx
│   │   │   ├── Chart.tsx
│   │   │   ├── AlertsPanel.tsx
│   │   │   └── TradeList.tsx
│   │   ├── hooks/
│   │   │   └── useWebSocket.ts  # WebSocket hook
│   │   ├── types/
│   │   │   └── index.ts         # TypeScript types
│   │   └── styles/
│   │       └── tailwind.css
│   ├── package.json
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── tailwind.config.js
│   ├── Dockerfile
│   └── .dockerignore
├── docker-compose.yml          # Production compose
├── docker-compose.dev.yml      # Development compose
├── kafka/
│   ├── broker-config.properties
│   └── init-topics.sh           # Topic initialization script
└── docs/
    ├── KAFKA_GUIDE.md          # Kafka concepts explained
    ├── ARCHITECTURE.md         # System architecture
    ├── DEPLOYMENT.md           # Cloud deployment guide
    └── LEARNING_NOTES.md       # Learning outcomes
```

## Kafka Concepts Explained

### Topics & Partitions

- **crypto-trades**: Partitioned by symbol (BTC, ETH, SOL) for parallel processing
- **price-alerts**: Single partition, ordered alerts
- **processed-market-data**: Analytics results
- **anomalies**: DLQ for suspicious patterns

Partitioning allows different cryptos to be processed in parallel.

### Consumer Groups

- **analytics-group**: Processes all trades for analytics
- **alerts-group**: Generates price alerts
- **storage-group**: Persists processed data

Each consumer group reads independently, enabling multiple processing pipelines.

### Message Ordering & Offsets

- Messages within a partition maintain order
- Offset = position in partition stream
- Consumer tracks offset to resume from crashes
- Rebalancing: when consumers join/leave a group

### Retry Handling & Dead Letter Queue

- Producer retries failed publishes
- Consumer retries failed processing
- Anomalies/errors go to DLQ for investigation

## Learning Outcomes

After building this project, you'll understand:

✅ **Event Streaming Architecture** - Real-time data pipelines
✅ **Kafka Producer Pattern** - Data ingestion with validation
✅ **Kafka Consumer Patterns** - Multiple processing styles
✅ **Consumer Groups** - Parallel stream processing
✅ **Partition Strategy** - Scaling and parallelism
✅ **WebSocket Real-Time Updates** - Async communication
✅ **Async Python** - asyncio, aiokafka, FastAPI
✅ **Docker Orchestration** - Multi-service deployments
✅ **Cloud Deployment** - Serverless & managed services
✅ **Stream Processing** - Windowing, aggregation, state

## Kafka Key Concepts

### Producer

```python
# Validates incoming trade data
# Partitions by crypto symbol for parallel processing
# Handles reconnection and retries
await producer.send_and_wait(
    "crypto-trades",
    value=validated_trade_message,
    key=symbol.encode()  # Key = partition determinism
)
```

### Consumer

```python
# Subscribes to topics
# Processes messages in order within partition
# Commits offset after processing (exactly-once semantics)
async for message in consumer:
    process_message(message)
    await consumer.commit()
```

### Offset Management

- **Auto-commit**: Periodic offset commits (simple, risky)
- **Manual-commit**: Commit after processing (recommended)
- **Seek**: Resume from specific offset

## Cloud Deployment

### Backend (Render or Railway)

```bash
# Render
1. Connect your Git repo to Render
2. Create a Web Service
3. Set environment variables
4. Deploy with docker-compose or Dockerfile

# Railway
1. Connect GitHub repo
2. Create service
3. Auto-deploys on push
```

### Frontend (Vercel or Netlify)

```bash
# Vercel
vercel deploy

# Netlify
netlify deploy --prod
```

### Kafka (Confluent Cloud)

Free tier provides:
- 1 GB ingress + egress per month
- Sufficient for learning/testing
- Configure bootstrap servers in environment

See DEPLOYMENT.md for detailed instructions.

## Configuration

### Environment Variables

**Backend (.env)**
```
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
REDIS_URL=redis://redis:6379
DATABASE_URL=postgresql://user:pass@postgres:5432/crypto
BINANCE_STREAM_URL=wss://stream.binance.com:9443/ws
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
```

**Frontend (.env.local)**
```
VITE_API_URL=http://localhost:8000
VITE_WS_URL=ws://localhost:8000
```

## Testing

```bash
# Run all tests
pytest

# Specific test
pytest backend/tests/test_producer.py

# With coverage
pytest --cov=backend tests/
```

## Monitoring & Debugging

### Kafka Metrics

```bash
# Check topic details
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic crypto-trades

# Consumer group lag
docker-compose exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group analytics-group --describe
```

### Logs

```bash
# Real-time logs
docker-compose logs -f backend

# Last 100 lines
docker-compose logs --tail=100 backend
```

## Next Steps

1. **Run locally** - Get familiar with the system
2. **Modify alert rules** - Experiment with different thresholds
3. **Add new consumers** - Build your own processing pipeline
4. **Deploy to cloud** - Use Render, Railway, Vercel
5. **Scale up** - Add more topics, consumers, or data sources

## Troubleshooting

### Kafka won't start
```bash
# Fresh Kafka start
docker-compose down -v
docker-compose up -d kafka zookeeper
sleep 30
```

### Producer not sending data
```bash
# Check Binance connectivity
curl -i https://api.binance.com/api/v3/ping

# Check Kafka broker
docker-compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### Frontend can't connect to backend
```bash
# Check CORS in FastAPI
# Check WebSocket URL in frontend env
# Check backend is running: curl http://localhost:8000/health
```

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [aiokafka Python Client](https://aiokafka.readthedocs.io/)
- [FastAPI WebSockets](https://fastapi.tiangolo.com/advanced/websockets/)
- [Binance WebSocket Streams](https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams)

## License

MIT - Feel free to use this for learning and projects.

## Contributing

Suggestions and improvements welcome! This is an educational project meant to be forked and extended.

---

**Built with ❤️ as an intermediate Kafka learning project**