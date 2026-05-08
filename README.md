# 🚗 Autonomous Vehicle — Sensor Data Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?style=flat-square&logo=react&logoColor=black)
![Kafka](https://img.shields.io/badge/Kafka-Confluent_7.5-231F20?style=flat-square&logo=apache-kafka)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

> Production-grade data pipeline processing **500M+ sensor records/day** from autonomous vehicle fleets.
> Batch + Streaming · Exactly-Once · Self-Healing · Config-Driven · 6-Stage Template Method

---

## ✨ Features

| Feature | Details |
|---------|---------|
| 🚀 **Throughput** | 500M+ records/day across 6 sensor types |
| ⚡ **Dual Mode** | Batch (Airflow-triggered) + Streaming (24/7 Kafka) |
| 🎯 **Exactly-Once** | At-least-once + idempotent writes (Hudi/Delta Lake) |
| 🔄 **Self-Healing** | Observer pattern — auto-restart with exponential backoff |
| ⚙️ **Config-Driven** | Pipeline recipes from MongoDB, metadata from PostgreSQL |
| 🏭 **Design Patterns** | Template Method · Factory ×4 · Strategy · Observer |
| 📊 **React Dashboard** | Live monitoring, pipeline control, data catalog |
| 🐳 **Docker + K8s** | Full containerization, K8s manifests, HPA |
| 🔬 **pytest Suite** | Validators, factories, checkpoints, observer |
| 📈 **Observability** | Prometheus metrics + Grafana dashboards |

---

## 🗂️ Project Structure

```
av-pipeline/
├── backend/
│   ├── core/
│   │   ├── interfaces.py            # ABC contracts: ISourceReader, IValidator, IProcessor, ISinkWriter
│   │   └── pipeline_driver.py       # Template Method: PipelineDriver → BatchDriver / StreamingDriver
│   ├── config/
│   │   ├── settings.py              # Kafka, Spark, Storage, Mongo, Postgres, RabbitMQ
│   │   └── config_loader.py         # Recipe loading from MongoDB (fallback to defaults)
│   ├── ingestion/source_factory.py  # Factory: Kafka, S3/HDFS, ROSBag, JDBC readers
│   ├── validators/validation_factory.py  # 10+ validators: GPS, LIDAR, IMU, Camera, Radar…
│   ├── transforms/sql_manager.py    # Spark SQL: coordinate transform, temporal alignment, windowing
│   ├── processors/processor_factory.py  # 12+ processors: PointCloud, SensorFusion, Anomaly…
│   ├── sink/sink_factory.py         # Hudi, Delta Lake, Parquet, API, RabbitMQ, Hive, DLQ
│   ├── checkpoint/checkpoint_manager.py # Exactly-once offset tracking
│   ├── self_healing/query_lifecycle_monitor.py  # Observer: auto-restart, alerts
│   ├── api/main.py                  # FastAPI: /api/pipeline, /api/metrics, /api/catalog
│   ├── airflow/dags/av_batch_dag.py # Airflow DAG: check_kafka → run → DQ → notify
│   ├── tests/test_pipeline.py       # pytest: 20+ test cases
│   └── requirements.txt
│
├── frontend/                        # React 18 + TypeScript dashboard
│   ├── src/
│   │   ├── App.tsx                  # Router: 5 pages
│   │   ├── store/pipelineStore.ts   # Zustand state management
│   │   ├── utils/api.ts             # REST client + mock data fallback
│   │   ├── components/
│   │   │   ├── common/              # Sidebar, MetricCard, StatusBadge, SectionHeader
│   │   │   ├── dashboard/SensorFleet.tsx  # 6 live-pulsing sensors
│   │   │   ├── pipeline/            # PipelineFlowChart (animated), RunHistory
│   │   │   └── monitoring/          # ThroughputChart, ValidatorStats, ProcessorMetrics
│   │   └── pages/
│   │       ├── Dashboard.tsx        # KPIs + fleet + downstream consumers
│   │       ├── PipelinePage.tsx     # Launch + 6-stage progress animation
│   │       ├── MonitoringPage.tsx   # Live recharts + self-healing hooks
│   │       ├── ConfigPage.tsx       # Recipe builder + JSON preview
│   │       └── CatalogPage.tsx      # Dataset browser + schema panel
│   ├── vite.config.ts
│   └── package.json
│
├── docker/
│   ├── docker-compose.yml           # 10 services: Kafka, Spark, Postgres, Mongo, Grafana…
│   ├── Dockerfile.backend           # Multi-stage Python + OpenJDK
│   ├── Dockerfile.frontend          # Node build → nginx:alpine
│   └── prometheus.yml
│
├── k8s/
│   ├── backend/deployment.yaml      # Deployment, Service, HPA (2-10 replicas)
│   ├── frontend/deployment.yaml     # Deployment, Service, Ingress
│   └── infra/
│       ├── base.yaml                # Namespace, RBAC, ConfigMap, Secrets, PVCs
│       └── spark-job.yaml           # SparkApplication CRD (15-50 dynamic executors)
│
├── scripts/
│   ├── setup_kafka.sh               # Create all Kafka topics with partitions
│   ├── seed_config.py               # Seed MongoDB recipes + PostgreSQL metadata
│   ├── run_pipeline.py              # CLI entrypoint with argparse
│   ├── init_postgres.sql            # Schema + initial data
│   └── init_mongo.js                # Collection + indexes + seed recipe
│
├── docs/architecture.md             # Full architecture documentation
├── .github/workflows/ci.yml         # CI: lint → test → Docker → K8s deploy
├── .env.example
└── README.md
```

---

## 🚀 Quick Start

### Option A: Docker Compose (Recommended)

```bash
git clone https://github.com/yourusername/av-pipeline.git
cd av-pipeline

# Start all 10 services
cd docker && docker-compose up -d

# Wait for services, then seed config
cd .. && python scripts/seed_config.py

# Open dashboard
open http://localhost:3000
```

### Option B: Local Development

```bash
# Backend
cd backend
pip install -r requirements.txt
PYTHONPATH=. uvicorn api.main:app --reload --port 8000

# Frontend (new terminal)
cd frontend
npm install && npm run dev   # → http://localhost:3000

# Run pipeline
python scripts/run_pipeline.py --mode batch
```

> **Note:** All services degrade gracefully — the pipeline runs with synthetic data if Kafka/S3/MongoDB are unavailable. The frontend uses mock data if the API is offline.

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:3000 | — |
| API + Swagger | http://localhost:8000/docs | — |
| Grafana | http://localhost:3001 | admin / avpassword |
| Airflow | http://localhost:8080 | admin / avpassword |
| RabbitMQ UI | http://localhost:15672 | avuser / avpassword |
| Prometheus | http://localhost:9090 | — |

---

## 🧪 Tests

```bash
cd backend
PYTHONPATH=. pytest tests/ -v --cov=. --cov-report=term-missing
```

Test coverage:
- **Validators** — GPSBoundsCheck, TimestampMonotonicity, LIDARIntensityRange, IMUDriftDetector
- **Config** — recipe loading, all-stages validation
- **Factories** — ValidationFactory, ProcessorFactory, SourceReaderFactory, SinkWriterFactory
- **Checkpoint** — save/load offset, mark/check complete, list
- **Observer** — QueryLifecycleMonitor metrics summary

---

## 🔌 API Quick Reference

```bash
# Launch batch pipeline
curl -X POST http://localhost:8000/api/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"mode":"batch","recipe_name":"av_full_pipeline"}'

# Get metrics
curl http://localhost:8000/api/metrics

# List available processors
curl http://localhost:8000/api/config/processors

# View checkpoints
curl http://localhost:8000/api/checkpoints
```

---

## 🏭 Design Patterns

### Template Method
```python
class PipelineDriver(ABC):
    def processPipeline(self):     # invariant 6-stage skeleton
        df = self._read_sources()        # 1
        df, dlq = self._run_validators() # 2
        df = self._run_transforms()      # 3
        df = self._run_processors()      # 4
        self._write_sinks(df)            # 5
        self._run_checkpoint()           # 6
```

### Factory (×4 — all pluggable)
```python
SourceReaderFactory().get_reader("kafka_lidar")     # → KafkaSourceReader
ValidationFactory().get_validator("GPSBoundsCheck") # → GPSBoundsCheck
ProcessorFactory().get_processor("SensorFusion")    # → SensorFusion
SinkWriterFactory().get_writer("hudi_data_lake")    # → HudiSinkWriter
```

### Observer (Self-Healing)
```python
class QueryLifecycleMonitor(StreamingQueryListener):
    def onQueryStarted(self, event):    # log + metrics
    def onQueryProgress(self, event):   # alert if lag > 60s
    def onQueryTerminated(self, event): # PagerDuty alert + auto-restart
```

---

## ➕ Add a Custom Processor

```python
# 1. Implement IProcessor
class MyRadarEnricher(IProcessor):
    @property
    def processor_name(self): return "MyRadarEnricher"
    def process(self, df, config) -> ProcessingResult:
        df = df.withColumn("radar_zone", ...)
        return ProcessingResult(True, self.processor_name, df)

# 2. Register
ProcessorFactory().register("MyRadarEnricher", MyRadarEnricher)

# 3. Add to recipe (MongoDB or API)
recipe["processors"].append("MyRadarEnricher")

# Or use dynamic loading (no registration needed)
recipe["processors"].append("mypackage.processors.MyRadarEnricher")
```

---

## ☸️ Kubernetes

```bash
kubectl apply -f k8s/infra/base.yaml
kubectl apply -f k8s/backend/deployment.yaml
kubectl apply -f k8s/frontend/deployment.yaml
kubectl apply -f k8s/infra/spark-job.yaml   # Spark batch job
kubectl get pods -n av-pipeline
```

---

Architecture designed by **Kishan Patel**  
Built with Apache Spark · Kafka · Hudi · Delta Lake · FastAPI · React · Docker · Kubernetes
