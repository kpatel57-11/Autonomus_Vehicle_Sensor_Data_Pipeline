# Architecture — AV Sensor Data Pipeline

## Overview

This pipeline processes **500M+ sensor records per day** from autonomous vehicle fleets.
It supports both **batch** (Airflow-triggered) and **streaming** (24/7 Kafka → Spark Structured Streaming) modes.

Key guarantees: **Exactly-Once semantics**, **Self-Healing**, **Config-Driven**, **Schema-Enforced**.

---

## Architecture Diagram

```
Vehicle Fleet (Onboard Sensors)
  ├── LIDAR         : 3D point clouds, ~300K pts/frame
  ├── Camera        : 8 surround cams, 30 fps each
  ├── GPS/IMU       : Position + motion, 100 Hz
  ├── Radar         : Object detection, 76–81 GHz
  ├── CAN Bus       : Speed, steering, brake, throttle
  └── Ultrasonic    : Proximity sensors, 12 units

         ▼ (raw sensor data)

Ingestion & Buffering Layer
  ├── Kafka Cluster
  │     ├── Topic: lidar_raw       (12 partitions)
  │     ├── Topic: camera_meta     (8 partitions)
  │     └── Topic: gps_stream      (4 partitions)
  ├── Object Storage (S3/HDFS)
  │     ├── ROS bags
  │     ├── Parquet dumps
  │     └── Protobuf archives
  └── Schema Registry (Avro/Protobuf schemas)

         ▼ (buffered data)

Configuration Layer
  ├── MongoDB       → Pipeline Recipes (JSON configs)
  ├── PostgreSQL    → Dataset Metadata (paths, schemas)
  └── Properties   → spark, site, env settings

         ▼ (configured pipeline)

Driver Layer  —  PipelineDriver.main()
  ├── PipelineDriver (abstract — Template Method)
  │     └── processPipeline(): 6-stage invariant
  ├── BatchDriver     : read → process → write → exit
  │     └── Triggered by Airflow DAGs
  └── StreamingDriver : readStream → foreachBatch → 24/7
        └── Auto-restart on failure

         ▼ (processsPipeline() iterates 6 stages)

┌──────────────────────────────────────────────────────────────────────┐
│  PIPELINE CONTAINER CHAIN                                            │
│                                                                      │
│  1.SOURCES → 2.VALIDATORS → 3.SQL TRANSFORMS → 4.PROCESSORS         │
│            → 5.SINK → 6.CHECKPOINT                                   │
└──────────────────────────────────────────────────────────────────────┘

         ▼ (processed data)

Downstream Consumers
  ├── ML Training      : PyTorch/TF reads HUDI tables
  ├── Perception Model : Object detection inference pipeline
  ├── Simulation       : Replay sensor data in simulator
  ├── Data Quality     : Dashboards & alerts via Grafana + Trino
  ├── Map Building     : HD map generation from point clouds
  └── Fleet Analytics  : Vehicle health & driving patterns
```

---

## Pipeline Stages (6-Stage Template Method)

### Stage 1 — SOURCES

**Factory:** `SourceReaderFactory.get_reader(type, mode)`

| Reader | Description |
|--------|-------------|
| `KafkaSourceReader` | Streaming from `lidar_raw`, `camera_meta`, `gps_stream` topics |
| `HDFSFileReader` (S3SourceReader) | Batch reads from S3/HDFS (Parquet/ORC) |
| `ROSBagReader` | Binary ROS bag files from field data collection |
| `JDBCSourceReader` | Metadata reads from PostgreSQL |

SchemaBuilder resolves StructType from Avro/Protobuf Schema Registry.

---

### Stage 2 — VALIDATORS

**Factory:** `ValidationFactory` — 10+ `IValidator` implementations

| Validator | Check |
|-----------|-------|
| `GPSBoundsCheck` | lat ∈ [−90,90], lon ∈ [−180,180] |
| `TimestampMonotonicity` | No backward jumps per vehicle_id |
| `LIDARIntensityRange` | intensity ∈ [0,255] |
| `IMUDriftDetector` | Gyroscope angular rate < threshold |
| `CameraExposureValidator` | exposure ∈ [0,1] |
| `RadarFrequencyValidator` | frequency_ghz ∈ [76,81] |
| `SpeedPlausibilityCheck` | speed ∈ [0,300] km/h |
| `PointCloudDensityValidator` | point_count ≥ minimum threshold |
| `CANBusMessageValidator` | DLC ∈ [0,8], can_id > 0 |
| `HeadingValidator` | heading ∈ [0,360) |

Invalid records → **Dead Letter Queue** (RabbitMQ + S3 storage).

---

### Stage 3 — SQL TRANSFORMS

**Engine:** `SqlManager` (Spark SQL execution engine)

```sql
-- Coordinate transform (geo → local Mercator projection)
SELECT *,
  6371000.0 * RADIANS(lon - origin_lon) * COS(RADIANS(origin_lat)) AS x_local,
  6371000.0 * RADIANS(lat - origin_lat) AS y_local,
  FLOOR(timestamp_ms / 100) * 100 AS time_bucket
FROM raw_data
WHERE lat IS NOT NULL AND lon IS NOT NULL

-- Temporal alignment (100ms buckets)
SELECT *, FLOOR(timestamp_ms / 100) * 100 AS time_bucket FROM df

-- Filter valid records
WHERE valid_gps = true AND intensity > 0

-- Window functions (time-series)
AVG(speed) OVER (PARTITION BY vehicle_id ORDER BY timestamp_ms ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
```

Streaming: `withWatermark()` for late-arriving sensor data.

---

### Stage 4 — PROCESSORS

**Factory:** `ProcessorFactory` — 12+ `IProcessor` implementations

| Processor | Description |
|-----------|-------------|
| `PointCloudStitcher` | Merges partial LIDAR sweeps → complete 360° frames |
| `FrameAligner` | Temporal alignment across sensors |
| `SensorFusion` | Combines LIDAR + camera + radar |
| `AnomalyDetector` | IQR-based outlier removal |
| `TrajectoryInterpolator` | GPS gap filling via linear interpolation |
| `OccupancyGridBuilder` | 2D/3D grid maps (cell_size=0.5m) |
| `VelocityEstimator` | Speed from consecutive GPS positions |
| `ObjectDetectionEnricher` | Enriches with detected object metadata |
| `LaneDetectionProcessor` | Lane boundary detection |
| `WeatherConditionClassifier` | Fog/rain/clear from sensor patterns |
| `HDMapMatcher` | Matches position to HD map road segments |
| `PredictiveMotionModel` | 2s position prediction (Kalman filter) |

Custom processors supported via **reflection-based dynamic loading** (`Class.forName` equivalent).

---

### Stage 5 — SINK

**Factory:** `SinkWriterFactory` — at-least-once + idempotent write = **exactly-once effect**

| Sink | Description |
|------|-------------|
| `HudiSinkWriter` | Upsert to Apache Hudi data lake (ACID, COPY_ON_WRITE) |
| `DeltaLakeSinkWriter` | Delta Lake ACID transactions, columnar files |
| `ParquetSinkWriter` | Columnar Parquet to S3/HDFS with partitioning |
| `APIPublisherSinkWriter` | REST POST to downstream services |
| `RabbitMQSinkWriter` | Publish events to `av.events` exchange |
| `HiveSinkWriter` | Repartition + write Hive metadata tables |
| `DLQWriter` | Dead letter queue (bad records) |

Streaming: `Repartition → Write → Hive metadata → commitGlobalBatch()`

---

### Stage 6 — CHECKPOINT

**Manager:** `CheckpointManager` (exactly-once: read prev offset → find new files → process → write)

```
Read prev offset → Find new files → Process → Write offset
```

- **Spark Streaming checkpoints**: offset logs on distributed storage
- **Kafka offsets**: tracked per topic/partition
- **Batch IDs**: idempotency guard — skip already-processed batches

---

## Self-Healing (Observer Pattern)

`QueryLifecycleMonitor` implements Spark `StreamingQueryListener`:

```python
onQueryStarted    → log + push metrics to Prometheus
onQueryProgress   → metrics + alert if batch_ms > 60s or input_rows == 0
onQueryTerminated → send PagerDuty/Slack alert + schedule auto-restart
```

**WatchdogMonitor** (daemon thread) — restarts query if no progress within 5 minutes.

**Exponential backoff restart**: delay = min(30 × attempt, 300s), max 30 attempts.

Self-healing actions:
- Publish to Message Queue
- Watchdog auto-restart (30s)
- PagerDuty / Slack alerts
- Dead letter queue for bad data
- Spark checkpoints + Kafka offset tracking

---

## Design Patterns

| Pattern | Where Used |
|---------|------------|
| **Template Method** | `PipelineDriver` → `BatchDriver` / `StreamingDriver` |
| **Factory (×4)** | `SourceReaderFactory`, `ValidationFactory`, `ProcessorFactory`, `SinkWriterFactory` |
| **Strategy** | `IProcessor`, `IValidator`, `ISinkWriter` (pluggable implementations) |
| **Observer** | `QueryLifecycleMonitor` (Spark StreamingQueryListener) |

---

## Infrastructure

| Component | Technology |
|-----------|-----------|
| Stream processing | Apache Spark 3.5 Structured Streaming |
| Batch orchestration | Apache Airflow 2.8 |
| Message broker | Apache Kafka 7.5 (Confluent) |
| Schema registry | Confluent Schema Registry (Avro/Protobuf) |
| Pipeline config | MongoDB 7.0 |
| Dataset metadata | PostgreSQL 16 |
| Event queue | RabbitMQ 3.13 |
| Data lake (ACID) | Apache Hudi + Delta Lake |
| Monitoring | Grafana + Prometheus |
| Container orchestration | Kubernetes / Apache YARN |
| CI/CD | GitHub Actions → Docker → K8s |

---

## Exactly-Once Semantics

Achieved via **at-least-once delivery + idempotent writes**:

1. **Kafka**: `auto.offset.reset=earliest`, manual offset commit only after write
2. **Hudi**: upsert with `record_key` + `precombine_field` → deduplication
3. **Delta Lake**: `MERGE INTO` transactions → atomic, idempotent
4. **Checkpoint**: `is_completed(batch_id)` guard prevents reprocessing

---

## Spark Configuration

```python
SparkConf:
  executor_instances: 15–50 (dynamic allocation)
  executor_memory:    12g each
  serializer:         KryoSerializer
  extensions:         DeltaSparkSessionExtension
  dynamic_allocation: True (shuffleTracking enabled)
```
