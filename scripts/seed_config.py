#!/usr/bin/env python3
"""
scripts/seed_config.py
Seeds MongoDB pipeline recipes and PostgreSQL dataset metadata.
Run once after infrastructure is up:
    python scripts/seed_config.py
"""
import json, sys, time
from datetime import datetime

# ── MongoDB seed ───────────────────────────────────────────────────────────────
RECIPES = [
    {
        "name": "av_full_pipeline",
        "version": "2.0",
        "description": "Full AV sensor data pipeline — all 6 sensor types",
        "sources": ["kafka_lidar", "kafka_camera", "kafka_gps", "s3_radar"],
        "validators": [
            "GPSBoundsCheck", "TimestampMonotonicity", "LIDARIntensityRange",
            "IMUDriftDetector", "CameraExposureValidator", "RadarFrequencyValidator",
            "SpeedPlausibilityCheck", "PointCloudDensityValidator",
            "CANBusMessageValidator", "HeadingValidator",
        ],
        "transforms": [
            {"sql": "coordinate_transform", "params": {"geo_to_local": True, "origin_lat": 37.7749, "origin_lon": -122.4194}},
            {"sql": "temporal_alignment",   "params": {"bucket_ms": 100}},
            {"sql": "filter_valid",         "params": {"intensity_gt_0": True}},
            {"sql": "add_processing_timestamp", "params": {}},
            {"sql": "window_aggregate",     "params": {"window_rows": 10}},
        ],
        "processors": [
            "PointCloudStitcher", "FrameAligner", "SensorFusion",
            "AnomalyDetector", "TrajectoryInterpolator", "OccupancyGridBuilder",
            "VelocityEstimator", "ObjectDetectionEnricher", "LaneDetectionProcessor",
            "WeatherConditionClassifier", "HDMapMatcher", "PredictiveMotionModel",
        ],
        "sinks": ["hudi_data_lake", "delta_lake", "api_publisher", "rabbitmq_sink"],
        "mode": "batch",
        "extra": {
            "watermark_delay": "10 minutes",
            "hudi_path": "/tmp/av/hudi/sensor_data",
            "delta_path": "/tmp/av/delta",
        },
        "created_at": datetime.utcnow().isoformat(),
        "active": True,
    },
    {
        "name": "av_lidar_only",
        "version": "1.0",
        "description": "LIDAR-only pipeline for point cloud processing",
        "sources": ["kafka_lidar", "s3_lidar"],
        "validators": ["LIDARIntensityRange", "PointCloudDensityValidator", "TimestampMonotonicity"],
        "transforms": [
            {"sql": "coordinate_transform", "params": {}},
            {"sql": "filter_valid", "params": {}},
        ],
        "processors": ["PointCloudStitcher", "AnomalyDetector", "OccupancyGridBuilder"],
        "sinks": ["hudi_data_lake", "parquet"],
        "mode": "batch",
        "extra": {},
        "created_at": datetime.utcnow().isoformat(),
        "active": True,
    },
    {
        "name": "av_streaming_realtime",
        "version": "2.0",
        "description": "Real-time streaming pipeline — 24/7 auto-restart",
        "sources": ["kafka_lidar", "kafka_camera", "kafka_gps"],
        "validators": ["GPSBoundsCheck", "TimestampMonotonicity", "LIDARIntensityRange"],
        "transforms": [
            {"sql": "coordinate_transform", "params": {}},
            {"sql": "add_processing_timestamp", "params": {}},
        ],
        "processors": ["SensorFusion", "AnomalyDetector", "TrajectoryInterpolator"],
        "sinks": ["delta_lake", "rabbitmq_sink"],
        "mode": "stream",
        "extra": {"watermark_delay": "5 minutes"},
        "created_at": datetime.utcnow().isoformat(),
        "active": True,
    },
]

# ── PostgreSQL seed ────────────────────────────────────────────────────────────
DATASETS = [
    {"name": "lidar_raw",       "path": "s3://av-sensor-data/lidar/",     "format": "parquet",    "size_gb": 2400, "record_count": 300000000, "freshness_minutes": 15,  "schema_version": "3"},
    {"name": "camera_meta",     "path": "s3://av-sensor-data/camera/",    "format": "parquet",    "size_gb": 890,  "record_count": 80000000,  "freshness_minutes": 5,   "schema_version": "2"},
    {"name": "gps_stream",      "path": "s3://av-sensor-data/gps/",       "format": "delta",      "size_gb": 45,   "record_count": 10000000,  "freshness_minutes": 1,   "schema_version": "4"},
    {"name": "radar_points",    "path": "s3://av-sensor-data/radar/",     "format": "orc",        "size_gb": 320,  "record_count": 50000000,  "freshness_minutes": 30,  "schema_version": "1"},
    {"name": "fused_perception","path": "s3://av-sensor-data/fused/",     "format": "hudi",       "size_gb": 1100, "record_count": 200000000, "freshness_minutes": 30,  "schema_version": "5"},
    {"name": "occupancy_grid",  "path": "s3://av-sensor-data/occupancy/", "format": "delta",      "size_gb": 670,  "record_count": 120000000, "freshness_minutes": 60,  "schema_version": "2"},
]


def seed_mongodb():
    print("🍃 Seeding MongoDB pipeline recipes...")
    try:
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure

        client = MongoClient("mongodb://avadmin:avpassword@localhost:27017/av_pipeline?authSource=admin",
                             serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["av_pipeline"]
        col = db["pipeline_recipes"]

        col.drop()   # fresh seed
        col.create_index("name", unique=True)
        result = col.insert_many(RECIPES)
        print(f"  ✅ Inserted {len(result.inserted_ids)} recipes")

        # Create indexes for fast lookup
        col.create_index([("active", 1), ("mode", 1)])
        print("  ✅ Indexes created")

    except Exception as e:
        print(f"  ⚠️  MongoDB unavailable: {e}")
        print("  💡  Writing recipes to recipes_seed.json instead")
        with open("recipes_seed.json", "w") as f:
            json.dump(RECIPES, f, indent=2, default=str)
        print("  ✅  Written to recipes_seed.json")


def seed_postgres():
    print("\n🐘 Seeding PostgreSQL dataset metadata...")
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="localhost", port=5432, dbname="av_metadata",
            user="avuser", password="avpassword"
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dataset_metadata (
                id              SERIAL PRIMARY KEY,
                name            VARCHAR(128) UNIQUE NOT NULL,
                path            TEXT NOT NULL,
                format          VARCHAR(32),
                size_gb         INTEGER,
                record_count    BIGINT,
                freshness_minutes INTEGER,
                schema_version  VARCHAR(16),
                created_at      TIMESTAMP DEFAULT NOW(),
                updated_at      TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id              VARCHAR(64) PRIMARY KEY,
                recipe_name     VARCHAR(128),
                mode            VARCHAR(16),
                status          VARCHAR(32),
                records_read    BIGINT,
                records_valid   BIGINT,
                records_processed BIGINT,
                records_rejected BIGINT,
                elapsed_s       FLOAT,
                started_at      TIMESTAMP DEFAULT NOW(),
                finished_at     TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint_log (
                id              SERIAL PRIMARY KEY,
                source          VARCHAR(128),
                offset_json     TEXT,
                saved_at        TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()

        for ds in DATASETS:
            cur.execute("""
                INSERT INTO dataset_metadata (name, path, format, size_gb, record_count, freshness_minutes, schema_version)
                VALUES (%(name)s, %(path)s, %(format)s, %(size_gb)s, %(record_count)s, %(freshness_minutes)s, %(schema_version)s)
                ON CONFLICT (name) DO UPDATE SET
                    path = EXCLUDED.path, format = EXCLUDED.format,
                    size_gb = EXCLUDED.size_gb, updated_at = NOW();
            """, ds)
        conn.commit()
        cur.close(); conn.close()
        print(f"  ✅ Inserted/updated {len(DATASETS)} dataset records")

    except Exception as e:
        print(f"  ⚠️  PostgreSQL unavailable: {e}")
        print("  💡  Writing dataset metadata to datasets_seed.json instead")
        with open("datasets_seed.json", "w") as f:
            json.dump(DATASETS, f, indent=2)
        print("  ✅  Written to datasets_seed.json")


def seed_rabbitmq():
    print("\n🐇 Setting up RabbitMQ exchanges and queues...")
    try:
        import pika
        conn = pika.BlockingConnection(pika.ConnectionParameters(
            host="localhost", port=5672,
            credentials=pika.PlainCredentials("avuser", "avpassword")
        ))
        ch = conn.channel()
        ch.exchange_declare(exchange="av.events", exchange_type="topic", durable=True)
        ch.exchange_declare(exchange="av.dlq",    exchange_type="direct", durable=True)
        ch.queue_declare(queue="av.dead.letter", durable=True, arguments={"x-message-ttl": 2592000000})
        ch.queue_declare(queue="av.ml.training",  durable=True)
        ch.queue_declare(queue="av.simulation",   durable=True)
        ch.queue_declare(queue="av.fleet.analytics", durable=True)
        ch.queue_bind(exchange="av.dlq",    queue="av.dead.letter",    routing_key="av.dlq")
        ch.queue_bind(exchange="av.events", queue="av.ml.training",    routing_key="av.sensor.processed")
        ch.queue_bind(exchange="av.events", queue="av.simulation",     routing_key="av.sensor.processed")
        ch.queue_bind(exchange="av.events", queue="av.fleet.analytics",routing_key="av.sensor.processed")
        conn.close()
        print("  ✅ RabbitMQ exchanges/queues configured")
    except Exception as e:
        print(f"  ⚠️  RabbitMQ unavailable: {e}")


if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════╗")
    print("║    AV Pipeline — Config & Metadata Seeder                ║")
    print("╚══════════════════════════════════════════════════════════╝\n")
    seed_mongodb()
    seed_postgres()
    seed_rabbitmq()
    print("\n✅ Seeding complete! The pipeline is ready to run.")
    print("\nNext steps:")
    print("  1. cd docker && docker-compose up -d")
    print("  2. python scripts/seed_config.py")
    print("  3. Open http://localhost:3000")
