"""
config/config_manager.py
Configuration Layer: MongoDB (pipeline recipes), PostgreSQL (dataset metadata),
Properties files (spark, site, env). Mirrors the diagram's Configuration Layer.
"""
from __future__ import annotations
import os
import json
from pathlib import Path
from typing import Any, Dict, Optional
from functools import lru_cache

from pydantic import BaseModel
from pydantic_settings import BaseSettings
from loguru import logger

try:
    import motor.motor_asyncio as motor
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_DB = True
except ImportError:
    HAS_DB = False
    logger.warning("DB drivers not installed; using mock config.")


# ─────────────────────────────────────────────────────────────────────────────
# Settings (from .env / environment)
# ─────────────────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    # MongoDB — Pipeline Recipes (JSON configs)
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "av_pipeline"
    mongo_collection_recipes: str = "pipeline_recipes"

    # PostgreSQL — Dataset Metadata (paths, schemas)
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str = "av_user"
    pg_password: str = "av_password"
    pg_db: str = "av_metadata"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_group_id: str = "av-pipeline-group"

    # Spark
    spark_master: str = "local[*]"
    spark_app_name: str = "AVSensorPipeline"
    spark_executor_memory: str = "12g"
    spark_executor_cores: int = 4
    spark_num_executors: int = 15
    spark_dynamic_allocation: bool = True
    spark_kryo_serializer: bool = True

    # S3 / Object Storage
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket_raw: str = "av-raw"
    s3_bucket_processed: str = "av-processed"

    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_dlq_queue: str = "sensor_dlq"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_secret_key: str = "change-me-in-production"
    cors_origins: str = "http://localhost:3000"

    # Monitoring
    prometheus_port: int = 8001
    grafana_url: str = "http://localhost:3001"

    # Pipeline
    pipeline_mode: str = "batch"
    checkpoint_dir: str = "/tmp/av-checkpoints"
    dlq_max_retries: int = 3

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


# ─────────────────────────────────────────────────────────────────────────────
# MongoDB Config Manager — Pipeline Recipes
# ─────────────────────────────────────────────────────────────────────────────

class MongoConfigManager:
    """
    Manages pipeline recipes stored as JSON configs in MongoDB.
    Diagram: MongoDB → Pipeline Recipes (JSON configs)
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._client = None
        self._db = None

    async def connect(self):
        if not HAS_DB:
            return
        self._client = motor.AsyncIOMotorClient(self.settings.mongo_uri)
        self._db = self._client[self.settings.mongo_db]
        logger.info(f"Connected to MongoDB: {self.settings.mongo_uri}")

    async def disconnect(self):
        if self._client:
            self._client.close()

    async def get_pipeline_recipe(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """Fetch pipeline recipe (JSON config) by name."""
        if not HAS_DB or self._db is None:
            return self._get_default_recipe(pipeline_name)
        try:
            doc = await self._db[self.settings.mongo_collection_recipes].find_one(
                {"pipeline_name": pipeline_name, "enabled": True}
            )
            return doc
        except Exception as e:
            logger.error(f"MongoDB error fetching recipe '{pipeline_name}': {e}")
            return self._get_default_recipe(pipeline_name)

    async def save_pipeline_recipe(self, recipe: Dict[str, Any]) -> str:
        """Save or update a pipeline recipe."""
        if not HAS_DB or self._db is None:
            return recipe.get("pipeline_name", "unknown")
        result = await self._db[self.settings.mongo_collection_recipes].replace_one(
            {"pipeline_name": recipe["pipeline_name"]},
            recipe,
            upsert=True
        )
        return str(result.upserted_id or recipe["pipeline_name"])

    async def list_recipes(self) -> list:
        """List all active pipeline recipes."""
        if not HAS_DB or self._db is None:
            return [self._get_default_recipe("default")]
        cursor = self._db[self.settings.mongo_collection_recipes].find({"enabled": True})
        return await cursor.to_list(length=100)

    def _get_default_recipe(self, name: str) -> Dict[str, Any]:
        return {
            "pipeline_name": name,
            "mode": "batch",
            "kafka_topics": ["lidar_raw", "camera_meta", "gps_stream"],
            "batch_size": 10000,
            "checkpoint_interval_s": 60,
            "dlq_topic": "sensor_dlq",
            "enabled": True,
            "version": "1.0.0",
            "validators": ["GPSBoundsCheck", "TimestampMonotonicity",
                           "LIDARIntensityRange", "IMUDriftDetector", "CameraExposureValidator"],
            "processors": ["PointCloudStitcher", "FrameAligner", "SensorFusion",
                           "AnomalyDetector", "TrajectoryInterpolator", "OccupancyGridBuilder"],
            "sink": {"type": "delta_lake", "path": "s3://av-processed/sensor_data"},
        }


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL Config Manager — Dataset Metadata
# ─────────────────────────────────────────────────────────────────────────────

class PostgresConfigManager:
    """
    Manages dataset metadata (paths, schemas) in PostgreSQL.
    Diagram: PostgreSQL → Dataset Metadata (paths, schemas)
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self._conn = None

    def connect(self):
        if not HAS_DB:
            return
        try:
            self._conn = psycopg2.connect(
                host=self.settings.pg_host,
                port=self.settings.pg_port,
                user=self.settings.pg_user,
                password=self.settings.pg_password,
                dbname=self.settings.pg_db
            )
            logger.info("Connected to PostgreSQL")
            self._ensure_schema()
        except Exception as e:
            logger.warning(f"PostgreSQL not available: {e} — using in-memory config")

    def disconnect(self):
        if self._conn:
            self._conn.close()

    def _ensure_schema(self):
        """Create tables if they don't exist."""
        if not self._conn:
            return
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dataset_metadata (
                    id SERIAL PRIMARY KEY,
                    dataset_name VARCHAR(255) UNIQUE NOT NULL,
                    storage_path TEXT NOT NULL,
                    schema_json JSONB,
                    sensor_type VARCHAR(50),
                    record_count BIGINT DEFAULT 0,
                    size_bytes BIGINT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(255) UNIQUE NOT NULL,
                    pipeline_name VARCHAR(255),
                    mode VARCHAR(20),
                    status VARCHAR(50),
                    records_processed BIGINT DEFAULT 0,
                    records_failed BIGINT DEFAULT 0,
                    started_at TIMESTAMP DEFAULT NOW(),
                    completed_at TIMESTAMP,
                    error_message TEXT
                );
                CREATE TABLE IF NOT EXISTS schema_versions (
                    id SERIAL PRIMARY KEY,
                    schema_name VARCHAR(255),
                    version INT NOT NULL,
                    avro_schema JSONB,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            self._conn.commit()

    def get_dataset_metadata(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        if not self._conn:
            return {"dataset_name": dataset_name, "storage_path": f"s3://av-processed/{dataset_name}"}
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM dataset_metadata WHERE dataset_name = %s", (dataset_name,))
            row = cur.fetchone()
            return dict(row) if row else None

    def upsert_dataset_metadata(self, metadata: Dict[str, Any]):
        if not self._conn:
            return
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dataset_metadata (dataset_name, storage_path, schema_json, sensor_type, record_count, size_bytes)
                VALUES (%(dataset_name)s, %(storage_path)s, %(schema_json)s, %(sensor_type)s, %(record_count)s, %(size_bytes)s)
                ON CONFLICT (dataset_name) DO UPDATE
                SET storage_path = EXCLUDED.storage_path,
                    schema_json = EXCLUDED.schema_json,
                    record_count = EXCLUDED.record_count,
                    size_bytes = EXCLUDED.size_bytes,
                    updated_at = NOW()
            """, metadata)
            self._conn.commit()

    def log_pipeline_run(self, run: Dict[str, Any]):
        if not self._conn:
            return
        with self._conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (run_id, pipeline_name, mode, status, records_processed, records_failed)
                VALUES (%(run_id)s, %(pipeline_name)s, %(mode)s, %(status)s, %(records_processed)s, %(records_failed)s)
                ON CONFLICT (run_id) DO UPDATE
                SET status = EXCLUDED.status,
                    records_processed = EXCLUDED.records_processed,
                    records_failed = EXCLUDED.records_failed,
                    completed_at = NOW()
            """, run)
            self._conn.commit()

    def get_recent_runs(self, limit: int = 50) -> list:
        if not self._conn:
            return []
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT %s", (limit,)
            )
            return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Properties Config — spark, site, env settings
# ─────────────────────────────────────────────────────────────────────────────

class PropertiesConfig:
    """
    Manages flat properties files for spark, site, and env config.
    Diagram: Properties → spark, site, env
    """

    DEFAULTS: Dict[str, Dict[str, Any]] = {
        "spark": {
            "spark.app.name": "AVSensorPipeline",
            "spark.master": "yarn",
            "spark.executor.memory": "12g",
            "spark.executor.cores": "4",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "15",
            "spark.dynamicAllocation.maxExecutors": "50",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
            "spark.streaming.backpressure.enabled": "true",
        },
        "site": {
            "cluster.name": "av-pipeline-cluster",
            "hdfs.namenode": "hdfs://namenode:9000",
            "yarn.resourcemanager": "resourcemanager:8032",
            "hive.metastore.uris": "thrift://metastore:9083",
        },
        "env": {
            "environment": "production",
            "log_level": "INFO",
            "timezone": "UTC",
            "pipeline_version": "1.0.0",
        }
    }

    def __init__(self, config_dir: str = "configs"):
        self.config_dir = Path(config_dir)
        self._properties: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self):
        for section, defaults in self.DEFAULTS.items():
            props_file = self.config_dir / f"{section}.properties"
            if props_file.exists():
                self._properties[section] = self._parse_properties_file(props_file)
            else:
                self._properties[section] = defaults.copy()

    def _parse_properties_file(self, path: Path) -> Dict[str, Any]:
        props = {}
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    props[key.strip()] = value.strip()
        return props

    def get(self, section: str, key: str, default: Any = None) -> Any:
        return self._properties.get(section, {}).get(key, default)

    def get_section(self, section: str) -> Dict[str, Any]:
        return self._properties.get(section, {})

    def get_spark_config(self) -> Dict[str, str]:
        return {k: str(v) for k, v in self._properties.get("spark", {}).items()}

    def save(self, section: str, key: str, value: Any):
        if section not in self._properties:
            self._properties[section] = {}
        self._properties[section][key] = value


# ─────────────────────────────────────────────────────────────────────────────
# Unified Config Access Point
# ─────────────────────────────────────────────────────────────────────────────

class ConfigurationLayer:
    """
    Unified access to all three config stores.
    Diagram: Configuration Layer box (MongoDB + PostgreSQL + Properties)
    """

    def __init__(self):
        self.settings = get_settings()
        self.mongo = MongoConfigManager(self.settings)
        self.postgres = PostgresConfigManager(self.settings)
        self.properties = PropertiesConfig()

    async def initialize(self):
        await self.mongo.connect()
        self.postgres.connect()
        logger.info("Configuration Layer initialized")

    async def shutdown(self):
        await self.mongo.disconnect()
        self.postgres.disconnect()

    async def get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Get complete pipeline configuration (recipe + metadata + spark props)."""
        recipe = await self.mongo.get_pipeline_recipe(pipeline_name)
        spark_props = self.properties.get_spark_config()
        return {
            "recipe": recipe,
            "spark": spark_props,
            "env": self.properties.get_section("env"),
        }
