"""Configuration Layer — MongoDB + PostgreSQL + Properties"""
import os
from dataclasses import dataclass, field
from typing import Dict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topics: Dict[str, str] = field(default_factory=lambda: {
        "lidar": "lidar_raw", "camera": "camera_meta", "gps": "gps_stream"
    })
    consumer_group: str = "av-pipeline-group"
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


@dataclass
class SparkConfig:
    app_name: str = "AV-SensorPipeline"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    executor_instances: int = 15
    executor_memory: str = "12g"
    serializer: str = "org.apache.spark.serializer.KryoSerializer"
    dynamic_allocation: bool = True
    extra_configs: Dict[str, str] = field(default_factory=lambda: {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    })


@dataclass
class StorageConfig:
    s3_bucket: str = os.getenv("S3_BUCKET", "av-sensor-data")
    hdfs_base: str = os.getenv("HDFS_BASE", "/tmp/av")
    delta_lake_path: str = os.getenv("DELTA_PATH", "/tmp/av/delta")
    hive_metastore_uri: str = os.getenv("HIVE_METASTORE", "thrift://localhost:9083")


@dataclass
class MongoConfig:
    uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    database: str = "av_pipeline"
    recipes_collection: str = "pipeline_recipes"


@dataclass
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "av_metadata")
    user: str = os.getenv("POSTGRES_USER", "avuser")
    password: str = os.getenv("POSTGRES_PASSWORD", "avpassword")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class RabbitMQConfig:
    host: str = os.getenv("RABBITMQ_HOST", "localhost")
    port: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    dlq_queue: str = "av.dead.letter"
    events_exchange: str = "av.events"


@dataclass
class PipelineSettings:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    mongo: MongoConfig = field(default_factory=MongoConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    rabbitmq: RabbitMQConfig = field(default_factory=RabbitMQConfig)
    pipeline_mode: str = os.getenv("PIPELINE_MODE", "batch")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    dlq_enabled: bool = True
    exactly_once: bool = True
    watermark_delay: str = "10 minutes"


settings = PipelineSettings()
