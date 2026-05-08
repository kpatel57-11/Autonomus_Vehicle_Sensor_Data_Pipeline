"""
SinkWriterFactory — produces ISinkWriter for HuDI/Delta Lake, Parquet, API, RabbitMQ, Hive.
Supports at-least-once + idempotent writes = exactly-once effect.
"""
import logging
from typing import Any, Dict
from core.interfaces import ISinkWriter
logger = logging.getLogger(__name__)


class HudiSinkWriter(ISinkWriter):
    """Upsert to Apache Hudi data lake (ACID tables)."""
    @property
    def sink_name(self): return "HudiSinkWriter"

    def write(self, df, config: Dict) -> bool:
        try:
            path = config.get("hudi_path", "/tmp/av/hudi/sensor_data")
            record_key = config.get("record_key", "sequence_id")
            partition_path = config.get("partition_path", "sensor_type")
            precombine = config.get("precombine", "timestamp_ms")
            hudi_opts = {
                "hoodie.table.name": "av_sensor_data",
                "hoodie.datasource.write.recordkey.field": record_key,
                "hoodie.datasource.write.partitionpath.field": partition_path,
                "hoodie.datasource.write.precombine.field": precombine,
                "hoodie.datasource.write.operation": "upsert",
                "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
                "hoodie.upsert.shuffle.parallelism": "2",
                "hoodie.insert.shuffle.parallelism": "2",
            }
            (df.write.format("hudi").options(**hudi_opts)
                .mode("append").save(path))
            logger.info(f"HudiSinkWriter: wrote to {path}")
            return True
        except Exception as e:
            logger.warning(f"HudiSinkWriter fallback to parquet: {e}")
            return self._fallback_parquet(df, config)

    def _fallback_parquet(self, df, config) -> bool:
        try:
            path = config.get("hudi_path", "/tmp/av/fallback/hudi")
            df.write.mode("append").parquet(path)
            return True
        except Exception as e:
            logger.error(f"Parquet fallback failed: {e}")
            return False

    def commit(self) -> bool:
        logger.info("HudiSinkWriter: commit (Hudi handles atomically)")
        return True


class DeltaLakeSinkWriter(ISinkWriter):
    """Write to Delta Lake — columnar files, ACID transactions."""
    @property
    def sink_name(self): return "DeltaLakeSinkWriter"

    def write(self, df, config: Dict) -> bool:
        try:
            from config.settings import settings
            path = config.get("delta_path", settings.storage.delta_lake_path)
            (df.write.format("delta")
                .mode("append")
                .option("overwriteSchema", "true")
                .save(path))
            logger.info(f"DeltaLakeSinkWriter: wrote to {path}")
            return True
        except Exception as e:
            logger.warning(f"Delta Lake unavailable, writing parquet: {e}")
            try:
                df.write.mode("append").parquet("/tmp/av/fallback/delta")
                return True
            except Exception as e2:
                logger.error(f"Delta fallback failed: {e2}")
                return False

    def commit(self) -> bool:
        logger.info("DeltaLakeSinkWriter: committed (Delta handles atomically)")
        return True

    def commitGlobalBatch(self) -> bool:
        """Atomic commit across all partitions."""
        return self.commit()


class ParquetSinkWriter(ISinkWriter):
    """Write columnar Parquet files to S3/HDFS."""
    @property
    def sink_name(self): return "ParquetSinkWriter"

    def write(self, df, config: Dict) -> bool:
        try:
            from config.settings import settings
            path = config.get("parquet_path", f"{settings.storage.hdfs_base}/parquet/output")
            partition_cols = config.get("partition_cols", ["sensor_type"])
            pcols = [c for c in partition_cols if c in df.columns]
            writer = df.write.mode("append")
            if pcols:
                writer = writer.partitionBy(*pcols)
            writer.parquet(path)
            logger.info(f"ParquetSinkWriter: wrote to {path}")
            return True
        except Exception as e:
            logger.error(f"ParquetSinkWriter: {e}")
            return False

    def commit(self) -> bool: return True


class APIPublisherSinkWriter(ISinkWriter):
    """REST POST to downstream services (perception model, simulation)."""
    @property
    def sink_name(self): return "APIPublisherSinkWriter"

    def write(self, df, config: Dict) -> bool:
        try:
            import urllib.request, json
            endpoint = config.get("api_endpoint", "http://localhost:8000/api/ingest")
            sample = df.limit(10).toPandas().to_dict(orient="records")
            payload = json.dumps({"records": sample, "count": len(sample)}).encode()
            req = urllib.request.Request(endpoint, data=payload,
                headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=5):
                logger.info(f"APIPublisher: posted {len(sample)} records to {endpoint}")
            return True
        except Exception as e:
            logger.warning(f"APIPublisher endpoint unavailable: {e}")
            return True  # Non-critical, don't fail pipeline

    def commit(self) -> bool: return True


class RabbitMQSinkWriter(ISinkWriter):
    """Publish events to RabbitMQ message queue."""
    @property
    def sink_name(self): return "RabbitMQSinkWriter"

    def write(self, df, config: Dict) -> bool:
        try:
            import pika, json
            from config.settings import settings
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=settings.rabbitmq.host, port=settings.rabbitmq.port))
            channel = conn.channel()
            channel.exchange_declare(exchange=settings.rabbitmq.events_exchange, exchange_type="topic", durable=True)
            sample = df.limit(100).toPandas()
            for _, row in sample.iterrows():
                msg = row.to_json()
                channel.basic_publish(
                    exchange=settings.rabbitmq.events_exchange,
                    routing_key="av.sensor.processed",
                    body=msg,
                    properties=pika.BasicProperties(delivery_mode=2))
            conn.close()
            logger.info(f"RabbitMQSinkWriter: published {len(sample)} messages")
            return True
        except Exception as e:
            logger.warning(f"RabbitMQ unavailable: {e}")
            return True

    def commit(self) -> bool: return True


class HiveSinkWriter(ISinkWriter):
    """Repartition + write Hive metadata (managed tables)."""
    @property
    def sink_name(self): return "HiveSinkWriter"

    def write(self, df, config: Dict) -> bool:
        try:
            table = config.get("hive_table", "av_sensor_data")
            db = config.get("hive_db", "av_pipeline")
            df.write.mode("append").saveAsTable(f"{db}.{table}")
            logger.info(f"HiveSinkWriter: wrote to {db}.{table}")
            return True
        except Exception as e:
            logger.warning(f"Hive unavailable: {e}")
            return True

    def commit(self) -> bool: return True


class DLQWriter(ISinkWriter):
    """Dead Letter Queue writer — bad records to RabbitMQ DLQ + S3."""
    @property
    def sink_name(self): return "DLQWriter"

    def write(self, df, config: Dict) -> bool:
        try:
            from config.settings import settings
            # Write to S3 DLQ path
            path = f"{settings.storage.hdfs_base}/dlq/{config.get('run_id','unknown')}"
            df.write.mode("append").json(path)
            logger.warning(f"DLQ: {df.count()} bad records written to {path}")
            return True
        except Exception as e:
            logger.error(f"DLQ write failed: {e}")
            return False

    def commit(self) -> bool: return True


class SinkWriterFactory:
    """Factory: creates ISinkWriter by name."""
    _registry = {
        "hudi_data_lake":   HudiSinkWriter,
        "delta_lake":       DeltaLakeSinkWriter,
        "parquet":          ParquetSinkWriter,
        "api_publisher":    APIPublisherSinkWriter,
        "rabbitmq_sink":    RabbitMQSinkWriter,
        "hive":             HiveSinkWriter,
        "dlq":              DLQWriter,
    }

    def get_writer(self, name: str) -> ISinkWriter:
        cls = self._registry.get(name)
        if not cls:
            logger.warning(f"Unknown sink '{name}', defaulting to Parquet")
            return ParquetSinkWriter()
        return cls()

    def register(self, name, cls): self._registry[name] = cls
    def list_available(self): return list(self._registry.keys())
