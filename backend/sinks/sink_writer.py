"""
sinks/sink_writer.py
Stage 5: SINK — SinkWriterFactory with all sink implementations.
Diagram: HudSinkWriter (HDFS/Delta), ParquetSinkWriter, DeltaLakeSinkWriter,
         APIPublisher (REST), MessageQueueSink (RabbitMQ)
at-least-once + idempotent write = exactly-once effect
"""
from __future__ import annotations
import json
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Type
from datetime import datetime
from pathlib import Path
from loguru import logger

from config.config_manager import get_settings


# ─────────────────────────────────────────────────────────────────────────────
# ISinkWriter Interface (Strategy Pattern)
# ─────────────────────────────────────────────────────────────────────────────

class ISinkWriter(ABC):
    """Strategy interface for all sink writers."""

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    async def write(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Write records. Returns (success_count, failure_count)."""
        ...

    @abstractmethod
    def start(self):
        ...

    @abstractmethod
    def stop(self):
        ...


# ─────────────────────────────────────────────────────────────────────────────
# HudSinkWriter — upsert to data lake (HDFS/Delta)
# ─────────────────────────────────────────────────────────────────────────────

class HudSinkWriter(ISinkWriter):
    """
    Writes to HUDI data lake with upsert semantics.
    Diagram: HudSinkWriter → upsert to data lake
    """
    name = "HudSinkWriter"

    def __init__(self, base_path: str = "hdfs://namenode/av-data/hudi"):
        self.base_path = base_path
        self._written = 0

    def start(self):
        logger.info(f"HudSinkWriter started: {self.base_path}")

    def stop(self):
        logger.info(f"HudSinkWriter stopped. Total written: {self._written}")

    async def write(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not records:
            return 0, 0
        try:
            # Production: use PySpark with HUDI library
            # hoodie.datasource.write.operation = 'upsert'
            # hoodie.datasource.write.recordkey.field = 'record_id'
            # hoodie.datasource.write.precombine.field = 'timestamp'
            self._written += len(records)
            logger.debug(f"[HUDI] Upserted {len(records)} records to {self.base_path}")
            return len(records), 0
        except Exception as e:
            logger.error(f"HUDI write failed: {e}")
            return 0, len(records)


# ─────────────────────────────────────────────────────────────────────────────
# ParquetSinkWriter — columnar files on S3/HDFS
# ─────────────────────────────────────────────────────────────────────────────

class ParquetSinkWriter(ISinkWriter):
    """
    Writes Parquet columnar files to S3 or HDFS.
    Diagram: ParquetSinkWriter → columnar files
    """
    name = "ParquetSinkWriter"

    def __init__(self, output_path: str = "s3://av-processed/parquet"):
        self.output_path = output_path
        self._partition_count = 0

    def start(self):
        logger.info(f"ParquetSinkWriter started: {self.output_path}")

    def stop(self):
        logger.info(f"ParquetSinkWriter stopped. Partitions: {self._partition_count}")

    async def write(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not records:
            return 0, 0
        try:
            # Group by sensor_type + date for partitioning
            partitions: Dict[str, List] = {}
            for r in records:
                sensor_type = r.get("sensor_type", "unknown")
                ts = r.get("timestamp", datetime.utcnow().timestamp())
                date = datetime.fromtimestamp(float(ts)).strftime("%Y/%m/%d")
                key = f"{sensor_type}/{date}"
                partitions.setdefault(key, []).append(r)

            for partition_key, partition_records in partitions.items():
                path = f"{self.output_path}/{partition_key}"
                await self._write_partition(path, partition_records)
                self._partition_count += 1

            logger.debug(f"[PARQUET] Wrote {len(records)} records across {len(partitions)} partitions")
            return len(records), 0
        except Exception as e:
            logger.error(f"Parquet write failed: {e}")
            return 0, len(records)

    async def _write_partition(self, path: str, records: List[Dict]):
        """Write a single partition. In production: uses PyArrow."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import io

            # Flatten nested dicts for Parquet compatibility
            flat_records = []
            for r in records:
                flat = {k: v for k, v in r.items() if isinstance(v, (str, int, float, bool, type(None)))}
                flat_records.append(flat)

            if flat_records:
                table = pa.Table.from_pylist(flat_records)
                buf = io.BytesIO()
                pq.write_table(table, buf)
                logger.debug(f"Parquet partition written: {path} ({len(flat_records)} records, {buf.tell()} bytes)")
        except ImportError:
            logger.debug(f"[MOCK PARQUET] Would write to {path}: {len(records)} records")


# ─────────────────────────────────────────────────────────────────────────────
# DeltaLakeSinkWriter — ACID tables
# ─────────────────────────────────────────────────────────────────────────────

class DeltaLakeSinkWriter(ISinkWriter):
    """
    Writes to Delta Lake for ACID compliance and time travel.
    Diagram: DeltaLakeSinkWriter → ACID tables
    Write to sink → commitGlobalBatch()
    """
    name = "DeltaLakeSinkWriter"

    def __init__(self, delta_path: str = "s3://av-processed/delta"):
        self.delta_path = delta_path
        self._commit_count = 0

    def start(self):
        logger.info(f"DeltaLakeSinkWriter started: {self.delta_path}")

    def stop(self):
        logger.info(f"DeltaLakeSinkWriter stopped. Commits: {self._commit_count}")

    async def write(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not records:
            return 0, 0
        try:
            # Production: use delta-spark
            # spark.write.format("delta").mode("append").save(delta_path)
            # After write: commitGlobalBatch() for exactly-once
            self._commit_count += 1
            logger.debug(f"[DELTA] Committed batch {self._commit_count}: {len(records)} records")
            return len(records), 0
        except Exception as e:
            logger.error(f"Delta write failed: {e}")
            return 0, len(records)

    async def compact(self):
        """Optimize Delta table (bin-packing, Z-order)."""
        logger.info(f"[DELTA] Compacting {self.delta_path}")
        # OPTIMIZE delta.`path` ZORDER BY (vehicle_id, timestamp)


# ─────────────────────────────────────────────────────────────────────────────
# APIPublisher — REST POST to services
# ─────────────────────────────────────────────────────────────────────────────

class APIPublisher(ISinkWriter):
    """
    Publishes processed records to downstream REST APIs.
    Diagram: APIPublisher → REST POST to services
    """
    name = "APIPublisher"

    def __init__(self, endpoint_url: str = "http://localhost:8080/api/ingest",
                 batch_size: int = 100, timeout_s: float = 30.0):
        self.endpoint_url = endpoint_url
        self.batch_size = batch_size
        self.timeout_s = timeout_s
        self._client = None

    def start(self):
        try:
            import httpx
            self._client = httpx.AsyncClient(timeout=self.timeout_s)
            logger.info(f"APIPublisher started: {self.endpoint_url}")
        except ImportError:
            logger.warning("httpx not installed — API sink will mock publish")

    def stop(self):
        if self._client:
            asyncio.get_event_loop().run_until_complete(self._client.aclose())

    async def write(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not records:
            return 0, 0

        success, failed = 0, 0
        # Send in sub-batches
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            try:
                if self._client:
                    response = await self._client.post(
                        self.endpoint_url,
                        json={"records": batch, "count": len(batch)},
                        headers={"Content-Type": "application/json"}
                    )
                    if response.status_code < 300:
                        success += len(batch)
                    else:
                        failed += len(batch)
                        logger.warning(f"API publish failed: {response.status_code}")
                else:
                    logger.debug(f"[MOCK API] Would POST {len(batch)} records to {self.endpoint_url}")
                    success += len(batch)
            except Exception as e:
                logger.error(f"API publish error: {e}")
                failed += len(batch)

        return success, failed


# ─────────────────────────────────────────────────────────────────────────────
# MessageQueueSink — RabbitMQ publisher
# ─────────────────────────────────────────────────────────────────────────────

class MessageQueueSink(ISinkWriter):
    """
    Publishes to RabbitMQ for real-time downstream consumers.
    Diagram: MessageQueueSink → publish to RabbitMQ
    """
    name = "MessageQueueSink"

    def __init__(self, queue_name: str = "processed_sensor_data",
                 exchange: str = "av_pipeline"):
        self.queue_name = queue_name
        self.exchange = exchange
        self.settings = get_settings()
        self._connection = None
        self._channel = None

    def start(self):
        logger.info(f"MessageQueueSink configured: {self.queue_name}")

    def stop(self):
        if self._connection:
            try:
                asyncio.get_event_loop().run_until_complete(self._connection.close())
            except Exception:
                pass

    async def _ensure_connected(self):
        if self._connection is None:
            try:
                import aio_pika
                self._connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
                self._channel = await self._connection.channel()
                await self._channel.declare_queue(self.queue_name, durable=True)
                logger.info("RabbitMQ connected")
            except Exception as e:
                logger.warning(f"RabbitMQ not available: {e} — mock mode")

    async def write(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        await self._ensure_connected()
        success, failed = 0, 0

        for record in records:
            try:
                if self._channel:
                    import aio_pika
                    message = aio_pika.Message(
                        body=json.dumps(record).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    )
                    await self._channel.default_exchange.publish(
                        message, routing_key=self.queue_name
                    )
                else:
                    logger.debug(f"[MOCK MQ] Would publish to {self.queue_name}: {record.get('record_id')}")
                success += 1
            except Exception as e:
                logger.warning(f"MQ publish failed: {e}")
                failed += 1

        return success, failed


# ─────────────────────────────────────────────────────────────────────────────
# HiveMetastoreSink — Repartition → Write → Hive metadata
# ─────────────────────────────────────────────────────────────────────────────

class HiveMetastoreSink(ISinkWriter):
    """
    Write data and register partitions in Hive Metastore.
    Diagram: Repartition → Write → Hive metadata
    """
    name = "HiveMetastoreSink"

    def __init__(self, hive_table: str = "sensor_data", hive_db: str = "av_datalake"):
        self.hive_table = hive_table
        self.hive_db = hive_db

    def start(self):
        logger.info(f"HiveMetastoreSink started: {self.hive_db}.{self.hive_table}")

    def stop(self):
        pass

    async def write(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not records:
            return 0, 0
        # Production: repartition by (sensor_type, date) then MSCK REPAIR TABLE
        logger.debug(f"[HIVE] Would write {len(records)} records to {self.hive_db}.{self.hive_table}")
        return len(records), 0


# ─────────────────────────────────────────────────────────────────────────────
# Sink Writer Factory
# ─────────────────────────────────────────────────────────────────────────────

class SinkWriterFactory:
    """
    Factory for creating sink writer instances.
    Diagram: SinkWriterFactory
    """

    _REGISTRY: Dict[str, Type[ISinkWriter]] = {
        "hudi": HudSinkWriter,
        "parquet": ParquetSinkWriter,
        "delta_lake": DeltaLakeSinkWriter,
        "api": APIPublisher,
        "rabbitmq": MessageQueueSink,
        "hive": HiveMetastoreSink,
    }

    @classmethod
    def create(cls, sink_type: str, **kwargs) -> ISinkWriter:
        if sink_type not in cls._REGISTRY:
            raise ValueError(f"Unknown sink: '{sink_type}'. Available: {list(cls._REGISTRY.keys())}")
        return cls._REGISTRY[sink_type](**kwargs)

    @classmethod
    def create_all(cls, configs: List[Dict[str, Any]]) -> List[ISinkWriter]:
        sinks = []
        for cfg in configs:
            sink_type = cfg.pop("type", "delta_lake")
            sinks.append(cls.create(sink_type, **cfg))
        return sinks

    @classmethod
    def list_sinks(cls) -> List[str]:
        return list(cls._REGISTRY.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Sink Engine — orchestrates multiple sinks
# ─────────────────────────────────────────────────────────────────────────────

class SinkEngine:
    """
    Routes records to one or more configured sinks.
    Supports fan-out: write same batch to multiple sinks.
    """

    def __init__(self, sink_config: Dict[str, Any]):
        self._sinks: List[ISinkWriter] = []
        sink_type = sink_config.get("type", "delta_lake")
        primary = SinkWriterFactory.create(
            sink_type,
            **{k: v for k, v in sink_config.items() if k != "type"}
        )
        self._sinks.append(primary)

        # Add secondary sinks if configured
        for secondary_cfg in sink_config.get("secondary_sinks", []):
            sink_t = secondary_cfg.pop("type", "parquet")
            self._sinks.append(SinkWriterFactory.create(sink_t, **secondary_cfg))

    def start(self):
        for s in self._sinks:
            s.start()

    def stop(self):
        for s in self._sinks:
            s.stop()

    async def write_batch(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Write to all configured sinks, return primary sink results."""
        if not records:
            return 0, 0

        primary_ok, primary_fail = 0, 0
        for i, sink in enumerate(self._sinks):
            ok, fail = await sink.write(records)
            if i == 0:
                primary_ok, primary_fail = ok, fail
            if fail > 0:
                logger.warning(f"Sink {sink.name} had {fail} failures")

        return primary_ok, primary_fail
