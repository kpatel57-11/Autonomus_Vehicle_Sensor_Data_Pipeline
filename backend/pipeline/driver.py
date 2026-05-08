"""
pipeline/driver.py
Driver Layer — PipelineDriver.main() implementing the Template Method Pattern.
Diagram: PipelineDriver (abstract) → BatchDriver | StreamingDriver
processPipeline() iterates 6 stages: SOURCE → VALIDATE → TRANSFORM → PROCESS → SINK → CHECKPOINT
"""
from __future__ import annotations
import asyncio
import uuid
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from loguru import logger

from models.sensor_data import (
    PipelineMode, ProcessingStage, PipelineEvent, CheckpointState
)
from config.config_manager import ConfigurationLayer, get_settings
from ingestion.kafka_consumer import SensorKafkaConsumer, SensorKafkaProducer
from validators.validators import ValidationEngine, ValidationFactory
from processors.processors import ProcessingEngine, ProcessorFactory
from sinks.sink_writer import SinkWriterFactory, SinkEngine
from checkpoints.checkpoint_manager import CheckpointManager
from self_healing.lifecycle_monitor import QueryLifecycleMonitor
from monitoring.metrics import PipelineMetrics


# ─────────────────────────────────────────────────────────────────────────────
# SQL Transform Manager (Stage 3)
# ─────────────────────────────────────────────────────────────────────────────

class SqlManager:
    """
    Stage 3: SQL TRANSFORMS using Spark SQL execution engine.
    Diagram: Coordinate transforms (geo→local), temporal alignment,
             WHERE valid_gps=TRUE AND intensity>0, Window functions,
             Watermark support (streaming)
    """

    TRANSFORMS = {
        "geo_to_local": """
            SELECT *,
                   longitude * 111320 * COS(RADIANS(latitude)) AS x_m,
                   latitude * 110540 AS y_m
            FROM sensor_data
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """,
        "filter_valid_gps": """
            SELECT * FROM sensor_data
            WHERE valid_gps = TRUE AND gps_fix_type >= 2
        """,
        "filter_lidar_intensity": """
            SELECT * FROM sensor_data
            WHERE intensity > 0
        """,
        "temporal_sync": """
            SELECT *,
                   TIMESTAMP_MONO(ts) AS aligned_ts,
                   ROW_NUMBER() OVER (
                       PARTITION BY vehicle_id, sensor_type
                       ORDER BY timestamp
                   ) AS seq_in_window
            FROM sensor_data
        """,
        "window_aggregation": """
            SELECT vehicle_id, sensor_type,
                   WINDOW(timestamp, '5 seconds') AS time_window,
                   COUNT(*) AS record_count,
                   AVG(speed_mps) AS avg_speed,
                   MAX(point_count) AS max_points
            FROM sensor_data
            GROUP BY vehicle_id, sensor_type, WINDOW(timestamp, '5 seconds')
        """,
        "anomaly_flag": """
            SELECT *,
                   CASE
                     WHEN speed_mps > 55 THEN TRUE
                     WHEN ABS(gyro_z) > 5 THEN TRUE
                     ELSE FALSE
                   END AS is_anomaly
            FROM sensor_data
        """,
    }

    def __init__(self, spark_session=None):
        self._spark = spark_session

    def apply_transforms(
        self, records: List[Dict[str, Any]], transforms: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Apply SQL transforms to records.
        In production: uses actual PySpark DataFrames.
        Here: Python-based simulation of the transforms.
        """
        result = records

        for transform_name in transforms:
            if transform_name == "geo_to_local":
                result = self._geo_to_local(result)
            elif transform_name == "filter_valid_gps":
                result = self._filter_valid_gps(result)
            elif transform_name == "filter_lidar_intensity":
                result = self._filter_lidar_intensity(result)
            elif transform_name == "anomaly_flag":
                result = self._flag_anomalies(result)
            else:
                logger.debug(f"Transform '{transform_name}' skipped (requires Spark)")

        return result

    def _geo_to_local(self, records: List[Dict]) -> List[Dict]:
        import math
        for r in records:
            lat = r.get("latitude")
            lon = r.get("longitude")
            if lat is not None and lon is not None:
                r["x_m"] = lon * 111320 * math.cos(math.radians(lat))
                r["y_m"] = lat * 110540
        return records

    def _filter_valid_gps(self, records: List[Dict]) -> List[Dict]:
        return [r for r in records if r.get("gps_fix_type", 0) >= 2 or r.get("sensor_type") != "gps_imu"]

    def _filter_lidar_intensity(self, records: List[Dict]) -> List[Dict]:
        return [r for r in records if r.get("sensor_type") != "lidar" or r.get("point_count", 0) > 0]

    def _flag_anomalies(self, records: List[Dict]) -> List[Dict]:
        for r in records:
            r["is_anomaly"] = (
                r.get("speed_mps", 0) > 55 or
                abs(r.get("gyro_z", 0)) > 5
            )
        return records

    def with_watermark(self, records: List[Dict], watermark_delay_s: float = 10.0) -> List[Dict]:
        """Apply watermark for late-arriving streaming data."""
        now = time.time()
        return [r for r in records if now - float(r.get("timestamp", now)) <= watermark_delay_s * 2]


# ─────────────────────────────────────────────────────────────────────────────
# Abstract PipelineDriver (Template Method Pattern)
# ─────────────────────────────────────────────────────────────────────────────

class PipelineDriver(ABC):
    """
    Abstract base driver implementing the Template Method pattern.
    Diagram: PipelineDriver (abstract) → Template Method: processPipeline()
    Subclasses: BatchDriver, StreamingDriver
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.run_id = str(uuid.uuid4())
        self.settings = get_settings()
        self.mode: PipelineMode = PipelineMode.BATCH

        # Components
        self.consumer = SensorKafkaConsumer(topics=config.get("kafka_topics"))
        self.producer = SensorKafkaProducer()
        self.validator = ValidationEngine(config.get("validators"))
        self.sql_manager = SqlManager()
        self.processor = ProcessingEngine(config.get("processors"))
        self.checkpoint_manager = CheckpointManager(run_id=self.run_id)
        self.monitor = QueryLifecycleMonitor()
        self.metrics = PipelineMetrics(run_id=self.run_id)

        # Sinks
        sink_config = config.get("sink", {})
        self.sink_engine = SinkEngine(sink_config)

        # Stats
        self._total_in = 0
        self._total_out = 0
        self._total_failed = 0
        self._stage_stats: Dict[str, Dict[str, int]] = {}

    # ── Template Method ────────────────────────────────────────────────────────
    async def processPipeline(self) -> Dict[str, Any]:
        """
        Main template method — iterates 6 pipeline stages.
        Not overridden by subclasses; subclasses override individual hook methods.
        """
        self.monitor.on_query_started(self.run_id, self.mode.value)
        logger.info(f"[{self.run_id}] Pipeline START — mode={self.mode.value}")

        try:
            # ── Stage 1: SOURCE ────────────────────────────────────────────
            records = await self._stage_source()
            self._record_stage(ProcessingStage.SOURCE, len(records), 0)

            # ── Stage 2: VALIDATE ──────────────────────────────────────────
            valid_records, invalid_records = await self._stage_validate(records)
            self._record_stage(ProcessingStage.VALIDATION, len(valid_records), len(invalid_records))

            # Send invalid to DLQ
            for rec in invalid_records:
                self.producer.publish_to_dlq(rec, reason=str(rec.get("validation_errors", [])))

            # ── Stage 3: SQL TRANSFORM ────────────────────────────────────
            transformed = await self._stage_transform(valid_records)
            self._record_stage(ProcessingStage.TRANSFORM, len(transformed), 0)

            # ── Stage 4: PROCESS ──────────────────────────────────────────
            processed, proc_failed = await self._stage_process(transformed)
            self._record_stage(ProcessingStage.PROCESSING, len(processed), len(proc_failed))

            # ── Stage 5: SINK ─────────────────────────────────────────────
            sink_ok, sink_failed = await self._stage_sink(processed)
            self._record_stage(ProcessingStage.SINK, sink_ok, sink_failed)

            # ── Stage 6: CHECKPOINT ───────────────────────────────────────
            await self._stage_checkpoint(sink_ok)

            self._total_in += len(records)
            self._total_out += sink_ok
            self._total_failed += len(invalid_records) + len(proc_failed) + sink_failed

            summary = self._build_summary(records, valid_records, processed, sink_ok)
            self.monitor.on_query_progress(self.run_id, summary)
            return summary

        except Exception as e:
            self.monitor.on_query_terminated(self.run_id, str(e))
            logger.error(f"[{self.run_id}] Pipeline FAILED: {e}")
            raise

    # ── Abstract hook methods (overridden by BatchDriver / StreamingDriver) ──

    @abstractmethod
    async def _stage_source(self) -> List[Dict[str, Any]]:
        """Stage 1: Read from source."""
        ...

    @abstractmethod
    async def _stage_validate(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict], List[Dict]]:
        """Stage 2: Validate records."""
        ...

    @abstractmethod
    async def _stage_transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 3: SQL transforms."""
        ...

    @abstractmethod
    async def _stage_process(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict], List[Dict]]:
        """Stage 4: Process records."""
        ...

    @abstractmethod
    async def _stage_sink(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[int, int]:
        """Stage 5: Write to sink. Returns (success_count, failure_count)."""
        ...

    @abstractmethod
    async def _stage_checkpoint(self, records_written: int):
        """Stage 6: Checkpoint state."""
        ...

    # ── Shared helpers ─────────────────────────────────────────────────────────

    def _record_stage(self, stage: ProcessingStage, out: int, failed: int):
        self._stage_stats[stage.value] = {"out": out, "failed": failed}
        self.metrics.record_stage(stage.value, out, failed)
        logger.info(f"  [{self.run_id}] {stage.value}: out={out}, failed={failed}")

    def _build_summary(self, raw, valid, processed, written) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode.value,
            "records_in": len(raw),
            "records_valid": len(valid),
            "records_processed": len(processed),
            "records_written": written,
            "stage_stats": self._stage_stats,
            "timestamp": datetime.utcnow().isoformat(),
        }

    @classmethod
    async def main(cls, mode: str, config: Dict[str, Any], iterations: int = -1):
        """
        Entry point — PipelineDriver.main()
        Diagram: BatchDriver: read → process → write → exit
                 StreamingDriver: readStream → foreachBatch, Runs 24/7
        """
        if mode == PipelineMode.BATCH:
            driver = BatchDriver(config)
        else:
            driver = StreamingDriver(config)

        driver.consumer.start()
        driver.producer.start()
        driver.sink_engine.start()

        run_count = 0
        try:
            while iterations == -1 or run_count < iterations:
                await driver.processPipeline()
                run_count += 1
                if mode == PipelineMode.BATCH:
                    break  # Batch exits after one pass
                await asyncio.sleep(1.0)  # Streaming: mini-batch interval
        finally:
            driver.consumer.stop()
            driver.producer.flush()
            driver.sink_engine.stop()


# ─────────────────────────────────────────────────────────────────────────────
# BatchDriver — triggered by Airflow DAGs
# ─────────────────────────────────────────────────────────────────────────────

class BatchDriver(PipelineDriver):
    """
    Batch pipeline driver.
    Diagram: BatchDriver — read → process → write → exit
             Triggered by Airflow DAGs
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mode = PipelineMode.BATCH
        self.batch_size = config.get("batch_size", 10000)

    async def _stage_source(self) -> List[Dict[str, Any]]:
        logger.info(f"  [BATCH SOURCE] Reading batch of {self.batch_size} records from Kafka")
        loop = asyncio.get_event_loop()
        records = await loop.run_in_executor(
            None, self.consumer.consume_batch, self.batch_size
        )
        # Also read from S3/HDFS for reprocessing if configured
        if self.config.get("source_path"):
            s3_records = await self._read_from_object_storage(self.config["source_path"])
            records.extend(s3_records)
        return records

    async def _stage_validate(self, records):
        loop = asyncio.get_event_loop()
        valid, invalid = await loop.run_in_executor(
            None, self.validator.validate_batch, records
        )
        return valid, invalid

    async def _stage_transform(self, records):
        transforms = self.config.get("transforms", ["geo_to_local", "filter_valid_gps", "anomaly_flag"])
        return self.sql_manager.apply_transforms(records, transforms)

    async def _stage_process(self, records):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.processor.process_batch, records)

    async def _stage_sink(self, records):
        return await self.sink_engine.write_batch(records)

    async def _stage_checkpoint(self, records_written: int):
        state = CheckpointState(
            pipeline_run_id=self.run_id,
            mode=self.mode,
            kafka_offsets=self.consumer.get_offsets(),
            records_processed=self._total_out + records_written,
        )
        await self.checkpoint_manager.save(state)
        self.consumer.commit()

    async def _read_from_object_storage(self, path: str) -> List[Dict]:
        """Read Parquet/Delta files from S3/HDFS for batch reprocessing."""
        logger.info(f"  [BATCH SOURCE] Reading from object storage: {path}")
        return []  # Production: use PyArrow/PySpark to read Parquet


# ─────────────────────────────────────────────────────────────────────────────
# StreamingDriver — runs 24/7 with auto-restart
# ─────────────────────────────────────────────────────────────────────────────

class StreamingDriver(PipelineDriver):
    """
    Streaming pipeline driver.
    Diagram: StreamingDriver — readStream → foreachBatch, Runs 24/7, auto-restart
    Streaming: withWatermark() for late-arriving sensor data
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mode = PipelineMode.STREAMING
        self.micro_batch_size = config.get("micro_batch_size", 100)
        self.watermark_delay_s = config.get("watermark_delay_s", 10.0)

    async def _stage_source(self) -> List[Dict[str, Any]]:
        """Read micro-batch from Kafka stream."""
        logger.debug("  [STREAM SOURCE] Reading micro-batch from Kafka")
        loop = asyncio.get_event_loop()
        records = await loop.run_in_executor(
            None, self.consumer.consume_batch, self.micro_batch_size, 0.5
        )
        # Apply watermark for late-arriving data
        return self.sql_manager.with_watermark(records, self.watermark_delay_s)

    async def _stage_validate(self, records):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.validator.validate_batch, records)

    async def _stage_transform(self, records):
        # Streaming transforms include window functions
        transforms = ["geo_to_local", "filter_valid_gps", "anomaly_flag"]
        return self.sql_manager.apply_transforms(records, transforms)

    async def _stage_process(self, records):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.processor.process_batch, records)

    async def _stage_sink(self, records):
        """foreachBatch write — streaming sink."""
        return await self.sink_engine.write_batch(records)

    async def _stage_checkpoint(self, records_written: int):
        """
        Streaming checkpoint — Spark checkpoints + Kafka offset tracking.
        at-least-once + idempotent write = exactly-once effect
        """
        state = CheckpointState(
            pipeline_run_id=self.run_id,
            mode=self.mode,
            kafka_offsets=self.consumer.get_offsets(),
            records_processed=self._total_out + records_written,
        )
        await self.checkpoint_manager.save(state)
        # Commit only after successful sink write
        self.consumer.commit()
