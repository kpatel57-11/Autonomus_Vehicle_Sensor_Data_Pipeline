"""
ingestion/kafka_consumer.py
Ingestion & Buffering Layer: Kafka Cluster + Schema Registry
Topics: lidar_raw, camera_meta, gps_stream
"""
from __future__ import annotations
import asyncio
import json
from typing import AsyncIterator, Callable, Dict, List, Optional, Any
from datetime import datetime
from loguru import logger

try:
    from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    logger.warning("Confluent Kafka not installed; using mock ingestion.")

from models.sensor_data import (
    BaseSensorRecord, SensorType, RecordStatus, LidarRecord,
    CameraRecord, GpsImuRecord, RadarRecord, CanBusRecord, UltrasonicRecord
)
from config.config_manager import get_settings


# ─────────────────────────────────────────────────────────────────────────────
# Schema Registry client
# ─────────────────────────────────────────────────────────────────────────────

class SchemaRegistryManager:
    """
    Manages Avro/Protobuf schemas via Confluent Schema Registry.
    Diagram: Schema Registry → Avro/Protobuf schemas
    """

    # Avro schemas for each sensor type
    AVRO_SCHEMAS: Dict[str, str] = {
        "lidar_raw": json.dumps({
            "type": "record", "name": "LidarRecord",
            "namespace": "com.av.sensors",
            "fields": [
                {"name": "record_id", "type": "string"},
                {"name": "vehicle_id", "type": "string"},
                {"name": "sensor_id", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "point_count", "type": "int"},
                {"name": "points_json", "type": "string"},
            ]
        }),
        "camera_meta": json.dumps({
            "type": "record", "name": "CameraRecord",
            "namespace": "com.av.sensors",
            "fields": [
                {"name": "record_id", "type": "string"},
                {"name": "vehicle_id", "type": "string"},
                {"name": "camera_index", "type": "int"},
                {"name": "timestamp", "type": "long"},
                {"name": "width", "type": "int"},
                {"name": "height", "type": "int"},
                {"name": "fps", "type": "float"},
                {"name": "detections_json", "type": "string"},
            ]
        }),
        "gps_stream": json.dumps({
            "type": "record", "name": "GpsImuRecord",
            "namespace": "com.av.sensors",
            "fields": [
                {"name": "record_id", "type": "string"},
                {"name": "vehicle_id", "type": "string"},
                {"name": "timestamp", "type": "long"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "altitude_m", "type": "double"},
                {"name": "speed_mps", "type": "double"},
                {"name": "heading_deg", "type": "double"},
            ]
        }),
    }

    def __init__(self):
        self.settings = get_settings()
        self._client: Optional[Any] = None
        self._registered_schemas: Dict[str, int] = {}

    def connect(self):
        if not HAS_KAFKA:
            return
        try:
            self._client = SchemaRegistryClient({"url": self.settings.schema_registry_url})
            logger.info(f"Schema Registry connected: {self.settings.schema_registry_url}")
        except Exception as e:
            logger.warning(f"Schema Registry not available: {e}")

    def get_schema(self, topic: str) -> Optional[str]:
        return self.AVRO_SCHEMAS.get(topic)

    def register_schemas(self):
        """Register all sensor schemas on startup."""
        if not self._client:
            return
        for topic, schema_str in self.AVRO_SCHEMAS.items():
            try:
                from confluent_kafka.schema_registry import Schema
                schema_id = self._client.register_schema(
                    f"{topic}-value",
                    Schema(schema_str, schema_type="AVRO")
                )
                self._registered_schemas[topic] = schema_id
                logger.info(f"Registered schema for '{topic}': id={schema_id}")
            except Exception as e:
                logger.warning(f"Could not register schema for '{topic}': {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Consumer
# ─────────────────────────────────────────────────────────────────────────────

class SensorKafkaConsumer:
    """
    Kafka consumer for all sensor topics.
    Diagram: Kafka Cluster → topics: lidar_raw, camera_meta, gps_stream
    """

    TOPIC_SENSOR_MAP: Dict[str, SensorType] = {
        "lidar_raw": SensorType.LIDAR,
        "camera_meta": SensorType.CAMERA,
        "gps_stream": SensorType.GPS_IMU,
        "radar_stream": SensorType.RADAR,
        "can_bus_stream": SensorType.CAN_BUS,
        "ultrasonic_stream": SensorType.ULTRASONIC,
    }

    def __init__(self, topics: Optional[List[str]] = None):
        self.settings = get_settings()
        self.topics = topics or list(self.TOPIC_SENSOR_MAP.keys())
        self._consumer: Optional[Any] = None
        self._running = False
        self._message_count = 0
        self._error_count = 0

    def _build_config(self) -> Dict[str, Any]:
        return {
            "bootstrap.servers": self.settings.kafka_bootstrap_servers,
            "group.id": self.settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,   # manual commit for exactly-once
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
            "fetch.max.bytes": 52428800,    # 50MB
        }

    def start(self):
        if not HAS_KAFKA:
            logger.warning("Kafka not available — running in mock mode")
            return
        self._consumer = Consumer(self._build_config())
        self._consumer.subscribe(self.topics)
        self._running = True
        logger.info(f"Kafka consumer started, topics: {self.topics}")

    def stop(self):
        self._running = False
        if self._consumer:
            self._consumer.close()
            logger.info(f"Kafka consumer stopped. Processed: {self._message_count}, Errors: {self._error_count}")

    def commit(self):
        """Manual commit for exactly-once semantics."""
        if self._consumer:
            self._consumer.commit(asynchronous=False)

    def consume_batch(self, batch_size: int = 1000, timeout_s: float = 1.0) -> List[Dict[str, Any]]:
        """
        Consume a batch of messages from Kafka.
        Returns raw message dicts for downstream processing.
        """
        if not HAS_KAFKA or not self._consumer:
            return self._generate_mock_batch(batch_size)

        messages = []
        for _ in range(batch_size):
            msg = self._consumer.poll(timeout=timeout_s)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.error(f"Kafka error: {msg.error()}")
                self._error_count += 1
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                value["_kafka_topic"] = msg.topic()
                value["_kafka_partition"] = msg.partition()
                value["_kafka_offset"] = msg.offset()
                messages.append(value)
                self._message_count += 1
            except Exception as e:
                logger.warning(f"Failed to decode message: {e}")
                self._error_count += 1

        return messages

    async def consume_stream(
        self,
        handler: Callable[[Dict[str, Any]], None],
        poll_interval_s: float = 0.1
    ):
        """
        Async streaming consumer — continuously polls and calls handler.
        Used by StreamingDriver.
        """
        self.start()
        try:
            while self._running:
                batch = self.consume_batch(batch_size=100, timeout_s=poll_interval_s)
                for msg in batch:
                    await asyncio.get_event_loop().run_in_executor(None, handler, msg)
                if batch:
                    self.commit()
                await asyncio.sleep(poll_interval_s)
        finally:
            self.stop()

    def get_offsets(self) -> Dict[str, Dict[int, int]]:
        """Get current committed offsets per topic/partition."""
        if not self._consumer:
            return {}
        offsets = {}
        for topic in self.topics:
            offsets[topic] = {}
            # In production, enumerate partitions from metadata
        return offsets

    def _generate_mock_batch(self, size: int) -> List[Dict[str, Any]]:
        """Generate realistic mock sensor data for development/testing."""
        import random
        import uuid
        from datetime import timezone

        records = []
        topics = list(self.TOPIC_SENSOR_MAP.keys())
        base_ts = datetime.now(timezone.utc).timestamp()

        for i in range(min(size, 100)):
            topic = random.choice(topics)
            sensor_type = self.TOPIC_SENSOR_MAP[topic]
            ts = base_ts + i * 0.01

            base = {
                "_kafka_topic": topic,
                "_kafka_partition": random.randint(0, 3),
                "_kafka_offset": self._message_count + i,
                "record_id": str(uuid.uuid4()),
                "vehicle_id": f"AV-{random.randint(1, 10):03d}",
                "sensor_id": f"{sensor_type.value}-{random.randint(0, 5)}",
                "sensor_type": sensor_type.value,
                "timestamp": ts,
                "sequence_number": i,
            }

            if sensor_type == SensorType.GPS_IMU:
                base.update({
                    "latitude": 37.7749 + random.uniform(-0.01, 0.01),
                    "longitude": -122.4194 + random.uniform(-0.01, 0.01),
                    "altitude_m": 10.0 + random.uniform(-2, 2),
                    "speed_mps": random.uniform(0, 30),
                    "heading_deg": random.uniform(0, 360),
                    "gps_fix_type": 3,
                    "gps_accuracy_m": random.uniform(0.3, 2.0),
                    "accel_x": random.uniform(-5, 5),
                    "accel_y": random.uniform(-5, 5),
                    "accel_z": 9.81 + random.uniform(-0.5, 0.5),
                    "gyro_x": random.uniform(-0.5, 0.5),
                    "gyro_y": random.uniform(-0.5, 0.5),
                    "gyro_z": random.uniform(-0.5, 0.5),
                    "pitch_deg": random.uniform(-5, 5),
                    "roll_deg": random.uniform(-5, 5),
                })
            elif sensor_type == SensorType.LIDAR:
                base.update({
                    "point_count": random.randint(100000, 300000),
                    "rotation_rate_hz": 10.0,
                    "range_min": 0.1,
                    "range_max": 200.0,
                })
            elif sensor_type == SensorType.CAN_BUS:
                base.update({
                    "vehicle_speed_kmh": random.uniform(0, 120),
                    "steering_angle_deg": random.uniform(-45, 45),
                    "brake_pressure_bar": random.uniform(0, 20),
                    "throttle_position_pct": random.uniform(0, 80),
                    "engine_rpm": random.uniform(800, 4000),
                    "gear": random.randint(1, 6),
                    "abs_active": False,
                    "traction_control_active": False,
                    "odometer_km": random.uniform(0, 50000),
                })
            elif sensor_type == SensorType.RADAR:
                base.update({
                    "frequency_ghz": random.uniform(76, 81),
                    "detected_objects": [
                        {"object_id": j, "range_m": random.uniform(5, 200),
                         "azimuth_deg": random.uniform(-60, 60),
                         "elevation_deg": 0.0,
                         "relative_speed_mps": random.uniform(-30, 30),
                         "rcs_dbsm": random.uniform(-10, 20)}
                        for j in range(random.randint(0, 10))
                    ],
                })

            records.append(base)

        self._message_count += len(records)
        return records


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Producer (for DLQ publishing, etc.)
# ─────────────────────────────────────────────────────────────────────────────

class SensorKafkaProducer:
    """Kafka producer for publishing to DLQ and downstream topics."""

    def __init__(self):
        self.settings = get_settings()
        self._producer: Optional[Any] = None

    def start(self):
        if not HAS_KAFKA:
            return
        self._producer = Producer({
            "bootstrap.servers": self.settings.kafka_bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 200,
            "enable.idempotence": True,
        })
        logger.info("Kafka producer started")

    def publish(self, topic: str, key: str, value: Dict[str, Any]):
        if not self._producer:
            logger.debug(f"[MOCK] Publish → {topic}: {key}")
            return
        self._producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
            callback=self._delivery_callback
        )
        self._producer.poll(0)

    def publish_to_dlq(self, record: Dict[str, Any], reason: str):
        """Send failed record to Dead Letter Queue."""
        dlq_msg = {
            "original_record_id": record.get("record_id", "unknown"),
            "sensor_type": record.get("sensor_type", "unknown"),
            "failure_reason": reason,
            "raw_payload": json.dumps(record),
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.publish(self.settings.rabbitmq_dlq_queue, record.get("record_id", "unknown"), dlq_msg)

    def flush(self):
        if self._producer:
            self._producer.flush()

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            logger.error(f"Kafka delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
