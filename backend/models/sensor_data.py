"""
models/sensor_data.py
Pydantic data models for all sensor types and pipeline records.
"""
from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator
import uuid


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class SensorType(str, Enum):
    LIDAR = "lidar"
    CAMERA = "camera"
    GPS_IMU = "gps_imu"
    RADAR = "radar"
    CAN_BUS = "can_bus"
    ULTRASONIC = "ultrasonic"


class PipelineMode(str, Enum):
    BATCH = "batch"
    STREAMING = "streaming"


class RecordStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    DLQ = "dlq"          # Dead Letter Queue
    PROCESSED = "processed"
    CHECKPOINTED = "checkpointed"


class ProcessingStage(str, Enum):
    SOURCE = "source"
    VALIDATION = "validation"
    TRANSFORM = "transform"
    PROCESSING = "processing"
    SINK = "sink"
    CHECKPOINT = "checkpoint"


# ─────────────────────────────────────────────────────────────────────────────
# Base Sensor Record
# ─────────────────────────────────────────────────────────────────────────────

class SensorMetadata(BaseModel):
    vehicle_id: str
    sensor_id: str
    sensor_type: SensorType
    timestamp: datetime
    sequence_number: int
    firmware_version: str = "1.0.0"
    calibration_id: Optional[str] = None


class BaseSensorRecord(BaseModel):
    record_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    metadata: SensorMetadata
    raw_bytes: Optional[bytes] = None
    status: RecordStatus = RecordStatus.VALID
    processing_stage: ProcessingStage = ProcessingStage.SOURCE
    errors: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True
        json_encoders = {bytes: lambda v: v.hex() if v else None}


# ─────────────────────────────────────────────────────────────────────────────
# LIDAR Record — 3D Point Clouds, ~300K pts/frame
# ─────────────────────────────────────────────────────────────────────────────

class LidarPoint(BaseModel):
    x: float
    y: float
    z: float
    intensity: float = Field(ge=0.0, le=255.0)
    ring: int = Field(ge=0)
    timestamp_offset_ns: int = 0


class LidarRecord(BaseSensorRecord):
    """LIDAR: 3D point clouds, ~300K pts/frame"""
    points: List[LidarPoint] = Field(default_factory=list)
    point_count: int = 0
    scan_angle_min: float = -180.0
    scan_angle_max: float = 180.0
    range_min: float = 0.1   # meters
    range_max: float = 200.0  # meters
    rotation_rate_hz: float = 10.0

    @model_validator(mode="after")
    def sync_point_count(self) -> "LidarRecord":
        self.point_count = len(self.points)
        return self


# ─────────────────────────────────────────────────────────────────────────────
# Camera Record — 8 surround cams, 30fps each
# ─────────────────────────────────────────────────────────────────────────────

class BoundingBox(BaseModel):
    x: float
    y: float
    width: float
    height: float
    class_label: str
    confidence: float = Field(ge=0.0, le=1.0)
    track_id: Optional[int] = None


class CameraRecord(BaseSensorRecord):
    """Camera: 8 surround cameras, 30 fps each"""
    camera_index: int = Field(ge=0, le=7)
    width: int = 1920
    height: int = 1080
    fps: float = 30.0
    encoding: str = "H264"
    frame_data: Optional[bytes] = None
    frame_size_bytes: int = 0
    detections: List[BoundingBox] = Field(default_factory=list)
    exposure_time_ms: float = 10.0
    iso: int = 400


# ─────────────────────────────────────────────────────────────────────────────
# GPS/IMU Record — Position + Motion, 100Hz
# ─────────────────────────────────────────────────────────────────────────────

class GpsImuRecord(BaseSensorRecord):
    """GPS/IMU: Position + motion at 100 Hz"""
    # GPS
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    altitude_m: float
    gps_accuracy_m: float = 0.5
    gps_fix_type: int = Field(ge=0, le=3)  # 0=no fix, 3=3D fix

    # IMU
    accel_x: float  # m/s²
    accel_y: float
    accel_z: float
    gyro_x: float   # rad/s
    gyro_y: float
    gyro_z: float
    heading_deg: float = Field(ge=0.0, le=360.0)
    speed_mps: float = Field(ge=0.0)
    pitch_deg: float
    roll_deg: float


# ─────────────────────────────────────────────────────────────────────────────
# Radar Record — Object detection, 76-81 GHz
# ─────────────────────────────────────────────────────────────────────────────

class RadarObject(BaseModel):
    object_id: int
    range_m: float
    azimuth_deg: float
    elevation_deg: float
    relative_speed_mps: float
    rcs_dbsm: float          # Radar Cross Section


class RadarRecord(BaseSensorRecord):
    """Radar: Object detection, 76-81 GHz"""
    frequency_ghz: float = Field(ge=76.0, le=81.0)
    detected_objects: List[RadarObject] = Field(default_factory=list)
    max_range_m: float = 250.0
    azimuth_fov_deg: float = 120.0
    elevation_fov_deg: float = 30.0
    update_rate_hz: float = 20.0


# ─────────────────────────────────────────────────────────────────────────────
# CAN Bus Record — Speed, steering, brake, throttle
# ─────────────────────────────────────────────────────────────────────────────

class CanBusRecord(BaseSensorRecord):
    """CAN Bus: Speed, steering, brake, throttle"""
    vehicle_speed_kmh: float = Field(ge=0.0, le=300.0)
    steering_angle_deg: float = Field(ge=-540.0, le=540.0)
    brake_pressure_bar: float = Field(ge=0.0, le=200.0)
    throttle_position_pct: float = Field(ge=0.0, le=100.0)
    engine_rpm: float = Field(ge=0.0, le=8000.0)
    gear: int = Field(ge=-1, le=8)   # -1=reverse
    abs_active: bool = False
    traction_control_active: bool = False
    odometer_km: float = Field(ge=0.0)
    fuel_level_pct: Optional[float] = None
    battery_voltage_v: Optional[float] = None


# ─────────────────────────────────────────────────────────────────────────────
# Ultrasonic Record — Proximity sensors, 12 units
# ─────────────────────────────────────────────────────────────────────────────

class UltrasonicReading(BaseModel):
    sensor_index: int = Field(ge=0, le=11)
    distance_cm: float = Field(ge=0.0, le=600.0)
    is_valid: bool = True


class UltrasonicRecord(BaseSensorRecord):
    """Ultrasonic: 12 proximity sensors"""
    readings: List[UltrasonicReading] = Field(default_factory=list)
    update_rate_hz: float = 50.0
    min_detection_cm: float = 20.0
    max_detection_cm: float = 600.0


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline-level models
# ─────────────────────────────────────────────────────────────────────────────

SensorRecord = Union[
    LidarRecord, CameraRecord, GpsImuRecord,
    RadarRecord, CanBusRecord, UltrasonicRecord
]


class PipelineEvent(BaseModel):
    """Event emitted at each pipeline stage for monitoring/observability."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_run_id: str
    mode: PipelineMode
    stage: ProcessingStage
    records_in: int = 0
    records_out: int = 0
    records_failed: int = 0
    duration_ms: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    extra: Dict[str, Any] = Field(default_factory=dict)


class DLQRecord(BaseModel):
    """Dead Letter Queue record for failed/invalid data."""
    dlq_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    original_record_id: str
    sensor_type: SensorType
    failure_stage: ProcessingStage
    failure_reason: str
    raw_payload: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    retry_count: int = 0
    max_retries: int = 3


class CheckpointState(BaseModel):
    """Checkpoint state for exactly-once processing."""
    checkpoint_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_run_id: str
    mode: PipelineMode
    kafka_offsets: Dict[str, Dict[int, int]] = Field(default_factory=dict)
    last_processed_batch: Optional[str] = None
    records_processed: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PipelineConfig(BaseModel):
    """Runtime pipeline configuration loaded from MongoDB/PostgreSQL."""
    config_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str
    mode: PipelineMode
    kafka_topics: List[str]
    spark_config: Dict[str, Any] = Field(default_factory=dict)
    validator_config: Dict[str, Any] = Field(default_factory=dict)
    processor_config: Dict[str, Any] = Field(default_factory=dict)
    sink_config: Dict[str, Any] = Field(default_factory=dict)
    batch_size: int = 10000
    checkpoint_interval_s: int = 60
    dlq_topic: str = "sensor_dlq"
    enabled: bool = True
    version: str = "1.0.0"
