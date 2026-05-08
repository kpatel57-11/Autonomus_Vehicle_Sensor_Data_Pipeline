"""
Core Interfaces — Strategy pattern contracts for all pipeline components.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum


class SensorType(Enum):
    LIDAR = "lidar"
    CAMERA = "camera"
    GPS_IMU = "gps_imu"
    RADAR = "radar"
    CAN_BUS = "can_bus"
    ULTRASONIC = "ultrasonic"


class PipelineMode(Enum):
    BATCH = "batch"
    STREAM = "stream"


@dataclass
class SensorRecord:
    sensor_type: SensorType
    vehicle_id: str
    timestamp_ms: int
    sequence_id: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    valid: bool
    validator_name: str
    error_message: Optional[str] = None
    record_id: Optional[str] = None


@dataclass
class ProcessingResult:
    success: bool
    processor_name: str
    output: Any = None
    metrics: Dict[str, float] = field(default_factory=dict)


class ISourceReader(ABC):
    @abstractmethod
    def read(self, spark_session: Any, config: Dict[str, Any]) -> Any: pass
    @abstractmethod
    def get_schema(self) -> Any: pass
    @property
    @abstractmethod
    def source_name(self) -> str: pass


class IValidator(ABC):
    @abstractmethod
    def validate(self, record: SensorRecord) -> ValidationResult: pass
    @abstractmethod
    def validate_batch(self, df: Any) -> Any: pass
    @property
    @abstractmethod
    def validator_name(self) -> str: pass


class IProcessor(ABC):
    @abstractmethod
    def process(self, df: Any, config: Dict[str, Any]) -> ProcessingResult: pass
    @property
    @abstractmethod
    def processor_name(self) -> str: pass
    @property
    def requires_sensors(self) -> List[SensorType]: return []


class ISinkWriter(ABC):
    @abstractmethod
    def write(self, df: Any, config: Dict[str, Any]) -> bool: pass
    @abstractmethod
    def commit(self) -> bool: pass
    @property
    @abstractmethod
    def sink_name(self) -> str: pass


class ICheckpointManager(ABC):
    @abstractmethod
    def save_offset(self, source: str, offset: Any) -> None: pass
    @abstractmethod
    def get_last_offset(self, source: str) -> Optional[Any]: pass
    @abstractmethod
    def mark_complete(self, batch_id: str) -> None: pass
    @abstractmethod
    def is_completed(self, batch_id: str) -> bool: pass
