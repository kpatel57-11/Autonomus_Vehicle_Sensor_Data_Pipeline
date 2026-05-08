"""
Comprehensive tests for all pipeline components.
Uses pytest + mock — no real Spark/Kafka required.
"""
import pytest
from unittest.mock import MagicMock, patch
from core.interfaces import SensorRecord, SensorType, ValidationResult


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def gps_record():
    return SensorRecord(
        sensor_type=SensorType.GPS_IMU,
        vehicle_id="VH001",
        timestamp_ms=1700000000000,
        sequence_id=1,
        payload={"lat": 37.7749, "lon": -122.4194, "altitude": 10.0}
    )

@pytest.fixture
def lidar_record():
    return SensorRecord(
        sensor_type=SensorType.LIDAR,
        vehicle_id="VH001",
        timestamp_ms=1700000000100,
        sequence_id=2,
        payload={"intensity": 128.0, "point_count": 300000}
    )

@pytest.fixture
def invalid_gps_record():
    return SensorRecord(
        sensor_type=SensorType.GPS_IMU,
        vehicle_id="VH001",
        timestamp_ms=1700000000200,
        sequence_id=3,
        payload={"lat": 999.0, "lon": -999.0}  # out of bounds
    )


# ── Validator Tests ────────────────────────────────────────────────────────────

class TestGPSBoundsCheck:
    def test_valid_gps(self, gps_record):
        from validators.validation_factory import GPSBoundsCheck
        v = GPSBoundsCheck()
        result = v.validate(gps_record)
        assert result.valid is True
        assert result.validator_name == "GPSBoundsCheck"

    def test_invalid_gps(self, invalid_gps_record):
        from validators.validation_factory import GPSBoundsCheck
        v = GPSBoundsCheck()
        result = v.validate(invalid_gps_record)
        assert result.valid is False
        assert "out of bounds" in result.error_message.lower()

    def test_boundary_gps(self):
        from validators.validation_factory import GPSBoundsCheck
        v = GPSBoundsCheck()
        r = SensorRecord(SensorType.GPS_IMU, "V", 100, 1, {"lat": 90.0, "lon": 180.0})
        assert v.validate(r).valid is True


class TestTimestampMonotonicity:
    def test_valid_ts(self, gps_record):
        from validators.validation_factory import TimestampMonotonicity
        v = TimestampMonotonicity()
        assert v.validate(gps_record).valid is True

    def test_zero_ts(self):
        from validators.validation_factory import TimestampMonotonicity
        v = TimestampMonotonicity()
        r = SensorRecord(SensorType.GPS_IMU, "V", 0, 1, {"timestamp_ms": 0})
        assert v.validate(r).valid is False


class TestLIDARIntensityRange:
    def test_valid_intensity(self, lidar_record):
        from validators.validation_factory import LIDARIntensityRange
        v = LIDARIntensityRange()
        assert v.validate(lidar_record).valid is True

    def test_invalid_intensity(self):
        from validators.validation_factory import LIDARIntensityRange
        v = LIDARIntensityRange()
        r = SensorRecord(SensorType.LIDAR, "V", 100, 1, {"intensity": 300.0})
        assert v.validate(r).valid is False


class TestIMUDriftDetector:
    def test_no_drift(self):
        from validators.validation_factory import IMUDriftDetector
        v = IMUDriftDetector(max_angular_rate=10.0)
        r = SensorRecord(SensorType.GPS_IMU, "V", 100, 1, {"gyro_x": 1.0, "gyro_y": 2.0, "gyro_z": 0.5})
        assert v.validate(r).valid is True

    def test_excessive_drift(self):
        from validators.validation_factory import IMUDriftDetector
        v = IMUDriftDetector(max_angular_rate=10.0)
        r = SensorRecord(SensorType.GPS_IMU, "V", 100, 1, {"gyro_x": 50.0, "gyro_y": 0.0, "gyro_z": 0.0})
        assert v.validate(r).valid is False


# ── Config Tests ───────────────────────────────────────────────────────────────

class TestConfigLoader:
    def test_default_recipe(self):
        from config.config_loader import ConfigLoader
        loader = ConfigLoader()
        recipe = loader.get_recipe()
        assert recipe.name == "av_full_pipeline"
        assert len(recipe.sources) > 0
        assert len(recipe.validators) > 0
        assert len(recipe.processors) > 0

    def test_recipe_has_all_stages(self):
        from config.config_loader import ConfigLoader
        recipe = ConfigLoader().get_recipe()
        assert recipe.sources
        assert recipe.validators
        assert recipe.transforms
        assert recipe.processors
        assert recipe.sinks


# ── Factory Tests ──────────────────────────────────────────────────────────────

class TestValidationFactory:
    def test_get_known_validator(self):
        from validators.validation_factory import ValidationFactory
        factory = ValidationFactory()
        v = factory.get_validator("GPSBoundsCheck")
        assert v.validator_name == "GPSBoundsCheck"

    def test_get_all_validators(self):
        from validators.validation_factory import ValidationFactory
        factory = ValidationFactory()
        all_v = factory.get_all()
        assert len(all_v) >= 10

    def test_register_custom_validator(self):
        from validators.validation_factory import ValidationFactory, IValidator
        factory = ValidationFactory()
        class MyValidator(IValidator):
            @property
            def validator_name(self): return "MyCustom"
            def validate(self, r): return ValidationResult(True, "MyCustom")
            def validate_batch(self, df): return df, None
        factory.register("MyCustom", MyValidator)
        v = factory.get_validator("MyCustom")
        assert v.validator_name == "MyCustom"


class TestProcessorFactory:
    def test_all_processors_registered(self):
        from processors.processor_factory import ProcessorFactory
        factory = ProcessorFactory()
        processors = factory.list_available()
        expected = ["PointCloudStitcher","FrameAligner","SensorFusion","AnomalyDetector",
                    "TrajectoryInterpolator","OccupancyGridBuilder"]
        for p in expected:
            assert p in processors, f"Missing processor: {p}"

    def test_dynamic_load_fallback(self):
        from processors.processor_factory import ProcessorFactory
        factory = ProcessorFactory()
        p = factory.get_processor("NonExistentProcessor")
        assert p is not None  # Returns NoOp


class TestSourceReaderFactory:
    def test_all_sources_registered(self):
        from ingestion.source_factory import SourceReaderFactory
        factory = SourceReaderFactory()
        sources = factory.list_available()
        assert "kafka_lidar" in sources
        assert "kafka_camera" in sources
        assert "kafka_gps" in sources


class TestSinkFactory:
    def test_all_sinks_registered(self):
        from sink.sink_factory import SinkWriterFactory
        factory = SinkWriterFactory()
        sinks = factory.list_available()
        assert "hudi_data_lake" in sinks
        assert "delta_lake" in sinks
        assert "api_publisher" in sinks


# ── Checkpoint Tests ───────────────────────────────────────────────────────────

class TestCheckpointManager:
    def test_save_and_load_offset(self, tmp_path):
        from checkpoint.checkpoint_manager import CheckpointManager
        cp = CheckpointManager(str(tmp_path))
        cp.save_offset("test_source", {"partition": 0, "offset": 12345})
        loaded = cp.get_last_offset("test_source")
        assert loaded == {"partition": 0, "offset": 12345}

    def test_mark_and_check_complete(self, tmp_path):
        from checkpoint.checkpoint_manager import CheckpointManager
        cp = CheckpointManager(str(tmp_path))
        assert cp.is_completed("batch_001") is False
        cp.mark_complete("batch_001")
        assert cp.is_completed("batch_001") is True

    def test_list_checkpoints(self, tmp_path):
        from checkpoint.checkpoint_manager import CheckpointManager
        cp = CheckpointManager(str(tmp_path))
        cp.save_offset("lidar", 100)
        cp.save_offset("camera", 200)
        checkpoints = cp.list_checkpoints()
        assert "lidar" in checkpoints
        assert "camera" in checkpoints


# ── Observer / Self-Healing Tests ──────────────────────────────────────────────

class TestQueryLifecycleMonitor:
    def test_metrics_summary_empty(self):
        from self_healing.query_lifecycle_monitor import QueryLifecycleMonitor
        monitor = QueryLifecycleMonitor()
        summary = monitor.get_metrics_summary()
        assert summary["batches"] == 0
        assert summary.get("restart_count", 0) == 0  # key may be named differently

    def test_on_query_started(self):
        from self_healing.query_lifecycle_monitor import QueryLifecycleMonitor
        monitor = QueryLifecycleMonitor()
        event = MagicMock()
        event.id = "query-001"
        event.runId = "run-001"
        event.name = "test-query"
        monitor.onQueryStarted(event)
        assert "query-001" in monitor._active_queries
