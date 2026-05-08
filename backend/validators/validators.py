"""
validators/validators.py
Stage 2: VALIDATORS — ValidationFactory with 20+ IValidator implementations.
Diagram: GPSBoundsCheck, TimestampMonotonicity, LIDARIntensityRange,
         IMUDriftDetector, CameraExposureValidator + more.
"""
from __future__ import annotations
import math
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone
from loguru import logger


# ─────────────────────────────────────────────────────────────────────────────
# IValidator Interface
# ─────────────────────────────────────────────────────────────────────────────

class ValidationResult:
    def __init__(self, passed: bool, message: str = "", field: str = ""):
        self.passed = passed
        self.message = message
        self.field = field

    def __bool__(self):
        return self.passed

    def __repr__(self):
        status = "✓" if self.passed else "✗"
        return f"ValidationResult({status} {self.field}: {self.message})"


class IValidator(ABC):
    """Strategy interface for all validators."""

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    def sensor_types(self) -> List[str]:
        """Which sensor types this validator applies to. Empty = all."""
        return []

    @abstractmethod
    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        ...

    def applies_to(self, sensor_type: str) -> bool:
        if not self.sensor_types:
            return True
        return sensor_type in self.sensor_types


# ─────────────────────────────────────────────────────────────────────────────
# GPS Validators
# ─────────────────────────────────────────────────────────────────────────────

class GPSBoundsCheck(IValidator):
    """Check lat/lon in valid range — no backward jumps."""
    name = "GPSBoundsCheck"
    sensor_types = ["gps_imu"]

    def __init__(self, max_jump_m: float = 50.0):
        self.max_jump_m = max_jump_m
        self._last_lat: Optional[float] = None
        self._last_lon: Optional[float] = None

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        lat = record.get("latitude")
        lon = record.get("longitude")

        if lat is None or lon is None:
            return ValidationResult(False, "Missing latitude/longitude", "gps")

        if not (-90.0 <= lat <= 90.0):
            return ValidationResult(False, f"Latitude {lat} out of range [-90, 90]", "latitude")
        if not (-180.0 <= lon <= 180.0):
            return ValidationResult(False, f"Longitude {lon} out of range [-180, 180]", "longitude")

        # Check backward jumps
        if self._last_lat is not None:
            dist = self._haversine(self._last_lat, self._last_lon, lat, lon)
            if dist > self.max_jump_m:
                return ValidationResult(
                    False, f"GPS jump of {dist:.1f}m exceeds max {self.max_jump_m}m", "gps_jump"
                )

        self._last_lat = lat
        self._last_lon = lon
        return ValidationResult(True, "GPS bounds valid")

    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Distance in meters between two GPS points."""
        R = 6371000
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlam = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


class GPSFixTypeValidator(IValidator):
    """Ensure GPS has a valid fix (>= 2D fix)."""
    name = "GPSFixTypeValidator"
    sensor_types = ["gps_imu"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        fix_type = record.get("gps_fix_type", 0)
        if fix_type < 2:
            return ValidationResult(False, f"GPS fix type {fix_type} insufficient (need >= 2)", "gps_fix_type")
        return ValidationResult(True, f"GPS fix type {fix_type} valid")


# ─────────────────────────────────────────────────────────────────────────────
# Timestamp Validators
# ─────────────────────────────────────────────────────────────────────────────

class TimestampMonotonicityValidator(IValidator):
    """Verify timestamps are monotonically increasing — no backward jumps."""
    name = "TimestampMonotonicity"

    def __init__(self):
        self._last_ts: Dict[str, float] = {}

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        ts = record.get("timestamp")
        vehicle_id = record.get("vehicle_id", "unknown")
        sensor_id = record.get("sensor_id", "unknown")
        key = f"{vehicle_id}:{sensor_id}"

        if ts is None:
            return ValidationResult(False, "Missing timestamp", "timestamp")

        ts_val = ts if isinstance(ts, (int, float)) else datetime.fromisoformat(str(ts)).timestamp()

        if key in self._last_ts:
            if ts_val < self._last_ts[key]:
                return ValidationResult(
                    False, f"Timestamp went backward: {self._last_ts[key]} → {ts_val}", "timestamp"
                )
            # Allow max 1 second gap for streaming sensor data
            gap = ts_val - self._last_ts[key]
            if gap > 10.0:
                return ValidationResult(False, f"Timestamp gap too large: {gap:.2f}s", "timestamp_gap")

        self._last_ts[key] = ts_val
        return ValidationResult(True, "Timestamp monotonic")


class TimestampFreshnessValidator(IValidator):
    """Reject records older than max_age_s seconds."""
    name = "TimestampFreshnessValidator"

    def __init__(self, max_age_s: float = 60.0):
        self.max_age_s = max_age_s

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        ts = record.get("timestamp")
        if ts is None:
            return ValidationResult(False, "Missing timestamp", "timestamp")
        ts_val = ts if isinstance(ts, (int, float)) else datetime.fromisoformat(str(ts)).timestamp()
        now = datetime.now(timezone.utc).timestamp()
        age = now - ts_val
        if age > self.max_age_s:
            return ValidationResult(False, f"Record is {age:.1f}s old (max {self.max_age_s}s)", "timestamp_age")
        return ValidationResult(True, f"Record fresh ({age:.2f}s old)")


# ─────────────────────────────────────────────────────────────────────────────
# LIDAR Validators
# ─────────────────────────────────────────────────────────────────────────────

class LIDARIntensityRangeValidator(IValidator):
    """Validate LIDAR intensity within bounds."""
    name = "LIDARIntensityRange"
    sensor_types = ["lidar"]

    def __init__(self, min_intensity: float = 0.0, max_intensity: float = 255.0,
                 min_points: int = 1000):
        self.min_intensity = min_intensity
        self.max_intensity = max_intensity
        self.min_points = min_points

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        point_count = record.get("point_count", 0)
        if point_count < self.min_points:
            return ValidationResult(
                False, f"LIDAR point count {point_count} < min {self.min_points}", "point_count"
            )

        points = record.get("points", [])
        for i, pt in enumerate(points[:100]):  # Sample check first 100
            intensity = pt.get("intensity", 0)
            if not (self.min_intensity <= intensity <= self.max_intensity):
                return ValidationResult(
                    False, f"Point {i} intensity {intensity} out of range", "intensity"
                )
        return ValidationResult(True, f"LIDAR valid ({point_count} points)")


class LIDARRangeValidator(IValidator):
    """Check LIDAR range bounds."""
    name = "LIDARRangeValidator"
    sensor_types = ["lidar"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        range_min = record.get("range_min", 0)
        range_max = record.get("range_max", 0)
        if range_min < 0 or range_min >= range_max:
            return ValidationResult(False, f"Invalid LIDAR range: [{range_min}, {range_max}]", "range")
        return ValidationResult(True, "LIDAR range valid")


# ─────────────────────────────────────────────────────────────────────────────
# IMU Validators
# ─────────────────────────────────────────────────────────────────────────────

class IMUDriftDetector(IValidator):
    """Detect gyroscope drift — excessive rotation rate."""
    name = "IMUDriftDetector"
    sensor_types = ["gps_imu"]

    def __init__(self, max_gyro_rad_s: float = 5.0, max_accel_m_s2: float = 50.0):
        self.max_gyro = max_gyro_rad_s
        self.max_accel = max_accel_m_s2

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        for axis in ["x", "y", "z"]:
            gyro = record.get(f"gyro_{axis}")
            if gyro is not None and abs(gyro) > self.max_gyro:
                return ValidationResult(
                    False, f"Gyro {axis} = {gyro:.3f} rad/s exceeds max {self.max_gyro}", f"gyro_{axis}"
                )

        for axis in ["x", "y", "z"]:
            accel = record.get(f"accel_{axis}")
            if accel is not None and abs(accel) > self.max_accel:
                return ValidationResult(
                    False, f"Accel {axis} = {accel:.2f} m/s² exceeds max {self.max_accel}", f"accel_{axis}"
                )

        return ValidationResult(True, "IMU within drift thresholds")


class IMUConsistencyValidator(IValidator):
    """Verify IMU values are physically consistent."""
    name = "IMUConsistencyValidator"
    sensor_types = ["gps_imu"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        accel_z = record.get("accel_z")
        if accel_z is not None:
            # On flat ground, accel_z should be ~9.81 ± 2 m/s²
            if not (7.0 <= abs(accel_z) <= 12.0):
                return ValidationResult(
                    False, f"Unexpected vertical acceleration: {accel_z:.2f} m/s²", "accel_z"
                )
        return ValidationResult(True, "IMU consistent")


# ─────────────────────────────────────────────────────────────────────────────
# Camera Validators
# ─────────────────────────────────────────────────────────────────────────────

class CameraExposureValidator(IValidator):
    """Validate camera exposure settings."""
    name = "CameraExposureValidator"
    sensor_types = ["camera"]

    def __init__(self, min_exposure_ms: float = 1.0, max_exposure_ms: float = 100.0,
                 min_iso: int = 50, max_iso: int = 6400):
        self.min_exposure = min_exposure_ms
        self.max_exposure = max_exposure_ms
        self.min_iso = min_iso
        self.max_iso = max_iso

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        exp = record.get("exposure_time_ms")
        iso = record.get("iso")

        if exp is not None and not (self.min_exposure <= exp <= self.max_exposure):
            return ValidationResult(
                False, f"Exposure {exp}ms out of range [{self.min_exposure}, {self.max_exposure}]", "exposure"
            )
        if iso is not None and not (self.min_iso <= iso <= self.max_iso):
            return ValidationResult(
                False, f"ISO {iso} out of range [{self.min_iso}, {self.max_iso}]", "iso"
            )
        return ValidationResult(True, "Camera exposure valid")


class CameraResolutionValidator(IValidator):
    """Ensure camera frame dimensions are valid."""
    name = "CameraResolutionValidator"
    sensor_types = ["camera"]

    VALID_RESOLUTIONS = {(1920, 1080), (1280, 720), (3840, 2160)}

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        w = record.get("width", 0)
        h = record.get("height", 0)
        if (w, h) not in self.VALID_RESOLUTIONS and w > 0:
            return ValidationResult(False, f"Unexpected resolution {w}x{h}", "resolution")
        return ValidationResult(True, f"Resolution {w}x{h} valid")


class CameraFpsValidator(IValidator):
    """Validate camera framerate."""
    name = "CameraFpsValidator"
    sensor_types = ["camera"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        fps = record.get("fps", 0)
        if not (1.0 <= fps <= 120.0):
            return ValidationResult(False, f"FPS {fps} out of range [1, 120]", "fps")
        return ValidationResult(True, f"FPS {fps} valid")


# ─────────────────────────────────────────────────────────────────────────────
# CAN Bus Validators
# ─────────────────────────────────────────────────────────────────────────────

class CanBusSpeedValidator(IValidator):
    """Validate vehicle speed is plausible."""
    name = "CanBusSpeedValidator"
    sensor_types = ["can_bus"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        speed = record.get("vehicle_speed_kmh")
        if speed is None:
            return ValidationResult(False, "Missing vehicle_speed_kmh", "speed")
        if speed < 0 or speed > 250:
            return ValidationResult(False, f"Speed {speed} km/h out of range [0, 250]", "speed")
        return ValidationResult(True, f"Speed {speed} km/h valid")


class CanBusSteeringValidator(IValidator):
    """Validate steering angle."""
    name = "CanBusSteeringValidator"
    sensor_types = ["can_bus"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        angle = record.get("steering_angle_deg")
        if angle is not None and not (-540 <= angle <= 540):
            return ValidationResult(False, f"Steering angle {angle}° out of range", "steering")
        return ValidationResult(True, "Steering valid")


class CanBusThrottleBrakeValidator(IValidator):
    """Ensure throttle and brake aren't both fully applied."""
    name = "CanBusThrottleBrakeValidator"
    sensor_types = ["can_bus"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        throttle = record.get("throttle_position_pct", 0)
        brake = record.get("brake_pressure_bar", 0)
        if throttle > 80 and brake > 50:
            return ValidationResult(
                False, f"Simultaneous high throttle ({throttle}%) and brake ({brake} bar)", "throttle_brake"
            )
        return ValidationResult(True, "Throttle/brake consistent")


# ─────────────────────────────────────────────────────────────────────────────
# Radar Validators
# ─────────────────────────────────────────────────────────────────────────────

class RadarFrequencyValidator(IValidator):
    """Check radar operating frequency is 76-81 GHz."""
    name = "RadarFrequencyValidator"
    sensor_types = ["radar"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        freq = record.get("frequency_ghz")
        if freq is not None and not (76.0 <= freq <= 81.0):
            return ValidationResult(False, f"Radar frequency {freq} GHz out of range [76, 81]", "frequency")
        return ValidationResult(True, "Radar frequency valid")


class RadarObjectValidator(IValidator):
    """Validate detected radar objects."""
    name = "RadarObjectValidator"
    sensor_types = ["radar"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        objects = record.get("detected_objects", [])
        for obj in objects:
            if obj.get("range_m", 0) < 0:
                return ValidationResult(False, f"Negative radar range for object {obj.get('object_id')}", "range")
        return ValidationResult(True, f"Radar objects valid ({len(objects)} detected)")


# ─────────────────────────────────────────────────────────────────────────────
# Ultrasonic Validators
# ─────────────────────────────────────────────────────────────────────────────

class UltrasonicRangeValidator(IValidator):
    """Validate ultrasonic sensor readings."""
    name = "UltrasonicRangeValidator"
    sensor_types = ["ultrasonic"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        readings = record.get("readings", [])
        for r in readings:
            dist = r.get("distance_cm", 0)
            if not (0 <= dist <= 600):
                return ValidationResult(
                    False, f"Ultrasonic distance {dist}cm out of range [0, 600]", "distance"
                )
        return ValidationResult(True, f"Ultrasonic readings valid ({len(readings)} sensors)")


# ─────────────────────────────────────────────────────────────────────────────
# Generic / Cross-Sensor Validators
# ─────────────────────────────────────────────────────────────────────────────

class RequiredFieldsValidator(IValidator):
    """Ensure all required fields are present."""
    name = "RequiredFieldsValidator"

    REQUIRED_FIELDS = ["record_id", "vehicle_id", "sensor_id", "sensor_type", "timestamp"]

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        missing = [f for f in self.REQUIRED_FIELDS if record.get(f) is None]
        if missing:
            return ValidationResult(False, f"Missing required fields: {missing}", "required_fields")
        return ValidationResult(True, "All required fields present")


class VehicleIdFormatValidator(IValidator):
    """Validate vehicle ID format."""
    name = "VehicleIdFormatValidator"

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        vid = record.get("vehicle_id", "")
        if not vid.startswith("AV-") or len(vid) != 6:
            return ValidationResult(False, f"Invalid vehicle_id format: '{vid}' (expect AV-XXX)", "vehicle_id")
        return ValidationResult(True, f"Vehicle ID '{vid}' valid")


class DuplicateRecordDetector(IValidator):
    """Detect duplicate record IDs (using in-memory set, or Redis in prod)."""
    name = "DuplicateRecordDetector"

    def __init__(self, window_size: int = 10000):
        self._seen: set = set()
        self._window_size = window_size

    def validate(self, record: Dict[str, Any]) -> ValidationResult:
        rid = record.get("record_id")
        if rid in self._seen:
            return ValidationResult(False, f"Duplicate record_id: {rid}", "record_id")
        self._seen.add(rid)
        if len(self._seen) > self._window_size:
            # Rolling window — remove oldest (simplified: clear half)
            self._seen = set(list(self._seen)[self._window_size // 2:])
        return ValidationResult(True, "Record ID unique")


# ─────────────────────────────────────────────────────────────────────────────
# Validation Factory
# ─────────────────────────────────────────────────────────────────────────────

class ValidationFactory:
    """
    Factory that creates and manages all validators.
    Diagram: ValidationFactory → 20+ IValidator implementations
    """

    # Registry of all available validators
    _REGISTRY: Dict[str, type] = {
        "GPSBoundsCheck": GPSBoundsCheck,
        "GPSFixTypeValidator": GPSFixTypeValidator,
        "TimestampMonotonicity": TimestampMonotonicityValidator,
        "TimestampFreshnessValidator": TimestampFreshnessValidator,
        "LIDARIntensityRange": LIDARIntensityRangeValidator,
        "LIDARRangeValidator": LIDARRangeValidator,
        "IMUDriftDetector": IMUDriftDetector,
        "IMUConsistencyValidator": IMUConsistencyValidator,
        "CameraExposureValidator": CameraExposureValidator,
        "CameraResolutionValidator": CameraResolutionValidator,
        "CameraFpsValidator": CameraFpsValidator,
        "CanBusSpeedValidator": CanBusSpeedValidator,
        "CanBusSteeringValidator": CanBusSteeringValidator,
        "CanBusThrottleBrakeValidator": CanBusThrottleBrakeValidator,
        "RadarFrequencyValidator": RadarFrequencyValidator,
        "RadarObjectValidator": RadarObjectValidator,
        "UltrasonicRangeValidator": UltrasonicRangeValidator,
        "RequiredFieldsValidator": RequiredFieldsValidator,
        "VehicleIdFormatValidator": VehicleIdFormatValidator,
        "DuplicateRecordDetector": DuplicateRecordDetector,
    }

    @classmethod
    def create(cls, name: str, **kwargs) -> IValidator:
        if name not in cls._REGISTRY:
            raise ValueError(f"Unknown validator: '{name}'. Available: {list(cls._REGISTRY.keys())}")
        return cls._REGISTRY[name](**kwargs)

    @classmethod
    def create_all(cls) -> List[IValidator]:
        """Instantiate all validators."""
        return [klass() for klass in cls._REGISTRY.values()]

    @classmethod
    def create_for_sensor(cls, sensor_type: str) -> List[IValidator]:
        """Create validators applicable to a specific sensor type."""
        all_validators = cls.create_all()
        return [v for v in all_validators if v.applies_to(sensor_type)]

    @classmethod
    def register(cls, name: str, validator_class: type):
        """Register a custom validator."""
        cls._REGISTRY[name] = validator_class

    @classmethod
    def list_validators(cls) -> List[str]:
        return list(cls._REGISTRY.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Validation Engine — runs all applicable validators on a record
# ─────────────────────────────────────────────────────────────────────────────

class ValidationEngine:
    """
    Runs all applicable validators on each record.
    Returns (passed, list_of_failures).
    """

    def __init__(self, validator_names: Optional[List[str]] = None):
        if validator_names:
            self._validators = [ValidationFactory.create(n) for n in validator_names]
        else:
            self._validators = ValidationFactory.create_all()

        self._pass_count = 0
        self._fail_count = 0

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, List[ValidationResult]]:
        sensor_type = record.get("sensor_type", "")
        failures = []

        for v in self._validators:
            if not v.applies_to(sensor_type):
                continue
            result = v.validate(record)
            if not result.passed:
                failures.append(result)

        passed = len(failures) == 0
        if passed:
            self._pass_count += 1
        else:
            self._fail_count += 1

        return passed, failures

    def validate_batch(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Validate a batch. Returns (valid_records, invalid_records).
        Invalid records have 'validation_errors' appended.
        """
        valid, invalid = [], []
        for record in records:
            passed, failures = self.validate(record)
            if passed:
                valid.append(record)
            else:
                record["validation_errors"] = [f.message for f in failures]
                invalid.append(record)
        return valid, invalid

    @property
    def stats(self) -> Dict[str, int]:
        return {"passed": self._pass_count, "failed": self._fail_count,
                "total": self._pass_count + self._fail_count}
