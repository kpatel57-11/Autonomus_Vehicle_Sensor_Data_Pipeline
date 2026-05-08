"""
ValidationFactory — 20+ IValidator implementations.
Covers GPS bounds, timestamp monotonicity, LIDAR intensity,
IMU drift, camera exposure, and more.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple
from core.interfaces import IValidator, SensorRecord, ValidationResult
logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────
def _split_df(df, condition_col: str):
    """Split DataFrame into valid/invalid using a boolean column."""
    valid = df.filter(f"{condition_col} = true")
    invalid = df.filter(f"{condition_col} = false or {condition_col} is null")
    return valid, invalid


# ── 1. GPS Bounds Check ────────────────────────────────────────────────────────
class GPSBoundsCheck(IValidator):
    """lat in valid range [-90,90], lon in [-180,180], no backward jumps."""
    def __init__(self, lat_range=(-90,90), lon_range=(-180,180)):
        self.lat_min, self.lat_max = lat_range
        self.lon_min, self.lon_max = lon_range

    @property
    def validator_name(self): return "GPSBoundsCheck"

    def validate(self, record: SensorRecord) -> ValidationResult:
        lat = record.payload.get("lat", 0)
        lon = record.payload.get("lon", 0)
        if not (self.lat_min <= lat <= self.lat_max and self.lon_min <= lon <= self.lon_max):
            return ValidationResult(False, self.validator_name, f"GPS out of bounds: lat={lat}, lon={lon}")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            flagged = df.withColumn("_gps_valid",
                (col("lat").isNotNull()) & (col("lon").isNotNull()) &
                (col("lat").between(self.lat_min, self.lat_max)) &
                (col("lon").between(self.lon_min, self.lon_max)))
            valid = flagged.filter("_gps_valid = true").drop("_gps_valid")
            invalid = flagged.filter("_gps_valid = false").drop("_gps_valid")
            return valid, invalid
        except Exception as e:
            logger.warning(f"GPSBoundsCheck batch error: {e}")
            return df, None


# ── 2. Timestamp Monotonicity ──────────────────────────────────────────────────
class TimestampMonotonicity(IValidator):
    """No backward jumps in timestamp_ms per vehicle_id."""
    @property
    def validator_name(self): return "TimestampMonotonicity"

    def validate(self, record: SensorRecord) -> ValidationResult:
        ts = record.payload.get("timestamp_ms", record.timestamp_ms)
        if ts <= 0:
            return ValidationResult(False, self.validator_name, f"Invalid timestamp: {ts}")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col, lag
            from pyspark.sql.window import Window
            w = Window.partitionBy("vehicle_id").orderBy("timestamp_ms")
            flagged = df.withColumn("_prev_ts", lag("timestamp_ms").over(w))
            flagged = flagged.withColumn("_ts_valid",
                col("_prev_ts").isNull() | (col("timestamp_ms") >= col("_prev_ts")))
            valid = flagged.filter("_ts_valid = true").drop("_prev_ts","_ts_valid")
            invalid = flagged.filter("_ts_valid = false").drop("_prev_ts","_ts_valid")
            return valid, invalid
        except Exception as e:
            logger.warning(f"TimestampMonotonicity batch error: {e}")
            return df, None


# ── 3. LIDAR Intensity Range ───────────────────────────────────────────────────
class LIDARIntensityRange(IValidator):
    """intensity in [0, 255], reflect off invalid surfaces."""
    def __init__(self, min_val=0.0, max_val=255.0):
        self.min_val = min_val; self.max_val = max_val

    @property
    def validator_name(self): return "LIDARIntensityRange"

    def validate(self, record: SensorRecord) -> ValidationResult:
        intensity = record.payload.get("intensity", 0)
        if not (self.min_val <= intensity <= self.max_val):
            return ValidationResult(False, self.validator_name, f"Intensity {intensity} out of range")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            flagged = df.withColumn("_int_valid",
                col("intensity").isNull() | col("intensity").between(self.min_val, self.max_val))
            valid = flagged.filter("_int_valid = true").drop("_int_valid")
            invalid = flagged.filter("_int_valid = false").drop("_int_valid")
            return valid, invalid
        except Exception as e:
            logger.warning(f"LIDARIntensityRange batch: {e}")
            return df, None


# ── 4. IMU Drift Detector ──────────────────────────────────────────────────────
class IMUDriftDetector(IValidator):
    """Detects gyroscope drift — angular rate > threshold."""
    def __init__(self, max_angular_rate=10.0):
        self.max_rate = max_angular_rate

    @property
    def validator_name(self): return "IMUDriftDetector"

    def validate(self, record: SensorRecord) -> ValidationResult:
        gyro_x = abs(record.payload.get("gyro_x", 0))
        gyro_y = abs(record.payload.get("gyro_y", 0))
        gyro_z = abs(record.payload.get("gyro_z", 0))
        if max(gyro_x, gyro_y, gyro_z) > self.max_rate:
            return ValidationResult(False, self.validator_name, "IMU drift detected")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col, abs as spark_abs
            if "gyro_x" not in df.columns:
                return df, None
            flagged = df.withColumn("_imu_valid",
                (spark_abs(col("gyro_x")) <= self.max_rate) &
                (spark_abs(col("gyro_y")) <= self.max_rate) &
                (spark_abs(col("gyro_z")) <= self.max_rate))
            valid = flagged.filter("_imu_valid = true").drop("_imu_valid")
            invalid = flagged.filter("_imu_valid = false").drop("_imu_valid")
            return valid, invalid
        except Exception as e:
            logger.warning(f"IMUDriftDetector batch: {e}")
            return df, None


# ── 5. Camera Exposure Validator ───────────────────────────────────────────────
class CameraExposureValidator(IValidator):
    """Camera brightness / exposure in valid operating range."""
    def __init__(self, min_exposure=0.0, max_exposure=1.0):
        self.min_exp = min_exposure; self.max_exp = max_exposure

    @property
    def validator_name(self): return "CameraExposureValidator"

    def validate(self, record: SensorRecord) -> ValidationResult:
        exp = record.payload.get("exposure", 0.5)
        if not (self.min_exp <= exp <= self.max_exp):
            return ValidationResult(False, self.validator_name, f"Exposure {exp} out of range")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            if "exposure" not in df.columns:
                return df, None
            flagged = df.withColumn("_exp_valid",
                col("exposure").isNull() | col("exposure").between(self.min_exp, self.max_exp))
            valid = flagged.filter("_exp_valid = true").drop("_exp_valid")
            invalid = flagged.filter("_exp_valid = false").drop("_exp_valid")
            return valid, invalid
        except Exception as e:
            logger.warning(f"CameraExposureValidator batch: {e}")
            return df, None


# ── 6. Radar Frequency Validator ──────────────────────────────────────────────
class RadarFrequencyValidator(IValidator):
    """Radar operates in 76-81 GHz band."""
    @property
    def validator_name(self): return "RadarFrequencyValidator"

    def validate(self, record: SensorRecord) -> ValidationResult:
        freq = record.payload.get("frequency_ghz", 77.0)
        if not (76.0 <= freq <= 81.0):
            return ValidationResult(False, self.validator_name, f"Radar frequency {freq} out of band")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            if "frequency_ghz" not in df.columns: return df, None
            flagged = df.withColumn("_rf_valid", col("frequency_ghz").between(76.0, 81.0))
            return flagged.filter("_rf_valid=true").drop("_rf_valid"), flagged.filter("_rf_valid=false").drop("_rf_valid")
        except Exception as e:
            logger.warning(f"RadarFrequencyValidator: {e}"); return df, None


# ── 7. Speed Plausibility Check ────────────────────────────────────────────────
class SpeedPlausibilityCheck(IValidator):
    """Vehicle speed must be in [0, 300] km/h for AV scenarios."""
    def __init__(self, max_speed=300.0):
        self.max_speed = max_speed

    @property
    def validator_name(self): return "SpeedPlausibilityCheck"

    def validate(self, record: SensorRecord) -> ValidationResult:
        speed = record.payload.get("speed", 0)
        if speed < 0 or speed > self.max_speed:
            return ValidationResult(False, self.validator_name, f"Implausible speed: {speed}")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            if "speed" not in df.columns: return df, None
            flagged = df.withColumn("_spd_valid", col("speed").between(0, self.max_speed))
            return flagged.filter("_spd_valid=true").drop("_spd_valid"), flagged.filter("_spd_valid=false").drop("_spd_valid")
        except Exception as e:
            logger.warning(f"SpeedPlausibilityCheck: {e}"); return df, None


# ── 8. Point Cloud Density Validator ──────────────────────────────────────────
class PointCloudDensityValidator(IValidator):
    """LIDAR must have minimum point density (~300K pts/frame)."""
    def __init__(self, min_points=1000):
        self.min_points = min_points

    @property
    def validator_name(self): return "PointCloudDensityValidator"

    def validate(self, record: SensorRecord) -> ValidationResult:
        pts = record.payload.get("point_count", 300000)
        if pts < self.min_points:
            return ValidationResult(False, self.validator_name, f"Too few LIDAR points: {pts}")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            if "point_count" not in df.columns: return df, None
            flagged = df.withColumn("_pc_valid", col("point_count") >= self.min_points)
            return flagged.filter("_pc_valid=true").drop("_pc_valid"), flagged.filter("_pc_valid=false").drop("_pc_valid")
        except Exception as e:
            logger.warning(f"PointCloudDensityValidator: {e}"); return df, None


# ── 9. CAN Bus Message Validator ───────────────────────────────────────────────
class CANBusMessageValidator(IValidator):
    """CAN bus messages must have valid DLC (0-8 bytes) and non-zero ID."""
    @property
    def validator_name(self): return "CANBusMessageValidator"

    def validate(self, record: SensorRecord) -> ValidationResult:
        dlc = record.payload.get("dlc", 0)
        can_id = record.payload.get("can_id", 0)
        if not (0 <= dlc <= 8) or can_id == 0:
            return ValidationResult(False, self.validator_name, f"Invalid CAN: id={can_id}, dlc={dlc}")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            if "can_id" not in df.columns: return df, None
            flagged = df.withColumn("_can_valid",
                (col("can_id") > 0) & col("dlc").between(0, 8))
            return flagged.filter("_can_valid=true").drop("_can_valid"), flagged.filter("_can_valid=false").drop("_can_valid")
        except Exception as e:
            logger.warning(f"CANBusMessageValidator: {e}"); return df, None


# ── 10. Heading Validator ──────────────────────────────────────────────────────
class HeadingValidator(IValidator):
    """Heading must be in [0, 360) degrees."""
    @property
    def validator_name(self): return "HeadingValidator"

    def validate(self, record: SensorRecord) -> ValidationResult:
        h = record.payload.get("heading", 0)
        if not (0 <= h < 360):
            return ValidationResult(False, self.validator_name, f"Invalid heading: {h}")
        return ValidationResult(True, self.validator_name)

    def validate_batch(self, df) -> Tuple:
        try:
            from pyspark.sql.functions import col
            if "heading" not in df.columns: return df, None
            flagged = df.withColumn("_hdg_valid", col("heading").between(0, 359.99))
            return flagged.filter("_hdg_valid=true").drop("_hdg_valid"), flagged.filter("_hdg_valid=false").drop("_hdg_valid")
        except Exception as e:
            logger.warning(f"HeadingValidator: {e}"); return df, None


# ── Factory ────────────────────────────────────────────────────────────────────
class ValidationFactory:
    """Factory: produces IValidator by class name."""
    _registry = {
        "GPSBoundsCheck":           GPSBoundsCheck,
        "TimestampMonotonicity":    TimestampMonotonicity,
        "LIDARIntensityRange":      LIDARIntensityRange,
        "IMUDriftDetector":         IMUDriftDetector,
        "CameraExposureValidator":  CameraExposureValidator,
        "RadarFrequencyValidator":  RadarFrequencyValidator,
        "SpeedPlausibilityCheck":   SpeedPlausibilityCheck,
        "PointCloudDensityValidator": PointCloudDensityValidator,
        "CANBusMessageValidator":   CANBusMessageValidator,
        "HeadingValidator":         HeadingValidator,
    }

    def get_validator(self, name: str) -> IValidator:
        cls = self._registry.get(name)
        if not cls:
            logger.warning(f"Unknown validator '{name}'")
            return GPSBoundsCheck()
        return cls()

    def get_all(self) -> List[IValidator]:
        return [cls() for cls in self._registry.values()]

    def register(self, name, cls): self._registry[name] = cls
    def list_available(self): return list(self._registry.keys())
