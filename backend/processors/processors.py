"""
processors/processors.py
Stage 4: PROCESSORS — ProcessorFactory with 12+ IProcessor implementations.
Diagram: PointCloudStitcher, FrameAligner, SensorFusion, AnomalyDetector,
         TrajectoryInterpolator, OccupancyGridBuilder + more.
"""
from __future__ import annotations
import math
import json
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Type
from datetime import datetime
from loguru import logger


# ─────────────────────────────────────────────────────────────────────────────
# IProcessor Interface (Strategy Pattern)
# ─────────────────────────────────────────────────────────────────────────────

class ProcessingResult:
    def __init__(self, success: bool, data: Any = None, error: str = ""):
        self.success = success
        self.data = data
        self.error = error

    def __bool__(self):
        return self.success


class IProcessor(ABC):
    """Strategy interface for all processors."""

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    def sensor_types(self) -> List[str]:
        return []

    @abstractmethod
    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        ...

    def applies_to(self, sensor_type: str) -> bool:
        return not self.sensor_types or sensor_type in self.sensor_types


# ─────────────────────────────────────────────────────────────────────────────
# 1. PointCloudStitcher — merge partial LIDAR sweeps
# ─────────────────────────────────────────────────────────────────────────────

class PointCloudStitcher(IProcessor):
    """
    Merge partial LIDAR sweeps across multiple sensors into a full 360° scan.
    Diagram: PointCloudStitcher → merge partial LIDAR sweeps
    """
    name = "PointCloudStitcher"
    sensor_types = ["lidar"]

    def __init__(self, num_sensors: int = 4, expected_coverage_deg: float = 360.0):
        self.num_sensors = num_sensors
        self.expected_coverage = expected_coverage_deg
        self._sweep_buffer: Dict[str, List[Dict]] = {}

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        vehicle_id = record.get("vehicle_id")
        timestamp = record.get("timestamp")
        points = record.get("points", [])

        sweep_key = f"{vehicle_id}:{int(float(timestamp) * 10)}"  # 100ms window

        if sweep_key not in self._sweep_buffer:
            self._sweep_buffer[sweep_key] = []

        self._sweep_buffer[sweep_key].extend(points)

        # Check if we have enough points for a complete sweep
        merged_count = len(self._sweep_buffer[sweep_key])

        # Clean old sweeps
        old_keys = [k for k in self._sweep_buffer if k != sweep_key]
        for k in old_keys[:max(0, len(old_keys) - 10)]:
            del self._sweep_buffer[k]

        stitched = {
            **record,
            "points": self._sweep_buffer.get(sweep_key, points),
            "point_count": merged_count,
            "stitched": True,
            "coverage_deg": min(360.0, merged_count / 1000 * 1.2),
        }

        return ProcessingResult(True, stitched)


# ─────────────────────────────────────────────────────────────────────────────
# 2. FrameAligner — temporal alignment across sensors
# ─────────────────────────────────────────────────────────────────────────────

class FrameAligner(IProcessor):
    """
    Temporally align data across LIDAR, camera, and radar sensors.
    Diagram: FrameAligner → temporal alignment across sensors
    """
    name = "FrameAligner"
    sensor_types = ["lidar", "camera", "radar"]

    def __init__(self, alignment_window_ms: float = 50.0):
        self.window_ms = alignment_window_ms
        self._frame_buffer: Dict[str, List[Dict]] = {}

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        ts = float(record.get("timestamp", 0))
        vehicle_id = record.get("vehicle_id")
        sensor_type = record.get("sensor_type")

        # Find nearest context frames within alignment window
        aligned_frame_ids = context.get("nearby_frames", {})
        alignment_offset_ms = 0.0

        # If we have GPS/IMU context, use it for interpolation
        gps_context = context.get("latest_gps", {})
        if gps_context:
            gps_ts = float(gps_context.get("timestamp", ts))
            alignment_offset_ms = (ts - gps_ts) * 1000

        result = {
            **record,
            "aligned": True,
            "alignment_offset_ms": alignment_offset_ms,
            "aligned_frame_ids": aligned_frame_ids,
            "alignment_timestamp": ts,
        }

        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 3. SensorFusion — combine LIDAR + camera + radar
# ─────────────────────────────────────────────────────────────────────────────

class SensorFusion(IProcessor):
    """
    Fuse LIDAR point clouds with camera detections and radar objects.
    Diagram: SensorFusion → combine LIDAR + camera + radar
    WHERE valid_gps = TRUE AND intensity > 0
    """
    name = "SensorFusion"
    sensor_types = ["lidar", "camera", "radar"]

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        sensor_type = record.get("sensor_type")

        # Only fuse if we have valid GPS
        if not context.get("gps_valid", True):
            return ProcessingResult(False, record, "GPS invalid — skipping fusion")

        fused_objects = []

        if sensor_type == "lidar":
            # Match LIDAR clusters with radar objects
            radar_objects = context.get("radar_objects", [])
            for lidar_cluster in record.get("point_count", []):
                matched_radar = next(
                    (r for r in radar_objects if abs(r.get("range_m", 999) - 10) < 5), None
                )
                fused_objects.append({
                    "source": "lidar",
                    "radar_match": matched_radar is not None,
                    "confidence": 0.9 if matched_radar else 0.6,
                })

        elif sensor_type == "camera":
            # Enrich camera detections with depth from LIDAR
            lidar_context = context.get("latest_lidar", {})
            for detection in record.get("detections", []):
                detection["depth_m"] = lidar_context.get("range_max", 50.0) * detection.get("confidence", 0.5)
                fused_objects.append(detection)

        result = {
            **record,
            "fused": True,
            "fused_objects": fused_objects,
            "fusion_timestamp": datetime.utcnow().isoformat(),
        }

        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 4. AnomalyDetector — outlier point removal
# ─────────────────────────────────────────────────────────────────────────────

class AnomalyDetector(IProcessor):
    """
    Detect and remove outlier points in LIDAR and sensor streams.
    Diagram: AnomalyDetector → outlier point removal
    """
    name = "AnomalyDetector"

    def __init__(self, z_score_threshold: float = 3.0):
        self.z_threshold = z_score_threshold

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        sensor_type = record.get("sensor_type")
        anomalies = []

        if sensor_type == "lidar":
            points = record.get("points", [])
            if points:
                intensities = [p.get("intensity", 0) for p in points]
                mean_i = sum(intensities) / len(intensities)
                std_i = math.sqrt(sum((x - mean_i)**2 for x in intensities) / len(intensities))

                filtered_points = []
                for p in points:
                    z = abs(p.get("intensity", 0) - mean_i) / max(std_i, 1e-6)
                    if z <= self.z_threshold:
                        filtered_points.append(p)
                    else:
                        anomalies.append({"type": "intensity_outlier", "value": p.get("intensity")})

                record = {**record, "points": filtered_points, "point_count": len(filtered_points)}

        elif sensor_type == "gps_imu":
            speed = record.get("speed_mps", 0)
            if speed > 55:  # ~200 km/h — unrealistic
                anomalies.append({"type": "speed_outlier", "value": speed})

        result = {
            **record,
            "anomaly_check": True,
            "anomalies_detected": len(anomalies),
            "anomalies": anomalies[:10],  # cap at 10 for storage
        }

        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 5. TrajectoryInterpolator — GPS gap filling
# ─────────────────────────────────────────────────────────────────────────────

class TrajectoryInterpolator(IProcessor):
    """
    Fill GPS/trajectory gaps using dead reckoning from IMU.
    Diagram: TrajectoryInterpolator → GPS gap filling
    """
    name = "TrajectoryInterpolator"
    sensor_types = ["gps_imu"]

    def __init__(self, max_gap_s: float = 1.0):
        self.max_gap_s = max_gap_s
        self._last_record: Optional[Dict] = None

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        current_ts = float(record.get("timestamp", 0))
        interpolated = False
        interpolated_points = []

        if self._last_record:
            last_ts = float(self._last_record.get("timestamp", current_ts))
            gap = current_ts - last_ts

            if 0 < gap <= self.max_gap_s and gap > 0.05:
                # Interpolate intermediate positions
                steps = max(1, int(gap / 0.01))  # 10ms steps
                lat1 = self._last_record.get("latitude", 0)
                lon1 = self._last_record.get("longitude", 0)
                lat2 = record.get("latitude", lat1)
                lon2 = record.get("longitude", lon1)

                for i in range(1, min(steps, 10)):
                    t = i / steps
                    interpolated_points.append({
                        "timestamp": last_ts + gap * t,
                        "latitude": lat1 + (lat2 - lat1) * t,
                        "longitude": lon1 + (lon2 - lon1) * t,
                        "interpolated": True,
                    })
                interpolated = True

        self._last_record = record
        result = {
            **record,
            "trajectory_interpolated": interpolated,
            "interpolated_points": interpolated_points,
        }

        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 6. OccupancyGridBuilder — 2D/3D grid maps
# ─────────────────────────────────────────────────────────────────────────────

class OccupancyGridBuilder(IProcessor):
    """
    Build 2D/3D occupancy grids from LIDAR point clouds.
    Diagram: OccupancyGridBuilder → 2D/3D grid maps
    """
    name = "OccupancyGridBuilder"
    sensor_types = ["lidar"]

    def __init__(self, resolution_m: float = 0.1, grid_size_m: float = 100.0):
        self.resolution = resolution_m
        self.grid_size = grid_size_m
        self.cells = int(grid_size_m / resolution_m)

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        points = record.get("points", [])

        # Build simplified 2D occupancy grid
        grid = {}
        for pt in points[:10000]:  # Cap at 10K points for processing
            gx = int(pt.get("x", 0) / self.resolution + self.cells // 2)
            gy = int(pt.get("y", 0) / self.resolution + self.cells // 2)
            if 0 <= gx < self.cells and 0 <= gy < self.cells:
                key = f"{gx},{gy}"
                grid[key] = grid.get(key, 0) + 1

        occupied_cells = len(grid)
        max_height = max((pt.get("z", 0) for pt in points[:1000]), default=0)

        result = {
            **record,
            "occupancy_grid": {
                "resolution_m": self.resolution,
                "grid_size_m": self.grid_size,
                "occupied_cells": occupied_cells,
                "max_height_m": max_height,
                "grid_data": json.dumps(grid) if len(grid) < 1000 else "TRUNCATED",
            },
        }

        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 7. VehicleStateEstimator — combine IMU + GPS into state
# ─────────────────────────────────────────────────────────────────────────────

class VehicleStateEstimator(IProcessor):
    """
    Extended Kalman Filter-based vehicle state estimation.
    Combines GPS position + IMU acceleration/rotation.
    """
    name = "VehicleStateEstimator"
    sensor_types = ["gps_imu"]

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        # Simplified EKF state: [x, y, heading, speed]
        lat = record.get("latitude", 0)
        lon = record.get("longitude", 0)
        heading = record.get("heading_deg", 0)
        speed = record.get("speed_mps", 0)
        gyro_z = record.get("gyro_z", 0)
        accel_x = record.get("accel_x", 0)

        # Convert to local Cartesian (simplified)
        state = {
            "x_m": lon * 111320 * math.cos(math.radians(lat)),
            "y_m": lat * 110540,
            "heading_rad": math.radians(heading),
            "speed_mps": speed,
            "yaw_rate_rad_s": gyro_z,
            "longitudinal_accel_m_s2": accel_x,
        }

        result = {**record, "vehicle_state": state, "state_timestamp": record.get("timestamp")}
        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 8. ObjectTracker — maintain tracks across frames
# ─────────────────────────────────────────────────────────────────────────────

class ObjectTracker(IProcessor):
    """
    Multi-object tracking using SORT-like algorithm.
    Maintains persistent object IDs across frames.
    """
    name = "ObjectTracker"
    sensor_types = ["camera", "radar"]

    def __init__(self, max_missed_frames: int = 5):
        self.max_missed = max_missed_frames
        self._tracks: Dict[int, Dict] = {}
        self._next_id = 0

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        detections = record.get("detections", []) or record.get("detected_objects", [])
        updated_tracks = []

        for det in detections:
            track_id = det.get("track_id")
            if track_id is None:
                track_id = self._next_id
                self._next_id += 1

            self._tracks[track_id] = {
                "track_id": track_id,
                "last_seen": record.get("timestamp"),
                "missed_frames": 0,
                "detection": det,
            }
            updated_tracks.append({**det, "track_id": track_id})

        # Age out missing tracks
        for tid in list(self._tracks.keys()):
            if tid not in {t.get("track_id") for t in updated_tracks}:
                self._tracks[tid]["missed_frames"] += 1
                if self._tracks[tid]["missed_frames"] > self.max_missed:
                    del self._tracks[tid]

        result = {
            **record,
            "tracked_objects": updated_tracks,
            "active_tracks": len(self._tracks),
        }
        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 9. LaneDetector
# ─────────────────────────────────────────────────────────────────────────────

class LaneDetector(IProcessor):
    """
    Detect lane markings from camera frames using edge detection heuristics.
    """
    name = "LaneDetector"
    sensor_types = ["camera"]

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        # In production: calls CV model. Here: return structural result.
        result = {
            **record,
            "lanes": {
                "detected": True,
                "left_lane_confidence": random.uniform(0.7, 1.0),
                "right_lane_confidence": random.uniform(0.7, 1.0),
                "lane_width_m": random.uniform(3.2, 3.8),
                "lane_departure": False,
                "curvature_m": random.uniform(50, 2000),
            }
        }
        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 10. SpeedProfiler — analyze CAN Bus data
# ─────────────────────────────────────────────────────────────────────────────

class SpeedProfiler(IProcessor):
    """Compute speed profiles and detect aggressive driving from CAN Bus."""
    name = "SpeedProfiler"
    sensor_types = ["can_bus"]

    def __init__(self):
        self._speed_history: List[float] = []

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        speed = record.get("vehicle_speed_kmh", 0)
        self._speed_history.append(speed)

        # Keep rolling window of last 100 readings
        if len(self._speed_history) > 100:
            self._speed_history.pop(0)

        avg_speed = sum(self._speed_history) / len(self._speed_history)
        max_speed = max(self._speed_history)

        # Detect rapid acceleration/braking
        aggressive = False
        if len(self._speed_history) >= 2:
            delta = abs(self._speed_history[-1] - self._speed_history[-2])
            aggressive = delta > 10  # >10 km/h in one sample

        result = {
            **record,
            "speed_profile": {
                "current_kmh": speed,
                "avg_kmh": round(avg_speed, 2),
                "max_kmh": round(max_speed, 2),
                "aggressive_driving": aggressive,
            }
        }
        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 11. WeatherConditionEstimator
# ─────────────────────────────────────────────────────────────────────────────

class WeatherConditionEstimator(IProcessor):
    """
    Estimate weather conditions from LIDAR point density and camera brightness.
    """
    name = "WeatherConditionEstimator"
    sensor_types = ["lidar", "camera"]

    CONDITIONS = ["clear", "light_rain", "heavy_rain", "fog", "snow"]

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        sensor_type = record.get("sensor_type")
        condition = "clear"
        confidence = 0.9

        if sensor_type == "lidar":
            point_count = record.get("point_count", 100000)
            if point_count < 50000:
                condition = "fog"
                confidence = 0.7
            elif point_count < 80000:
                condition = "light_rain"
                confidence = 0.75

        result = {
            **record,
            "weather_estimate": {
                "condition": condition,
                "confidence": confidence,
                "sensor_degraded": condition != "clear",
            }
        }
        return ProcessingResult(True, result)


# ─────────────────────────────────────────────────────────────────────────────
# 12. DataEnricher — add metadata enrichments
# ─────────────────────────────────────────────────────────────────────────────

class DataEnricher(IProcessor):
    """
    Enrich records with map data, fleet metadata, and computed features.
    """
    name = "DataEnricher"

    def process(self, record: Dict[str, Any], context: Dict[str, Any]) -> ProcessingResult:
        enrichment = {
            "processing_timestamp": datetime.utcnow().isoformat(),
            "pipeline_version": "1.0.0",
            "enriched": True,
            "region": self._get_region(record),
            "road_type": context.get("road_type", "urban"),
            "fleet_segment": self._get_fleet_segment(record.get("vehicle_id", "")),
        }

        result = {**record, "enrichment": enrichment}
        return ProcessingResult(True, result)

    @staticmethod
    def _get_region(record: Dict) -> str:
        lat = record.get("latitude", 0)
        lon = record.get("longitude", 0)
        # Simplified region lookup
        if 37 < lat < 38 and -123 < lon < -121:
            return "san_francisco"
        return "unknown"

    @staticmethod
    def _get_fleet_segment(vehicle_id: str) -> str:
        num = int(vehicle_id.split("-")[-1]) if "-" in vehicle_id else 0
        if num <= 3:
            return "test"
        elif num <= 7:
            return "pilot"
        return "production"


# ─────────────────────────────────────────────────────────────────────────────
# Processor Factory
# ─────────────────────────────────────────────────────────────────────────────

class ProcessorFactory:
    """
    Factory for creating processor instances.
    Diagram: ProcessorFactory → 12+ IProcessor
    Custom: Class.forName(config.className) → reflection-based dynamic loading
    """

    _REGISTRY: Dict[str, Type[IProcessor]] = {
        "PointCloudStitcher": PointCloudStitcher,
        "FrameAligner": FrameAligner,
        "SensorFusion": SensorFusion,
        "AnomalyDetector": AnomalyDetector,
        "TrajectoryInterpolator": TrajectoryInterpolator,
        "OccupancyGridBuilder": OccupancyGridBuilder,
        "VehicleStateEstimator": VehicleStateEstimator,
        "ObjectTracker": ObjectTracker,
        "LaneDetector": LaneDetector,
        "SpeedProfiler": SpeedProfiler,
        "WeatherConditionEstimator": WeatherConditionEstimator,
        "DataEnricher": DataEnricher,
    }

    @classmethod
    def create(cls, name: str, **kwargs) -> IProcessor:
        if name not in cls._REGISTRY:
            raise ValueError(f"Unknown processor: '{name}'. Available: {list(cls._REGISTRY.keys())}")
        return cls._REGISTRY[name](**kwargs)

    @classmethod
    def create_all(cls) -> List[IProcessor]:
        return [klass() for klass in cls._REGISTRY.values()]

    @classmethod
    def create_for_sensor(cls, sensor_type: str) -> List[IProcessor]:
        all_procs = cls.create_all()
        return [p for p in all_procs if p.applies_to(sensor_type)]

    @classmethod
    def register(cls, name: str, processor_class: Type[IProcessor]):
        """Dynamic registration — mirrors Java Class.forName()."""
        cls._REGISTRY[name] = processor_class
        logger.info(f"Registered custom processor: {name}")

    @classmethod
    def list_processors(cls) -> List[str]:
        return list(cls._REGISTRY.keys())


# ─────────────────────────────────────────────────────────────────────────────
# Processing Engine
# ─────────────────────────────────────────────────────────────────────────────

class ProcessingEngine:
    """Runs all applicable processors in sequence on each record."""

    def __init__(self, processor_names: Optional[List[str]] = None):
        if processor_names:
            self._processors = [ProcessorFactory.create(n) for n in processor_names]
        else:
            self._processors = ProcessorFactory.create_all()

        self._context: Dict[str, Any] = {}

    def update_context(self, key: str, value: Any):
        self._context[key] = value

    def process(self, record: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
        """
        Process a single record through all applicable processors.
        Returns (success, processed_record, errors).
        """
        errors = []
        current = record
        sensor_type = record.get("sensor_type", "")

        for proc in self._processors:
            if not proc.applies_to(sensor_type):
                continue
            try:
                result = proc.process(current, self._context)
                if result.success:
                    current = result.data
                else:
                    errors.append(f"{proc.name}: {result.error}")
            except Exception as e:
                errors.append(f"{proc.name}: exception — {e}")
                logger.warning(f"Processor {proc.name} failed: {e}")

        return len(errors) == 0, current, errors

    def process_batch(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Returns (processed, failed)."""
        processed, failed = [], []
        for record in records:
            success, result, errors = self.process(record)
            if success:
                processed.append(result)
            else:
                result["processing_errors"] = errors
                failed.append(result)
        return processed, failed
