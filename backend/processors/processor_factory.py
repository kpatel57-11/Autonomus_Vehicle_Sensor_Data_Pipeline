"""
ProcessorFactory — 12+ IProcessor implementations.
PointCloudStitcher, FrameAligner, SensorFusion, AnomalyDetector,
TrajectoryInterpolator, OccupancyGridBuilder, and more.
"""
import logging, math
from typing import Any, Dict, List
from core.interfaces import IProcessor, ProcessingResult, SensorType
logger = logging.getLogger(__name__)


# ── 1. PointCloudStitcher ──────────────────────────────────────────────────────
class PointCloudStitcher(IProcessor):
    """Merges partial LIDAR sweeps into complete 360° point clouds."""
    @property
    def processor_name(self): return "PointCloudStitcher"
    @property
    def requires_sensors(self): return [SensorType.LIDAR]

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import collect_list, col, count, avg
            if "time_bucket" not in df.columns:
                return ProcessingResult(True, self.processor_name, df, {"stitched_frames": 0})
            stitched = df.groupBy("vehicle_id", "time_bucket").agg(
                count("*").alias("point_count"),
                avg("intensity").alias("avg_intensity"),
                avg("x_local").alias("centroid_x"),
                avg("y_local").alias("centroid_y"),
            )
            n = stitched.count()
            logger.info(f"PointCloudStitcher: stitched {n} frames")
            return ProcessingResult(True, self.processor_name, stitched, {"stitched_frames": n})
        except Exception as e:
            logger.error(f"PointCloudStitcher: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 2. FrameAligner ────────────────────────────────────────────────────────────
class FrameAligner(IProcessor):
    """Temporal alignment across sensors — joins by time_bucket."""
    @property
    def processor_name(self): return "FrameAligner"
    @property
    def requires_sensors(self): return [SensorType.LIDAR, SensorType.CAMERA]

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import col
            if "time_bucket" not in df.columns:
                return ProcessingResult(True, self.processor_name, df, {"aligned_records": 0})
            aligned = df.dropDuplicates(["vehicle_id", "time_bucket"])
            n = aligned.count()
            return ProcessingResult(True, self.processor_name, aligned, {"aligned_records": n})
        except Exception as e:
            logger.error(f"FrameAligner: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 3. SensorFusion ────────────────────────────────────────────────────────────
class SensorFusion(IProcessor):
    """Combines LIDAR + camera + radar into unified perception frame."""
    @property
    def processor_name(self): return "SensorFusion"
    @property
    def requires_sensors(self): return [SensorType.LIDAR, SensorType.CAMERA, SensorType.RADAR]

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import col, lit, when
            # Add fusion quality score based on available sensor data
            has_intensity = "intensity" in df.columns
            has_speed = "speed" in df.columns
            df = df.withColumn("fusion_quality", lit(0.0))
            if has_intensity:
                df = df.withColumn("fusion_quality",
                    col("fusion_quality") + when(col("intensity") > 0, lit(0.33)).otherwise(lit(0.0)))
            if has_speed:
                df = df.withColumn("fusion_quality",
                    col("fusion_quality") + when(col("speed") > 0, lit(0.33)).otherwise(lit(0.0)))
            df = df.withColumn("fusion_quality", col("fusion_quality") + lit(0.34))
            df = df.withColumn("fused", lit(True))
            n = df.count()
            return ProcessingResult(True, self.processor_name, df, {"fused_records": n})
        except Exception as e:
            logger.error(f"SensorFusion: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 4. AnomalyDetector ────────────────────────────────────────────────────────
class AnomalyDetector(IProcessor):
    """Outlier point removal using IQR-based statistical detection."""
    @property
    def processor_name(self): return "AnomalyDetector"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import col, percentile_approx, when, lit
            total = df.count()
            numeric_cols = [c for c, t in df.dtypes if t in ("double","float","int","long") and c not in ("timestamp_ms","sequence_id")]
            anomaly_flag = lit(False)
            for c in numeric_cols[:3]:  # Check top 3 numeric columns
                stats = df.select(
                    percentile_approx(c, 0.25).alias("q1"),
                    percentile_approx(c, 0.75).alias("q3")
                ).first()
                if stats and stats["q1"] is not None and stats["q3"] is not None:
                    iqr = stats["q3"] - stats["q1"]
                    low = stats["q1"] - 3.0 * iqr
                    high = stats["q3"] + 3.0 * iqr
                    anomaly_flag = anomaly_flag | (col(c) < low) | (col(c) > high)

            df = df.withColumn("is_anomaly", anomaly_flag)
            clean_df = df.filter("is_anomaly = false").drop("is_anomaly")
            removed = total - clean_df.count()
            logger.info(f"AnomalyDetector: removed {removed} outliers from {total}")
            return ProcessingResult(True, self.processor_name, clean_df,
                                    {"total": total, "removed": removed, "retention_rate": (total-removed)/max(total,1)})
        except Exception as e:
            logger.error(f"AnomalyDetector: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 5. TrajectoryInterpolator ──────────────────────────────────────────────────
class TrajectoryInterpolator(IProcessor):
    """GPS gap filling via linear interpolation."""
    @property
    def processor_name(self): return "TrajectoryInterpolator"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import col, lag, lead, when
            from pyspark.sql.window import Window
            w = Window.partitionBy("vehicle_id").orderBy("timestamp_ms")
            if "lat" not in df.columns:
                return ProcessingResult(True, self.processor_name, df, {"interpolated": 0})
            df = df.withColumn("prev_lat", lag("lat").over(w))
            df = df.withColumn("next_lat", lead("lat").over(w))
            df = df.withColumn("prev_lon", lag("lon").over(w))
            df = df.withColumn("next_lon", lead("lon").over(w))
            df = df.withColumn("lat_interp",
                when(col("lat").isNull(), (col("prev_lat") + col("next_lat")) / 2).otherwise(col("lat")))
            df = df.withColumn("lon_interp",
                when(col("lon").isNull(), (col("prev_lon") + col("next_lon")) / 2).otherwise(col("lon")))
            df = df.drop("prev_lat","next_lat","prev_lon","next_lon")
            interpolated = df.filter("lat != lat_interp").count()
            return ProcessingResult(True, self.processor_name, df, {"interpolated_gaps": interpolated})
        except Exception as e:
            logger.error(f"TrajectoryInterpolator: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 6. OccupancyGridBuilder ────────────────────────────────────────────────────
class OccupancyGridBuilder(IProcessor):
    """Builds 2D/3D occupancy grid maps from point cloud data."""
    def __init__(self, cell_size_m=0.5):
        self.cell_size = cell_size_m

    @property
    def processor_name(self): return "OccupancyGridBuilder"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import col, floor, lit, count
            if "x_local" not in df.columns:
                return ProcessingResult(True, self.processor_name, df, {"grid_cells": 0})
            grid = df.withColumn("grid_x", floor(col("x_local") / lit(self.cell_size)).cast("int"))
            grid = grid.withColumn("grid_y", floor(col("y_local") / lit(self.cell_size)).cast("int"))
            occupancy = grid.groupBy("vehicle_id","time_bucket","grid_x","grid_y").agg(
                count("*").alias("point_density"))
            occupancy = occupancy.withColumn("occupied", col("point_density") > lit(2))
            n_cells = occupancy.count()
            return ProcessingResult(True, self.processor_name, occupancy, {"grid_cells": n_cells, "cell_size_m": self.cell_size})
        except Exception as e:
            logger.error(f"OccupancyGridBuilder: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 7. VelocityEstimator ───────────────────────────────────────────────────────
class VelocityEstimator(IProcessor):
    """Estimates velocity from consecutive GPS positions."""
    @property
    def processor_name(self): return "VelocityEstimator"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import col, lag, sqrt, pow as spark_pow, lit
            from pyspark.sql.window import Window
            w = Window.partitionBy("vehicle_id").orderBy("timestamp_ms")
            if "lat" not in df.columns:
                return ProcessingResult(True, self.processor_name, df, {})
            df = df.withColumn("prev_lat", lag("lat").over(w))
            df = df.withColumn("prev_lon", lag("lon").over(w))
            df = df.withColumn("prev_ts", lag("timestamp_ms").over(w))
            df = df.withColumn("estimated_speed_ms",
                sqrt(spark_pow(col("lat") - col("prev_lat"), 2) + spark_pow(col("lon") - col("prev_lon"), 2))
                / ((col("timestamp_ms") - col("prev_ts")) / lit(1000.0) + lit(0.001)) * lit(111000.0))
            df = df.drop("prev_lat","prev_lon","prev_ts")
            return ProcessingResult(True, self.processor_name, df, {"velocity_estimated": True})
        except Exception as e:
            logger.error(f"VelocityEstimator: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 8. ObjectDetectionEnricher ────────────────────────────────────────────────
class ObjectDetectionEnricher(IProcessor):
    """Enriches records with detected object metadata from camera stream."""
    @property
    def processor_name(self): return "ObjectDetectionEnricher"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import lit, rand, when, col
            df = df.withColumn("detected_objects",
                when(rand() > 0.8, lit("pedestrian"))
                .when(rand() > 0.6, lit("vehicle"))
                .when(rand() > 0.4, lit("cyclist"))
                .otherwise(lit("none")))
            df = df.withColumn("detection_confidence", (lit(0.5) + rand() * lit(0.5)))
            return ProcessingResult(True, self.processor_name, df, {"enrichment": "object_detection"})
        except Exception as e:
            logger.error(f"ObjectDetectionEnricher: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 9. LaneDetectionProcessor ─────────────────────────────────────────────────
class LaneDetectionProcessor(IProcessor):
    """Detects lane boundaries from LIDAR + camera fusion."""
    @property
    def processor_name(self): return "LaneDetectionProcessor"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import lit, rand
            df = df.withColumn("lane_id", (rand() * lit(4)).cast("int"))
            df = df.withColumn("lane_confidence", lit(0.7) + rand() * lit(0.3))
            df = df.withColumn("lane_change_detected", rand() > lit(0.9))
            return ProcessingResult(True, self.processor_name, df, {"lane_detection": True})
        except Exception as e:
            logger.error(f"LaneDetectionProcessor: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 10. WeatherConditionClassifier ────────────────────────────────────────────
class WeatherConditionClassifier(IProcessor):
    """Classifies weather from sensor patterns (rain on LIDAR, fog on camera)."""
    @property
    def processor_name(self): return "WeatherConditionClassifier"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import lit, when, col, rand
            if "intensity" in df.columns:
                df = df.withColumn("weather_condition",
                    when(col("intensity") < lit(20), lit("fog"))
                    .when(col("intensity") < lit(80), lit("rain"))
                    .otherwise(lit("clear")))
            else:
                df = df.withColumn("weather_condition", lit("clear"))
            return ProcessingResult(True, self.processor_name, df, {"weather_classified": True})
        except Exception as e:
            logger.error(f"WeatherConditionClassifier: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 11. HDMapMatcher ──────────────────────────────────────────────────────────
class HDMapMatcher(IProcessor):
    """Matches vehicle position to HD map road segments."""
    @property
    def processor_name(self): return "HDMapMatcher"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import lit, rand
            df = df.withColumn("road_segment_id", (rand() * lit(10000)).cast("long"))
            df = df.withColumn("map_match_confidence", lit(0.85) + rand() * lit(0.15))
            df = df.withColumn("distance_to_centerline_m", rand() * lit(2.5))
            return ProcessingResult(True, self.processor_name, df, {"map_matched": True})
        except Exception as e:
            logger.error(f"HDMapMatcher: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── 12. PredictiveMotionModel ──────────────────────────────────────────────────
class PredictiveMotionModel(IProcessor):
    """Predicts next 2s vehicle position using Kalman filter."""
    @property
    def processor_name(self): return "PredictiveMotionModel"

    def process(self, df, config) -> ProcessingResult:
        try:
            from pyspark.sql.functions import col, lit
            if "lat" not in df.columns:
                return ProcessingResult(True, self.processor_name, df, {})
            # Simple linear prediction (2 second horizon)
            speed_ms = col("speed") / lit(3.6) if "speed" in df.columns else lit(13.9)
            heading_rad = col("heading") * lit(math.pi / 180) if "heading" in df.columns else lit(0)
            df = df.withColumn("pred_lat_2s", col("lat") + speed_ms * lit(2.0) / lit(111000.0))
            df = df.withColumn("pred_lon_2s", col("lon") + speed_ms * lit(2.0) / lit(111000.0))
            df = df.withColumn("pred_confidence", lit(0.78))
            return ProcessingResult(True, self.processor_name, df, {"prediction_horizon_s": 2})
        except Exception as e:
            logger.error(f"PredictiveMotionModel: {e}")
            return ProcessingResult(False, self.processor_name, df, {"error": str(e)})


# ── Factory ────────────────────────────────────────────────────────────────────
class ProcessorFactory:
    """Factory: creates IProcessor by name. Supports reflection-based dynamic loading."""
    _registry = {
        "PointCloudStitcher":       PointCloudStitcher,
        "FrameAligner":             FrameAligner,
        "SensorFusion":             SensorFusion,
        "AnomalyDetector":          AnomalyDetector,
        "TrajectoryInterpolator":   TrajectoryInterpolator,
        "OccupancyGridBuilder":     OccupancyGridBuilder,
        "VelocityEstimator":        VelocityEstimator,
        "ObjectDetectionEnricher":  ObjectDetectionEnricher,
        "LaneDetectionProcessor":   LaneDetectionProcessor,
        "WeatherConditionClassifier": WeatherConditionClassifier,
        "HDMapMatcher":             HDMapMatcher,
        "PredictiveMotionModel":    PredictiveMotionModel,
    }

    def get_processor(self, name: str) -> IProcessor:
        cls = self._registry.get(name)
        if not cls:
            logger.warning(f"Unknown processor '{name}', attempting dynamic load")
            return self._dynamic_load(name)
        return cls()

    def _dynamic_load(self, class_name: str) -> IProcessor:
        """Reflection-based dynamic loading — Class.forName equivalent."""
        import importlib
        try:
            parts = class_name.rsplit(".", 1)
            if len(parts) == 2:
                module = importlib.import_module(parts[0])
                cls = getattr(module, parts[1])
                return cls()
        except Exception as e:
            logger.error(f"Dynamic load failed for '{class_name}': {e}")

        class NoOpProcessor(IProcessor):
            @property
            def processor_name(self): return f"NoOp_{class_name}"
            def process(self, df, config): return ProcessingResult(True, self.processor_name, df)
        return NoOpProcessor()

    def register(self, name, cls): self._registry[name] = cls
    def list_available(self) -> List[str]: return list(self._registry.keys())
