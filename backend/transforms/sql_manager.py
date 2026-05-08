"""
SqlManager — Spark SQL execution engine.
Handles coordinate transforms, temporal alignment, filtering, and window functions.
"""
import logging
from typing import Any, Dict
logger = logging.getLogger(__name__)


class SqlManager:
    """
    Spark SQL execution engine for sensor data transforms.
    Supports both batch and streaming contexts.
    """

    def __init__(self, spark):
        self.spark = spark
        self._view_registry: Dict[str, str] = {}

    def create_or_replace_temp_view(self, name: str, df) -> None:
        """Register DataFrame as temp SQL view."""
        df.createOrReplaceTempView(name)
        self._view_registry[name] = name
        logger.debug(f"Registered temp view: {name}")

    def apply(self, df, transform_name: str, params: Dict[str, Any]):
        """Apply a named SQL transform to a DataFrame."""
        handler = getattr(self, f"_transform_{transform_name}", None)
        if handler is None:
            logger.warning(f"Unknown transform '{transform_name}', skipping")
            return df
        try:
            result = handler(df, params)
            logger.info(f"Applied transform '{transform_name}'")
            return result
        except Exception as e:
            logger.error(f"Transform '{transform_name}' failed: {e}", exc_info=True)
            return df

    def apply_streaming(self, df, transform_name: str, params: Dict[str, Any]):
        """Apply streaming-safe transforms (no aggregations requiring complete mode)."""
        streaming_safe = {"filter_valid", "add_processing_timestamp", "coordinate_transform"}
        if transform_name not in streaming_safe:
            logger.info(f"Skipping non-streaming transform '{transform_name}'")
            return df
        return self.apply(df, transform_name, params)

    def execute_sql(self, sql: str):
        """Execute raw Spark SQL and return DataFrame."""
        logger.debug(f"Executing SQL: {sql[:120]}...")
        return self.spark.sql(sql)

    # ── Transform Implementations ────────────────────────────────────────────

    def _transform_coordinate_transform(self, df, params: Dict):
        """Geo → local coordinate transform. Adds x_local, y_local columns."""
        try:
            from pyspark.sql.functions import col, lit, radians, cos, sin, expr
            if "lat" not in df.columns or "lon" not in df.columns:
                return df
            # Simple Mercator projection
            origin_lat = params.get("origin_lat", 37.7749)
            origin_lon = params.get("origin_lon", -122.4194)
            R = 6371000.0  # Earth radius in meters
            df = df.withColumn("x_local",
                lit(R) * radians(col("lon") - lit(origin_lon)) * cos(radians(lit(origin_lat))))
            df = df.withColumn("y_local",
                lit(R) * radians(col("lat") - lit(origin_lat)))
            return df
        except Exception as e:
            logger.warning(f"coordinate_transform: {e}")
            return df

    def _transform_temporal_alignment(self, df, params: Dict):
        """Temporal alignment — sync multi-sensor timestamps to nearest 100ms bucket."""
        try:
            from pyspark.sql.functions import col, floor, lit
            if "timestamp_ms" not in df.columns:
                return df
            bucket_ms = params.get("bucket_ms", 100)
            df = df.withColumn("time_bucket",
                (floor(col("timestamp_ms") / lit(bucket_ms)) * lit(bucket_ms)).cast("long"))
            return df
        except Exception as e:
            logger.warning(f"temporal_alignment: {e}")
            return df

    def _transform_filter_valid(self, df, params: Dict):
        """WHERE valid_gps = true AND intensity > 0."""
        try:
            filters = []
            if "lat" in df.columns and "lon" in df.columns:
                filters.append("lat IS NOT NULL AND lon IS NOT NULL")
            if "intensity" in df.columns and params.get("intensity_gt_0", False):
                filters.append("intensity > 0")
            if "timestamp_ms" in df.columns:
                filters.append("timestamp_ms > 0")
            if filters:
                condition = " AND ".join(filters)
                df = df.filter(condition)
            return df
        except Exception as e:
            logger.warning(f"filter_valid: {e}")
            return df

    def _transform_add_processing_timestamp(self, df, params: Dict):
        """Add processing_ts column (current timestamp)."""
        try:
            from pyspark.sql.functions import current_timestamp
            return df.withColumn("processing_ts", current_timestamp())
        except Exception as e:
            logger.warning(f"add_processing_timestamp: {e}")
            return df

    def _transform_window_aggregate(self, df, params: Dict):
        """Window functions for time-series analysis (batch only)."""
        try:
            from pyspark.sql.functions import avg, stddev, col, lag
            from pyspark.sql.window import Window
            w = (Window.partitionBy("vehicle_id")
                .orderBy("timestamp_ms")
                .rowsBetween(-params.get("window_rows", 10), 0))
            if "speed" in df.columns:
                df = df.withColumn("speed_avg_window", avg("speed").over(w))
                df = df.withColumn("speed_stddev_window", stddev("speed").over(w))
            if "lat" in df.columns:
                df = df.withColumn("prev_lat", lag("lat").over(Window.partitionBy("vehicle_id").orderBy("timestamp_ms")))
                df = df.withColumn("prev_lon", lag("lon").over(Window.partitionBy("vehicle_id").orderBy("timestamp_ms")))
            return df
        except Exception as e:
            logger.warning(f"window_aggregate: {e}")
            return df

    def _generate_sql_coordinate_transform(self, df, params: Dict):
        """SQL version: SELECT *, lat_expr AS x_local, lon_expr AS y_local FROM raw_data"""
        self.create_or_replace_temp_view("raw_data", df)
        sql = """
            SELECT *,
                6371000.0 * RADIANS(lon - (-122.4194)) * COS(RADIANS(37.7749)) AS x_local,
                6371000.0 * RADIANS(lat - 37.7749) AS y_local,
                FLOOR(timestamp_ms / 100) * 100 AS time_bucket
            FROM raw_data
            WHERE lat IS NOT NULL AND lon IS NOT NULL
        """
        return self.execute_sql(sql)
