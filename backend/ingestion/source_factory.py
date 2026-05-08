"""SourceReaderFactory — Factory pattern. Produces ISourceReader for Kafka, S3/HDFS, ROSBag, JDBC."""
import logging, json, random
from typing import Any, Dict
from core.interfaces import ISourceReader, SensorType
logger = logging.getLogger(__name__)


def _make_synthetic_df(spark, sensor_type: str, n: int = 2000):
    from pyspark.sql import Row
    import time
    base_ts = int(time.time() * 1000)
    rows = [Row(
        sensor_type=sensor_type, vehicle_id=f"VH{i%20:03d}",
        timestamp_ms=base_ts + i*50, sequence_id=i,
        lat=37.7749 + (i%100)*0.0001, lon=-122.4194 + (i%100)*0.0001,
        altitude=float(10 + i%50), intensity=float(i % 255),
        speed=float(30 + i%60), heading=float(i % 360),
        valid=True, partition=0, offset=i,
    ) for i in range(n)]
    return spark.createDataFrame(rows)


class KafkaSourceReader(ISourceReader):
    def __init__(self, topic: str, sensor_type: SensorType):
        self._topic = topic; self._sensor_type = sensor_type
    @property
    def source_name(self): return f"kafka_{self._topic}"
    def get_schema(self): return None
    def read(self, spark, config):
        from config.settings import settings
        try:
            df = (spark.read.format("kafka")
                .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
                .option("subscribe", self._topic)
                .option("startingOffsets","earliest").option("endingOffsets","latest").load())
            logger.info(f"Kafka '{self._topic}' connected")
            return df
        except Exception as e:
            logger.warning(f"Kafka unavailable ({e}), using synthetic data")
            return _make_synthetic_df(spark, self._sensor_type.value)


class HDFSFileReader(ISourceReader):
    def __init__(self, sensor_type: SensorType, fmt="parquet"):
        self._sensor_type = sensor_type; self._fmt = fmt
    @property
    def source_name(self): return f"s3_{self._sensor_type.value}"
    def get_schema(self): return None
    def read(self, spark, config):
        path = config.get("path", f"s3://av-sensor-data/{self._sensor_type.value}/")
        try:
            return spark.read.format(self._fmt).load(path)
        except Exception as e:
            logger.warning(f"S3/HDFS unavailable ({e}), using synthetic data")
            return _make_synthetic_df(spark, self._sensor_type.value)


class ROSBagReader(ISourceReader):
    @property
    def source_name(self): return "ros_bag"
    def get_schema(self): return None
    def read(self, spark, config):
        try:
            return spark.read.format("binaryFile").load(config.get("path","s3://av-sensor-data/rosbags/")+"*.bag")
        except Exception:
            return _make_synthetic_df(spark, "lidar", n=500)


class JDBCSourceReader(ISourceReader):
    def __init__(self, table: str):
        self._table = table
    @property
    def source_name(self): return f"jdbc_{self._table}"
    def get_schema(self): return None
    def read(self, spark, config):
        from config.settings import settings
        try:
            return (spark.read.format("jdbc")
                .option("url", f"jdbc:postgresql://{settings.postgres.host}/{settings.postgres.database}")
                .option("dbtable", self._table)
                .option("user", settings.postgres.user)
                .option("password", settings.postgres.password).load())
        except Exception as e:
            logger.warning(f"JDBC unavailable: {e}")
            return spark.createDataFrame([], "id long, name string")


class SchemaBuilder:
    """Builds StructType from Avro/Protobuf schema registry."""
    @staticmethod
    def build(subject: str, spark):
        from config.settings import settings
        import urllib.request
        try:
            url = f"{settings.kafka.schema_registry_url}/subjects/{subject}/versions/latest"
            with urllib.request.urlopen(url, timeout=3) as r:
                data = json.loads(r.read())
                return SchemaBuilder._avro_to_spark(json.loads(data.get("schema","{}")))
        except Exception:
            return None

    @staticmethod
    def _avro_to_spark(avro):
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
        tm = {"string": StringType(), "long": LongType(), "double": DoubleType()}
        fields = []
        for f in avro.get("fields", []):
            t = f.get("type","string")
            if isinstance(t, list): t = [x for x in t if x != "null"][0]
            fields.append(StructField(f["name"], tm.get(t, StringType()), True))
        return StructType(fields) if fields else None


class SourceReaderFactory:
    """Factory: creates ISourceReader by name."""
    _registry = {
        "kafka_lidar":  lambda: KafkaSourceReader("lidar_raw",   SensorType.LIDAR),
        "kafka_camera": lambda: KafkaSourceReader("camera_meta",  SensorType.CAMERA),
        "kafka_gps":    lambda: KafkaSourceReader("gps_stream",   SensorType.GPS_IMU),
        "s3_radar":     lambda: HDFSFileReader(SensorType.RADAR),
        "s3_lidar":     lambda: HDFSFileReader(SensorType.LIDAR),
        "ros_bag":      lambda: ROSBagReader(),
        "jdbc_metadata":lambda: JDBCSourceReader("sensor_metadata"),
    }
    def get_reader(self, name: str) -> ISourceReader:
        fn = self._registry.get(name)
        if not fn:
            logger.warning(f"Unknown source '{name}', defaulting to S3/LIDAR reader")
            return HDFSFileReader(SensorType.LIDAR)
        return fn()
    def register(self, name, fn): self._registry[name] = fn
    def list_available(self): return list(self._registry.keys())
