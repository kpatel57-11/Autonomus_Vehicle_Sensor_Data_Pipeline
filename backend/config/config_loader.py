"""Config Loader — reads recipes from MongoDB, metadata from PostgreSQL."""
import json, logging
from typing import Any, Dict, List, Optional
logger = logging.getLogger(__name__)

DEFAULT_RECIPE = {
    "name": "av_full_pipeline", "version": "2.0",
    "sources": ["kafka_lidar", "kafka_camera", "kafka_gps", "s3_radar"],
    "validators": ["GPSBoundsCheck","TimestampMonotonicity","LIDARIntensityRange","IMUDriftDetector","CameraExposureValidator"],
    "transforms": [
        {"sql": "coordinate_transform", "params": {"geo_to_local": True}},
        {"sql": "temporal_alignment", "params": {"sync_sensors": True}},
        {"sql": "filter_valid", "params": {}},
    ],
    "processors": ["PointCloudStitcher","FrameAligner","SensorFusion","AnomalyDetector","TrajectoryInterpolator","OccupancyGridBuilder"],
    "sinks": ["hudi_data_lake","delta_lake","api_publisher","rabbitmq_sink"],
    "mode": "batch",
    "extra": {"watermark_delay": "10 minutes"},
}


class PipelineRecipe:
    def __init__(self, data: Dict[str, Any]):
        self.name = data["name"]; self.version = data.get("version","1.0")
        self.sources: List[str] = data.get("sources",[])
        self.validators: List[str] = data.get("validators",[])
        self.transforms: List[Dict] = data.get("transforms",[])
        self.processors: List[str] = data.get("processors",[])
        self.sinks: List[str] = data.get("sinks",[])
        self.mode: str = data.get("mode","batch")
        self.extra: Dict = data.get("extra",{})

    def to_dict(self):
        return {"name":self.name,"version":self.version,"sources":self.sources,
                "validators":self.validators,"transforms":self.transforms,
                "processors":self.processors,"sinks":self.sinks,"mode":self.mode,"extra":self.extra}


class ConfigLoader:
    def __init__(self, mongo_uri=None, postgres_dsn=None):
        self.mongo_uri = mongo_uri
        self.postgres_dsn = postgres_dsn

    def get_recipe(self, name="av_full_pipeline") -> PipelineRecipe:
        try:
            if self.mongo_uri:
                from pymongo import MongoClient
                client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)
                doc = client["av_pipeline"]["pipeline_recipes"].find_one({"name": name})
                if doc:
                    logger.info(f"Loaded recipe '{name}' from MongoDB")
                    return PipelineRecipe(doc)
        except Exception as e:
            logger.warning(f"MongoDB unavailable: {e}")
        return PipelineRecipe(DEFAULT_RECIPE)

    def get_dataset_metadata(self, dataset_name: str) -> Dict[str, Any]:
        defaults = {
            "kafka_lidar": {"path": "s3://av-sensor-data/lidar/", "format": "parquet"},
            "kafka_camera": {"path": "s3://av-sensor-data/camera/", "format": "parquet"},
            "kafka_gps": {"path": "s3://av-sensor-data/gps/", "format": "parquet"},
            "s3_radar": {"path": "s3://av-sensor-data/radar/", "format": "parquet"},
        }
        return defaults.get(dataset_name, {"path": f"s3://av-sensor-data/{dataset_name}/", "format": "parquet"})
