// scripts/init_mongo.js
// Runs at MongoDB container startup

db = db.getSiblingDB('av_pipeline');

db.createCollection('pipeline_recipes');
db.pipeline_recipes.createIndex({ name: 1 }, { unique: true });
db.pipeline_recipes.createIndex({ active: 1, mode: 1 });

db.pipeline_recipes.insertOne({
    name: "av_full_pipeline",
    version: "2.0",
    description: "Full AV sensor data pipeline — all 6 sensor types",
    sources: ["kafka_lidar", "kafka_camera", "kafka_gps", "s3_radar"],
    validators: [
        "GPSBoundsCheck", "TimestampMonotonicity", "LIDARIntensityRange",
        "IMUDriftDetector", "CameraExposureValidator", "RadarFrequencyValidator",
        "SpeedPlausibilityCheck", "PointCloudDensityValidator",
        "CANBusMessageValidator", "HeadingValidator"
    ],
    transforms: [
        { sql: "coordinate_transform", params: { geo_to_local: true } },
        { sql: "temporal_alignment",   params: { bucket_ms: 100 } },
        { sql: "filter_valid",         params: { intensity_gt_0: true } },
        { sql: "add_processing_timestamp", params: {} },
        { sql: "window_aggregate",     params: { window_rows: 10 } }
    ],
    processors: [
        "PointCloudStitcher", "FrameAligner", "SensorFusion",
        "AnomalyDetector", "TrajectoryInterpolator", "OccupancyGridBuilder",
        "VelocityEstimator", "ObjectDetectionEnricher", "LaneDetectionProcessor",
        "WeatherConditionClassifier", "HDMapMatcher", "PredictiveMotionModel"
    ],
    sinks: ["hudi_data_lake", "delta_lake", "api_publisher", "rabbitmq_sink"],
    mode: "batch",
    extra: { watermark_delay: "10 minutes" },
    active: true,
    created_at: new Date()
});

print("MongoDB seeded: av_full_pipeline recipe inserted");
