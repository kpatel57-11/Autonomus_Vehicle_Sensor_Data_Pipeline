"""
monitoring/metrics.py
Prometheus metrics for pipeline observability.
Grafana + Prometheus: Pipeline metrics, latency dashboards
"""
from __future__ import annotations
from typing import Dict, Any, Optional
from loguru import logger

try:
    from prometheus_client import Counter, Gauge, Histogram, Summary, start_http_server, REGISTRY
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    logger.warning("prometheus_client not installed — metrics will be logged only")


class PipelineMetrics:
    """
    Prometheus metrics for the pipeline.
    Diagram: Grafana + Prometheus → Pipeline metrics, latency dashboards
    """

    def __init__(self, run_id: str):
        self.run_id = run_id
        self._in_memory: Dict[str, Any] = {}

        if HAS_PROMETHEUS:
            self._init_prometheus()

    def _init_prometheus(self):
        try:
            self.records_processed = Counter(
                "av_pipeline_records_processed_total",
                "Total records processed",
                ["stage", "sensor_type"],
                registry=REGISTRY
            )
            self.records_failed = Counter(
                "av_pipeline_records_failed_total",
                "Total records failed",
                ["stage"],
                registry=REGISTRY
            )
            self.pipeline_latency = Histogram(
                "av_pipeline_stage_latency_seconds",
                "Pipeline stage latency",
                ["stage"],
                buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
                registry=REGISTRY
            )
            self.active_pipelines = Gauge(
                "av_pipeline_active_total",
                "Number of active pipeline runs",
                registry=REGISTRY
            )
            self.kafka_lag = Gauge(
                "av_kafka_consumer_lag",
                "Kafka consumer lag by topic",
                ["topic"],
                registry=REGISTRY
            )
            self.dlq_size = Gauge(
                "av_dlq_size",
                "Dead letter queue size",
                registry=REGISTRY
            )
        except Exception as e:
            logger.warning(f"Prometheus metrics init failed (likely duplicate): {e}")

    def record_stage(self, stage: str, records_out: int, records_failed: int):
        key = f"{stage}_out"
        self._in_memory[key] = self._in_memory.get(key, 0) + records_out
        key_f = f"{stage}_failed"
        self._in_memory[key_f] = self._in_memory.get(key_f, 0) + records_failed

        if HAS_PROMETHEUS:
            try:
                self.records_processed.labels(stage=stage, sensor_type="all").inc(records_out)
                if records_failed > 0:
                    self.records_failed.labels(stage=stage).inc(records_failed)
            except Exception:
                pass

    def inc_counter(self, name: str, labels: Optional[Dict[str, str]] = None):
        self._in_memory[name] = self._in_memory.get(name, 0) + 1

    def set_gauge(self, name: str, value: float):
        self._in_memory[name] = value

    def get_all(self) -> Dict[str, Any]:
        return self._in_memory.copy()

    @staticmethod
    def start_server(port: int = 8001):
        if HAS_PROMETHEUS:
            try:
                start_http_server(port)
                logger.info(f"Prometheus metrics server started on port {port}")
            except Exception as e:
                logger.warning(f"Could not start Prometheus server: {e}")
