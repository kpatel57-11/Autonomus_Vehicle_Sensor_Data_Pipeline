"""
QueryLifecycleMonitor — Observer pattern.
Implements Spark StreamingQueryListener: onQueryStarted, onQueryProgress, onQueryTerminated.
Self-healing: watchdog auto-restart, PageDuty/Stack alerts, DLQ for bad data.
"""
import logging, time, threading
from typing import Any, Callable, Dict, List, Optional
logger = logging.getLogger(__name__)


class QueryLifecycleMonitor:
    """
    Observer pattern implementation for Spark Structured Streaming.
    Monitors query lifecycle and triggers self-healing actions.

    Hooks:
      onQueryStarted   → log + metrics
      onQueryProgress  → metrics + alert if lag > threshold
      onQueryTerminated → alert + auto-restart
    """

    def __init__(self,
                 prometheus_gateway: Optional[str] = None,
                 restart_callback: Optional[Callable] = None,
                 alert_callback: Optional[Callable] = None,
                 max_restart_attempts: int = 30):
        self.prometheus_gateway = prometheus_gateway
        self._restart_callback = restart_callback
        self._alert_callback = alert_callback
        self.max_restart_attempts = max_restart_attempts
        self._restart_count = 0
        self._active_queries: Dict[str, Dict] = {}
        self._metrics_history: List[Dict] = []

    # ── Spark StreamingQueryListener hooks ──────────────────────────────────

    def onQueryStarted(self, event) -> None:
        """Called when a streaming query starts."""
        query_id = str(getattr(event, 'id', 'unknown'))
        run_id = str(getattr(event, 'runId', 'unknown'))
        name = getattr(event, 'name', 'unnamed')
        self._active_queries[query_id] = {
            "id": query_id, "run_id": run_id,
            "name": name, "started_at": time.time(), "status": "running",
        }
        logger.info(f"[Monitor] Query STARTED: id={query_id}, name={name}, runId={run_id}")
        self._push_metric("query_started_total", 1, {"query_name": name})

    def onQueryProgress(self, event) -> None:
        """Called on each micro-batch completion."""
        try:
            progress = event.progress if hasattr(event, 'progress') else event
            query_id = str(getattr(progress, 'id', 'unknown'))
            batch_id = getattr(progress, 'batchId', 0)
            input_rows = getattr(progress, 'numInputRows', 0)
            proc_rate = getattr(progress, 'processedRowsPerSecond', 0.0)
            trigger_ms = getattr(progress, 'triggerExecution', {})
            if hasattr(trigger_ms, 'get'):
                batch_ms = trigger_ms.get('batchDuration', 0)
            else:
                batch_ms = 0

            metrics = {
                "query_id": query_id, "batch_id": batch_id,
                "input_rows": input_rows, "rows_per_second": proc_rate,
                "batch_duration_ms": batch_ms, "timestamp": time.time(),
            }
            self._metrics_history.append(metrics)
            if len(self._metrics_history) > 1000:
                self._metrics_history = self._metrics_history[-500:]

            logger.info(f"[Monitor] Progress: batch={batch_id}, rows={input_rows}, rate={proc_rate:.1f}r/s")
            self._push_metric("query_input_rows_total", input_rows, {"query_id": query_id})
            self._push_metric("query_processing_rate", proc_rate, {"query_id": query_id})

            # Alert on high latency
            if batch_ms > 60000:
                self._send_alert(f"HIGH LATENCY: batch {batch_id} took {batch_ms}ms", severity="warning")

            # Alert on no data (possible upstream failure)
            if input_rows == 0 and batch_id > 5:
                self._send_alert(f"NO DATA in batch {batch_id} — possible Kafka lag", severity="warning")
        except Exception as e:
            logger.warning(f"[Monitor] onQueryProgress error: {e}")

    def onQueryTerminated(self, event) -> None:
        """Called when query stops — triggers alert + auto-restart."""
        query_id = str(getattr(event, 'id', 'unknown'))
        exception = getattr(event, 'exception', None)

        if query_id in self._active_queries:
            self._active_queries[query_id]["status"] = "terminated"
            self._active_queries[query_id]["terminated_at"] = time.time()

        if exception:
            logger.error(f"[Monitor] Query TERMINATED with error: {exception}")
            self._send_alert(f"STREAM TERMINATED: {exception}", severity="critical")
            self._schedule_restart(exception)
        else:
            logger.info(f"[Monitor] Query TERMINATED gracefully: id={query_id}")

    # ── Self-Healing Logic ────────────────────────────────────────────────────

    def _schedule_restart(self, reason: str) -> None:
        """Schedule auto-restart with exponential backoff."""
        if self._restart_count >= self.max_restart_attempts:
            logger.error(f"[Monitor] Max restart attempts ({self.max_restart_attempts}) reached. Manual intervention required.")
            self._send_alert("MAX RESTARTS REACHED — manual intervention required", severity="critical")
            return

        self._restart_count += 1
        delay = min(30 * self._restart_count, 300)  # Max 5 minutes
        logger.warning(f"[Monitor] Scheduling restart #{self._restart_count} in {delay}s")

        def _restart():
            time.sleep(delay)
            logger.info(f"[Monitor] Auto-restart #{self._restart_count} triggered")
            if self._restart_callback:
                try:
                    self._restart_callback()
                    self._restart_count = 0  # Reset on success
                except Exception as e:
                    logger.error(f"[Monitor] Restart failed: {e}")

        t = threading.Thread(target=_restart, daemon=True, name=f"restart-{self._restart_count}")
        t.start()

    def _send_alert(self, message: str, severity: str = "info") -> None:
        """Send alert to PagerDuty / Slack / email."""
        logger.warning(f"[ALERT][{severity.upper()}] {message}")
        try:
            if self._alert_callback:
                self._alert_callback(message, severity)
        except Exception as e:
            logger.error(f"Alert callback failed: {e}")
        # Push to Prometheus
        self._push_metric("pipeline_alerts_total", 1, {"severity": severity})

    def _push_metric(self, metric_name: str, value: float, labels: Dict = None) -> None:
        """Push metric to Prometheus Pushgateway."""
        try:
            from prometheus_client import Counter, Gauge, push_to_gateway, CollectorRegistry
            if not self.prometheus_gateway:
                return
            registry = CollectorRegistry()
            g = Gauge(metric_name, metric_name, list(labels.keys()) if labels else [], registry=registry)
            g.labels(**(labels or {})).set(value)
            push_to_gateway(self.prometheus_gateway, job="av_pipeline", registry=registry)
        except Exception:
            pass

    def get_metrics_summary(self) -> Dict:
        """Return summary of all collected metrics."""
        if not self._metrics_history:
            return {"batches": 0, "total_rows": 0, "avg_rate": 0.0}
        total_rows = sum(m.get("input_rows", 0) for m in self._metrics_history)
        avg_rate = sum(m.get("rows_per_second", 0) for m in self._metrics_history) / len(self._metrics_history)
        return {
            "batches": len(self._metrics_history),
            "total_rows": total_rows,
            "avg_rate": round(avg_rate, 2),
            "restart_count": self._restart_count,
            "active_queries": len([q for q in self._active_queries.values() if q.get("status") == "running"]),
        }


class WatchdogMonitor:
    """
    Watchdog thread — restarts query if no progress within timeout.
    Runs as a daemon thread alongside the streaming query.
    """

    def __init__(self, timeout_seconds: int = 300):
        self.timeout_seconds = timeout_seconds
        self._last_progress_ts = time.time()
        self._running = False
        self._thread = None

    def heartbeat(self) -> None:
        """Call this on each batch completion to reset watchdog."""
        self._last_progress_ts = time.time()

    def start(self, restart_fn: Callable) -> None:
        self._running = True
        def _watch():
            while self._running:
                elapsed = time.time() - self._last_progress_ts
                if elapsed > self.timeout_seconds:
                    logger.error(f"[Watchdog] No progress for {elapsed:.0f}s, triggering restart")
                    try:
                        restart_fn()
                    except Exception as e:
                        logger.error(f"[Watchdog] Restart failed: {e}")
                    self._last_progress_ts = time.time()
                time.sleep(30)
        self._thread = threading.Thread(target=_watch, daemon=True, name="av-watchdog")
        self._thread.start()
        logger.info(f"[Watchdog] Started with timeout={self.timeout_seconds}s")

    def stop(self):
        self._running = False
