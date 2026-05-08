"""
self_healing/lifecycle_monitor.py
SELF-HEALING — QueryLifecycleMonitor (Observer Pattern)
Diagram: onQueryStarted → log, onQueryProgress → metrics, onQueryTerminated → alert
Publish → Message Queue → Watchdog auto-restart → PageDuty/Stack alerts
→ Dead letter queue for bad data
"""
from __future__ import annotations
import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime
from loguru import logger

from monitoring.metrics import PipelineMetrics


# ─────────────────────────────────────────────────────────────────────────────
# Observer Interface
# ─────────────────────────────────────────────────────────────────────────────

class IQueryObserver(ABC):
    @abstractmethod
    def on_started(self, run_id: str, mode: str): ...

    @abstractmethod
    def on_progress(self, run_id: str, summary: Dict[str, Any]): ...

    @abstractmethod
    def on_terminated(self, run_id: str, error: Optional[str] = None): ...


# ─────────────────────────────────────────────────────────────────────────────
# Concrete Observers
# ─────────────────────────────────────────────────────────────────────────────

class LoggingObserver(IQueryObserver):
    """Logs all lifecycle events."""

    def on_started(self, run_id: str, mode: str):
        logger.info(f"[LIFECYCLE] Query STARTED | run_id={run_id} | mode={mode}")

    def on_progress(self, run_id: str, summary: Dict[str, Any]):
        logger.info(
            f"[LIFECYCLE] Query PROGRESS | run_id={run_id} | "
            f"in={summary.get('records_in', 0)} "
            f"out={summary.get('records_written', 0)}"
        )

    def on_terminated(self, run_id: str, error: Optional[str] = None):
        if error:
            logger.error(f"[LIFECYCLE] Query TERMINATED WITH ERROR | run_id={run_id} | error={error}")
        else:
            logger.info(f"[LIFECYCLE] Query TERMINATED cleanly | run_id={run_id}")


class MetricsObserver(IQueryObserver):
    """Pushes metrics to Prometheus."""

    def __init__(self):
        self._metrics = PipelineMetrics(run_id="monitor")

    def on_started(self, run_id: str, mode: str):
        self._metrics.inc_counter("pipeline_starts_total", labels={"mode": mode})

    def on_progress(self, run_id: str, summary: Dict[str, Any]):
        records_in = summary.get("records_in", 0)
        records_out = summary.get("records_written", 0)
        self._metrics.set_gauge("pipeline_records_in", records_in)
        self._metrics.set_gauge("pipeline_records_out", records_out)

    def on_terminated(self, run_id: str, error: Optional[str] = None):
        if error:
            self._metrics.inc_counter("pipeline_failures_total")


class AlertObserver(IQueryObserver):
    """Sends alerts to PagerDuty / Slack on failures."""

    def __init__(self, alert_webhook: Optional[str] = None):
        self.webhook = alert_webhook

    def on_started(self, run_id: str, mode: str):
        pass

    def on_progress(self, run_id: str, summary: Dict[str, Any]):
        # Alert if record failure rate > 10%
        total = summary.get("records_in", 1)
        written = summary.get("records_written", 0)
        if total > 0 and (total - written) / total > 0.1:
            self._send_alert(
                f"⚠️ High failure rate: {total - written}/{total} records failed | run_id={run_id}",
                severity="warning"
            )

    def on_terminated(self, run_id: str, error: Optional[str] = None):
        if error:
            self._send_alert(
                f"🚨 Pipeline terminated with error | run_id={run_id} | error={error}",
                severity="critical"
            )

    def _send_alert(self, message: str, severity: str = "info"):
        logger.warning(f"[ALERT:{severity.upper()}] {message}")
        if self.webhook:
            try:
                import httpx
                httpx.post(self.webhook, json={"text": message, "severity": severity})
            except Exception as e:
                logger.error(f"Alert webhook failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# QueryLifecycleMonitor (Observable)
# ─────────────────────────────────────────────────────────────────────────────

class QueryLifecycleMonitor:
    """
    Observer-pattern lifecycle monitor.
    Diagram: QueryLifecycleMonitor (Observer pattern)
    onQueryStarted → log
    onQueryProgress → metrics
    onQueryTerminated → alert
    """

    def __init__(self):
        self._observers: List[IQueryObserver] = [
            LoggingObserver(),
            MetricsObserver(),
            AlertObserver(),
        ]
        self._run_states: Dict[str, Dict[str, Any]] = {}

    def add_observer(self, observer: IQueryObserver):
        self._observers.append(observer)

    def remove_observer(self, observer: IQueryObserver):
        self._observers.remove(observer)

    def on_query_started(self, run_id: str, mode: str):
        self._run_states[run_id] = {
            "run_id": run_id,
            "mode": mode,
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
        }
        for obs in self._observers:
            try:
                obs.on_started(run_id, mode)
            except Exception as e:
                logger.error(f"Observer {obs.__class__.__name__} error: {e}")

    def on_query_progress(self, run_id: str, summary: Dict[str, Any]):
        if run_id in self._run_states:
            self._run_states[run_id].update(summary)
        for obs in self._observers:
            try:
                obs.on_progress(run_id, summary)
            except Exception as e:
                logger.error(f"Observer {obs.__class__.__name__} error: {e}")

    def on_query_terminated(self, run_id: str, error: Optional[str] = None):
        if run_id in self._run_states:
            self._run_states[run_id]["status"] = "failed" if error else "completed"
            self._run_states[run_id]["completed_at"] = datetime.utcnow().isoformat()
            self._run_states[run_id]["error"] = error
        for obs in self._observers:
            try:
                obs.on_terminated(run_id, error)
            except Exception as e:
                logger.error(f"Observer {obs.__class__.__name__} error: {e}")

    def get_run_states(self) -> Dict[str, Dict]:
        return self._run_states.copy()

    def get_run(self, run_id: str) -> Optional[Dict]:
        return self._run_states.get(run_id)


# ─────────────────────────────────────────────────────────────────────────────
# Watchdog — auto-restart failed pipelines
# ─────────────────────────────────────────────────────────────────────────────

class PipelineWatchdog:
    """
    Watchdog that auto-restarts failed streaming pipelines.
    Diagram: Watchdog auto-restart (30s)
    → Publish to Message Queue → PageDuty/Stack alerts
    → Dead letter queue for bad data
    """

    def __init__(self, restart_delay_s: float = 30.0, max_restarts: int = 5):
        self.restart_delay = restart_delay_s
        self.max_restarts = max_restarts
        self._restart_counts: Dict[str, int] = {}
        self._running = False

    async def watch(self, pipeline_coro_factory: Callable, run_id: str):
        """Watch and restart a pipeline coroutine on failure."""
        self._restart_counts[run_id] = 0

        while self._restart_counts[run_id] <= self.max_restarts:
            try:
                logger.info(f"[WATCHDOG] Starting pipeline {run_id} (attempt {self._restart_counts[run_id] + 1})")
                await pipeline_coro_factory()
                logger.info(f"[WATCHDOG] Pipeline {run_id} completed normally")
                break
            except Exception as e:
                self._restart_counts[run_id] += 1
                count = self._restart_counts[run_id]
                logger.error(f"[WATCHDOG] Pipeline {run_id} failed (attempt {count}): {e}")

                if count > self.max_restarts:
                    logger.critical(f"[WATCHDOG] Max restarts ({self.max_restarts}) exceeded for {run_id}")
                    self._escalate(run_id, str(e))
                    break

                logger.info(f"[WATCHDOG] Restarting in {self.restart_delay}s...")
                await asyncio.sleep(self.restart_delay)

    def _escalate(self, run_id: str, error: str):
        """Escalate to PagerDuty/Slack after max restarts."""
        logger.critical(f"[ESCALATE] 🚨 CRITICAL: Pipeline {run_id} requires manual intervention. Error: {error}")
        # Production: POST to PagerDuty/OpsGenie API

    @property
    def restart_counts(self) -> Dict[str, int]:
        return self._restart_counts.copy()


# ─────────────────────────────────────────────────────────────────────────────
# Dead Letter Queue Manager
# ─────────────────────────────────────────────────────────────────────────────

class DLQManager:
    """
    Manages the Dead Letter Queue for bad/invalid records.
    Diagram: Dead Letter Queue → Bad records → DLQ for investigation
    """

    def __init__(self):
        self._dlq: list = []
        self._total_sent = 0

    def send(self, record: Dict[str, Any], reason: str, stage: str):
        dlq_entry = {
            "record_id": record.get("record_id"),
            "sensor_type": record.get("sensor_type"),
            "failure_stage": stage,
            "failure_reason": reason,
            "timestamp": datetime.utcnow().isoformat(),
            "retry_count": record.get("retry_count", 0),
        }
        self._dlq.append(dlq_entry)
        self._total_sent += 1
        logger.warning(f"[DLQ] Record {record.get('record_id')} → DLQ | reason={reason}")

    def get_entries(self, limit: int = 100) -> list:
        return self._dlq[-limit:]

    @property
    def total_sent(self) -> int:
        return self._total_sent

    def reprocess(self, record_id: str) -> Optional[Dict]:
        """Find a DLQ record by ID for reprocessing."""
        for entry in self._dlq:
            if entry.get("record_id") == record_id:
                return entry
        return None
