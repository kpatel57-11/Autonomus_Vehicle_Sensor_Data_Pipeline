"""
checkpoints/checkpoint_manager.py
Stage 6: CHECKPOINT — CheckpointManager
Exactly-once semantics via offset tracking.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
from loguru import logger

from models.sensor_data import CheckpointState, PipelineMode
from config.config_manager import get_settings


class CheckpointManager:
    """
    Manages checkpoint state for exactly-once processing.
    Diagram: CheckpointManager → Offset logs on distributed storage
    Read prev offset → Find new files → Process → Write to sink (atomic)
    → at-least-once + idempotent write = exactly-once effect
    Streaming: Spark checkpoints + Kafka offset tracking
    """

    def __init__(self, run_id: str, checkpoint_dir: Optional[str] = None):
        self.run_id = run_id
        settings = get_settings()
        self.checkpoint_dir = Path(checkpoint_dir or settings.checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._current_state: Optional[CheckpointState] = None

    def _checkpoint_path(self, run_id: str) -> Path:
        return self.checkpoint_dir / f"{run_id}.json"

    async def save(self, state: CheckpointState):
        """Atomically save checkpoint state."""
        state.updated_at = datetime.utcnow()
        self._current_state = state

        # Write to temp file then atomic rename for durability
        path = self._checkpoint_path(self.run_id)
        tmp_path = path.with_suffix(".tmp")

        try:
            with open(tmp_path, "w") as f:
                json.dump(state.model_dump(mode="json"), f, indent=2, default=str)
            os.replace(tmp_path, path)
            logger.debug(f"Checkpoint saved: {path} (records={state.records_processed})")
        except Exception as e:
            logger.error(f"Checkpoint save failed: {e}")
            if tmp_path.exists():
                tmp_path.unlink()

    async def load(self, run_id: str) -> Optional[CheckpointState]:
        """Load existing checkpoint for recovery / offset resumption."""
        path = self._checkpoint_path(run_id)
        if not path.exists():
            return None
        try:
            with open(path) as f:
                data = json.load(f)
            state = CheckpointState(**data)
            self._current_state = state
            logger.info(f"Checkpoint loaded: {path} (records={state.records_processed})")
            return state
        except Exception as e:
            logger.error(f"Checkpoint load failed: {e}")
            return None

    async def find_latest(self) -> Optional[CheckpointState]:
        """Find the most recent checkpoint for auto-recovery."""
        checkpoints = sorted(
            self.checkpoint_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        if not checkpoints:
            return None
        return await self.load(checkpoints[0].stem)

    def get_kafka_offsets(self) -> Dict[str, Dict[int, int]]:
        """Get last committed Kafka offsets from checkpoint."""
        if self._current_state:
            return self._current_state.kafka_offsets
        return {}

    async def delete(self, run_id: str):
        path = self._checkpoint_path(run_id)
        if path.exists():
            path.unlink()
            logger.info(f"Checkpoint deleted: {path}")
