"""
CheckpointManager — Offset logs on distributed storage.
Implements exactly-once: read prev offset → find new files → process → write offset.
Supports at-least-once + idempotent write = exactly-once effect.
"""
import json, logging, os, time
from typing import Any, Dict, Optional
from core.interfaces import ICheckpointManager
logger = logging.getLogger(__name__)


class CheckpointManager(ICheckpointManager):
    """
    Stores checkpoints in HDFS/S3 as JSON files.
    Tracks last processed offset per source + completed batch IDs.
    """

    def __init__(self, base_path: str = "/tmp/av/checkpoints"):
        self.base_path = base_path
        self._local_cache: Dict[str, Any] = {}
        os.makedirs(base_path, exist_ok=True)

    def _offset_path(self, source: str) -> str:
        return os.path.join(self.base_path, f"offset_{source}.json")

    def _batch_path(self, batch_id: str) -> str:
        return os.path.join(self.base_path, f"batch_{batch_id}.done")

    def save_offset(self, source: str, offset: Any) -> None:
        """Persist the last successfully processed offset for a source."""
        data = {
            "source": source,
            "offset": offset,
            "saved_at": time.time(),
            "saved_at_human": time.strftime("%Y-%m-%d %Human:%M:%S"),
        }
        path = self._offset_path(source)
        try:
            with open(path, "w") as f:
                json.dump(data, f)
            self._local_cache[source] = offset
            logger.info(f"Checkpoint saved: source={source}, offset={offset}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint for {source}: {e}")

    def get_last_offset(self, source: str) -> Optional[Any]:
        """Read the last persisted offset — used for resumption after failure."""
        if source in self._local_cache:
            return self._local_cache[source]
        path = self._offset_path(source)
        try:
            if os.path.exists(path):
                with open(path) as f:
                    data = json.load(f)
                offset = data.get("offset")
                self._local_cache[source] = offset
                logger.info(f"Loaded checkpoint: source={source}, offset={offset}")
                return offset
        except Exception as e:
            logger.warning(f"Failed to read checkpoint for {source}: {e}")
        return None

    def mark_complete(self, batch_id: str) -> None:
        """Mark a batch as successfully completed — idempotency guard."""
        path = self._batch_path(batch_id)
        try:
            with open(path, "w") as f:
                json.dump({"batch_id": batch_id, "completed_at": time.time()}, f)
            logger.info(f"Batch {batch_id} marked complete")
        except Exception as e:
            logger.error(f"Failed to mark batch complete: {e}")

    def is_completed(self, batch_id: str) -> bool:
        """Check if batch was already processed — prevents reprocessing."""
        return os.path.exists(self._batch_path(batch_id))

    def list_checkpoints(self) -> Dict[str, Any]:
        """List all saved offsets — useful for monitoring."""
        result = {}
        try:
            for fname in os.listdir(self.base_path):
                if fname.startswith("offset_") and fname.endswith(".json"):
                    source = fname[7:-5]
                    result[source] = self.get_last_offset(source)
        except Exception as e:
            logger.warning(f"list_checkpoints: {e}")
        return result

    def reset(self, source: Optional[str] = None) -> None:
        """Reset checkpoints — use for full reprocessing."""
        try:
            if source:
                path = self._offset_path(source)
                if os.path.exists(path):
                    os.remove(path)
                self._local_cache.pop(source, None)
                logger.warning(f"Checkpoint reset for source: {source}")
            else:
                import shutil
                shutil.rmtree(self.base_path)
                os.makedirs(self.base_path)
                self._local_cache.clear()
                logger.warning("ALL checkpoints reset")
        except Exception as e:
            logger.error(f"Checkpoint reset failed: {e}")


class SparkStreamingCheckpoint:
    """
    Wrapper for Spark Structured Streaming checkpoints.
    Manages Kafka offset tracking + Spark checkpoint dirs.
    """

    def __init__(self, base_path: str, run_id: str):
        self.checkpoint_dir = os.path.join(base_path, "spark_streaming", run_id)
        self.kafka_offsets_dir = os.path.join(base_path, "kafka_offsets", run_id)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.kafka_offsets_dir, exist_ok=True)

    def get_checkpoint_location(self) -> str:
        return self.checkpoint_dir

    def save_kafka_offsets(self, topic_offsets: Dict[str, Dict[int, int]]) -> None:
        """Save Kafka topic partition offsets."""
        path = os.path.join(self.kafka_offsets_dir, f"offsets_{int(time.time())}.json")
        with open(path, "w") as f:
            json.dump(topic_offsets, f)
        logger.info(f"Kafka offsets saved: {topic_offsets}")

    def get_latest_kafka_offsets(self) -> Optional[Dict]:
        """Load most recent Kafka offsets for resumption."""
        try:
            files = sorted(os.listdir(self.kafka_offsets_dir))
            if files:
                with open(os.path.join(self.kafka_offsets_dir, files[-1])) as f:
                    return json.load(f)
        except Exception:
            pass
        return None
