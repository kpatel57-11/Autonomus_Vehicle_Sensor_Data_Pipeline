"""
FastAPI REST API — Pipeline control, metrics, status, and data catalog.
Exposes endpoints for the React dashboard.
"""
import logging, time, uuid, json
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

logger = logging.getLogger(__name__)

if HAS_FASTAPI:
    app = FastAPI(
        title="AV Sensor Pipeline API",
        description="Autonomous Vehicle Sensor Data Pipeline — REST Control API",
        version="2.0.0",
    )

    app.add_middleware(CORSMiddleware,
        allow_origins=["*"], allow_methods=["*"],
        allow_headers=["*"], allow_credentials=True)

    # ── In-memory state (replace with Redis/DB in production) ─────────────────
    _runs: Dict[str, Dict] = {}
    _pipeline_status = {"status": "idle", "last_run": None, "total_records": 0}

    # ── Request Models ─────────────────────────────────────────────────────────
    class RunRequest(BaseModel):
        mode: str = "batch"
        recipe_name: str = "av_full_pipeline"
        dry_run: bool = False

    class RecipeCreateRequest(BaseModel):
        name: str
        sources: List[str]
        validators: List[str]
        processors: List[str]
        sinks: List[str]
        mode: str = "batch"

    # ── Health ─────────────────────────────────────────────────────────────────
    @app.get("/health")
    def health():
        return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "version": "2.0.0"}

    @app.get("/api/status")
    def pipeline_status():
        from checkpoint.checkpoint_manager import CheckpointManager
        cp = CheckpointManager()
        checkpoints = cp.list_checkpoints()
        return {
            "pipeline": _pipeline_status,
            "active_runs": {k: v for k, v in _runs.items() if v.get("status") == "running"},
            "checkpoints": checkpoints,
            "uptime_s": time.time(),
        }

    # ── Pipeline Control ───────────────────────────────────────────────────────
    @app.post("/api/pipeline/run")
    def run_pipeline(req: RunRequest, background_tasks: BackgroundTasks):
        run_id = str(uuid.uuid4())[:8]
        _runs[run_id] = {"id": run_id, "mode": req.mode, "recipe": req.recipe_name,
                         "status": "running", "started_at": time.time(), "metrics": {}}
        _pipeline_status["status"] = "running"

        def _run():
            try:
                from config.config_loader import ConfigLoader
                from config.settings import settings
                from core.pipeline_driver import BatchDriver, StreamingDriver
                loader = ConfigLoader()
                recipe = loader.get_recipe(req.recipe_name)
                recipe.mode = req.mode
                driver = BatchDriver(recipe) if req.mode == "batch" else StreamingDriver(recipe)
                metrics = driver.processPipeline()
                _runs[run_id].update({"status": "success", "metrics": metrics, "finished_at": time.time()})
                _pipeline_status.update({"status": "idle", "last_run": run_id,
                    "total_records": _pipeline_status["total_records"] + metrics.get("records_processed", 0)})
            except Exception as e:
                _runs[run_id].update({"status": "failed", "error": str(e), "finished_at": time.time()})
                _pipeline_status["status"] = "error"

        if not req.dry_run:
            background_tasks.add_task(_run)

        return {"run_id": run_id, "status": "started", "mode": req.mode}

    @app.get("/api/pipeline/runs")
    def list_runs(limit: int = Query(20, le=100)):
        runs = sorted(_runs.values(), key=lambda r: r.get("started_at", 0), reverse=True)
        return {"runs": runs[:limit], "total": len(_runs)}

    @app.get("/api/pipeline/runs/{run_id}")
    def get_run(run_id: str):
        if run_id not in _runs:
            raise HTTPException(404, f"Run '{run_id}' not found")
        return _runs[run_id]

    @app.delete("/api/pipeline/runs/{run_id}")
    def cancel_run(run_id: str):
        if run_id not in _runs:
            raise HTTPException(404, f"Run '{run_id}' not found")
        _runs[run_id]["status"] = "cancelled"
        return {"message": f"Run {run_id} cancelled"}

    # ── Config / Recipes ───────────────────────────────────────────────────────
    @app.get("/api/config/recipe")
    def get_default_recipe():
        from config.config_loader import ConfigLoader
        loader = ConfigLoader()
        recipe = loader.get_recipe()
        return recipe.to_dict()

    @app.post("/api/config/recipe")
    def create_recipe(req: RecipeCreateRequest):
        from config.config_loader import ConfigLoader, PipelineRecipe
        loader = ConfigLoader()
        recipe = PipelineRecipe(req.dict())
        loader.save_recipe(recipe)
        return {"message": f"Recipe '{req.name}' saved", "recipe": req.dict()}

    @app.get("/api/config/sources")
    def list_sources():
        from ingestion.source_factory import SourceReaderFactory
        return {"sources": SourceReaderFactory().list_available()}

    @app.get("/api/config/validators")
    def list_validators():
        from validators.validation_factory import ValidationFactory
        return {"validators": ValidationFactory().list_available()}

    @app.get("/api/config/processors")
    def list_processors():
        from processors.processor_factory import ProcessorFactory
        return {"processors": ProcessorFactory().list_available()}

    @app.get("/api/config/sinks")
    def list_sinks():
        from sink.sink_factory import SinkWriterFactory
        return {"sinks": SinkWriterFactory().list_available()}

    # ── Metrics ────────────────────────────────────────────────────────────────
    @app.get("/api/metrics")
    def get_metrics():
        completed = [r for r in _runs.values() if r.get("status") == "success"]
        total_records = sum(r.get("metrics", {}).get("records_processed", 0) for r in completed)
        total_rejected = sum(r.get("metrics", {}).get("records_rejected", 0) for r in completed)
        avg_elapsed = sum(r.get("metrics", {}).get("elapsed_s", 0) for r in completed) / max(len(completed), 1)
        return {
            "total_runs": len(_runs),
            "successful_runs": len(completed),
            "failed_runs": len([r for r in _runs.values() if r.get("status") == "failed"]),
            "total_records_processed": total_records,
            "total_records_rejected": total_rejected,
            "avg_run_duration_s": round(avg_elapsed, 2),
            "throughput_records_per_run": round(total_records / max(len(completed), 1)),
        }

    @app.get("/api/metrics/prometheus")
    def prometheus_metrics():
        """Prometheus-format metrics endpoint."""
        metrics = get_metrics()
        lines = [
            f'# HELP av_pipeline_runs_total Total pipeline runs',
            f'# TYPE av_pipeline_runs_total counter',
            f'av_pipeline_runs_total {metrics["total_runs"]}',
            f'# HELP av_pipeline_records_total Total records processed',
            f'# TYPE av_pipeline_records_total counter',
            f'av_pipeline_records_total {metrics["total_records_processed"]}',
        ]
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("\n".join(lines))

    # ── Data Catalog ───────────────────────────────────────────────────────────
    @app.get("/api/catalog")
    def data_catalog():
        from config.config_loader import ConfigLoader
        loader = ConfigLoader()
        datasets = ["kafka_lidar", "kafka_camera", "kafka_gps", "s3_radar"]
        return {
            "datasets": [{"name": d, **loader.get_dataset_metadata(d)} for d in datasets]
        }

    # ── Checkpoint Control ─────────────────────────────────────────────────────
    @app.get("/api/checkpoints")
    def list_checkpoints():
        from checkpoint.checkpoint_manager import CheckpointManager
        from config.settings import settings
        cp = CheckpointManager(settings.storage.hdfs_base + "/checkpoints")
        return {"checkpoints": cp.list_checkpoints()}

    @app.delete("/api/checkpoints/{source}")
    def reset_checkpoint(source: str):
        from checkpoint.checkpoint_manager import CheckpointManager
        from config.settings import settings
        cp = CheckpointManager(settings.storage.hdfs_base + "/checkpoints")
        cp.reset(source)
        return {"message": f"Checkpoint reset for source: {source}"}


    if __name__ == "__main__":
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
