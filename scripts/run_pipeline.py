#!/usr/bin/env python3
"""
scripts/run_pipeline.py
CLI entrypoint to run the AV Sensor Data Pipeline.

Usage:
    python scripts/run_pipeline.py --mode batch
    python scripts/run_pipeline.py --mode stream --recipe av_streaming_realtime
    python scripts/run_pipeline.py --mode batch --dry-run
"""
import argparse, sys, os, logging, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("av-pipeline")


def parse_args():
    p = argparse.ArgumentParser(description="AV Sensor Data Pipeline CLI")
    p.add_argument("--mode",   choices=["batch", "stream"], default="batch")
    p.add_argument("--recipe", default="av_full_pipeline")
    p.add_argument("--dry-run", action="store_true", help="Validate config only, don't run")
    p.add_argument("--reset-checkpoints", action="store_true")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    return p.parse_args()


def main():
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    print("╔══════════════════════════════════════════════════════════╗")
    print("║   AV Sensor Data Pipeline  v2.0                          ║")
    print("║   Batch + Streaming | 500M+ records/day | Exactly-Once   ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"\n  Mode:   {args.mode.upper()}")
    print(f"  Recipe: {args.recipe}")
    print(f"  Dry-run:{args.dry_run}\n")

    from config.config_loader import ConfigLoader
    from config.settings import settings

    if args.reset_checkpoints:
        from checkpoint.checkpoint_manager import CheckpointManager
        cp = CheckpointManager(settings.storage.hdfs_base + "/checkpoints")
        cp.reset()
        logger.warning("All checkpoints reset")

    loader = ConfigLoader()
    recipe = loader.get_recipe(args.recipe)
    logger.info(f"Loaded recipe: {recipe.name} v{recipe.version}")
    logger.info(f"  Sources:    {recipe.sources}")
    logger.info(f"  Validators: {recipe.validators}")
    logger.info(f"  Processors: {recipe.processors}")
    logger.info(f"  Sinks:      {recipe.sinks}")

    if args.dry_run:
        logger.info("✅ Dry-run complete — config is valid")
        return 0

    recipe.mode = args.mode
    from core.pipeline_driver import BatchDriver, StreamingDriver
    driver = BatchDriver(recipe) if args.mode == "batch" else StreamingDriver(recipe)

    start = time.time()
    try:
        metrics = driver.processPipeline()
        elapsed = time.time() - start
        print(f"\n✅ Pipeline completed in {elapsed:.1f}s")
        print(f"   Records read:      {metrics.get('records_read', 0):,}")
        print(f"   Records processed: {metrics.get('records_processed', 0):,}")
        print(f"   Records rejected:  {metrics.get('records_rejected', 0):,}")
        return 0
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
