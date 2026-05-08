"""
PipelineDriver — Template Method pattern.
Abstract base defines processPipeline() skeleton.
BatchDriver and StreamingDriver are concrete implementations.
"""
import logging, time, uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from config.settings import settings
from config.config_loader import ConfigLoader, PipelineRecipe
from core.interfaces import PipelineMode
logger = logging.getLogger(__name__)


class PipelineDriver(ABC):
    """Abstract PipelineDriver. Template Method: processPipeline()."""

    def __init__(self, recipe: Optional[PipelineRecipe] = None):
        self.loader = ConfigLoader(mongo_uri=settings.mongo.uri, postgres_dsn=settings.postgres.dsn)
        self.recipe = recipe or self.loader.get_recipe()
        self.spark = None
        self.run_id = str(uuid.uuid4())[:8]
        self._metrics: Dict[str, Any] = {}

    def processPipeline(self) -> Dict[str, Any]:
        """THE template method — 6-stage invariant skeleton."""
        logger.info(f"[{self.run_id}] Pipeline START — recipe={self.recipe.name} mode={self.mode.value}")
        start = time.time()
        try:
            self.spark = self._init_spark()

            logger.info(f"[{self.run_id}] Stage 1: SOURCES")
            df = self._read_sources()
            self._metrics["records_read"] = self._count(df)

            logger.info(f"[{self.run_id}] Stage 2: VALIDATORS")
            df, dlq_df = self._run_validators(df)
            self._metrics["records_valid"] = self._count(df)
            self._metrics["records_rejected"] = self._count(dlq_df)
            self._send_to_dlq(dlq_df)

            logger.info(f"[{self.run_id}] Stage 3: SQL TRANSFORMS")
            df = self._run_transforms(df)

            logger.info(f"[{self.run_id}] Stage 4: PROCESSORS")
            df = self._run_processors(df)
            self._metrics["records_processed"] = self._count(df)

            logger.info(f"[{self.run_id}] Stage 5: SINK")
            self._write_sinks(df)

            logger.info(f"[{self.run_id}] Stage 6: CHECKPOINT")
            self._run_checkpoint()

            self._metrics.update({"status": "SUCCESS", "elapsed_s": round(time.time()-start, 2)})
            logger.info(f"[{self.run_id}] Pipeline DONE — {self._metrics}")
            return self._metrics

        except Exception as e:
            self._metrics.update({"status": "FAILED", "error": str(e)})
            logger.error(f"[{self.run_id}] Pipeline FAILED: {e}", exc_info=True)
            self._on_failure(e)
            raise

    @property
    @abstractmethod
    def mode(self) -> PipelineMode: pass

    @abstractmethod
    def _init_spark(self) -> Any: pass

    @abstractmethod
    def _read_sources(self) -> Any: pass

    @abstractmethod
    def _run_validators(self, df: Any): pass

    @abstractmethod
    def _run_transforms(self, df: Any) -> Any: pass

    @abstractmethod
    def _run_processors(self, df: Any) -> Any: pass

    @abstractmethod
    def _write_sinks(self, df: Any) -> None: pass

    @abstractmethod
    def _run_checkpoint(self) -> None: pass

    def _send_to_dlq(self, dlq_df):
        if dlq_df is None or not settings.dlq_enabled: return
        try:
            n = self._count(dlq_df)
            if n and n > 0:
                from sink.sink_factory import DLQWriter
                DLQWriter().write(dlq_df, {"run_id": self.run_id})
                logger.warning(f"[{self.run_id}] DLQ: {n} records")
        except Exception as e:
            logger.error(f"DLQ write failed: {e}")

    def _on_failure(self, error: Exception): pass

    def _count(self, df) -> int:
        if df is None: return 0
        try: return df.count() if hasattr(df, "count") else len(df)
        except: return -1

    @classmethod
    def main(cls, mode: str = "batch", recipe_name: str = "av_full_pipeline"):
        loader = ConfigLoader(mongo_uri=settings.mongo.uri, postgres_dsn=settings.postgres.dsn)
        recipe = loader.get_recipe(recipe_name)
        recipe.mode = mode
        driver = BatchDriver(recipe) if mode == "batch" else StreamingDriver(recipe)
        driver.processPipeline()


class BatchDriver(PipelineDriver):
    """Batch: read → process → write → exit. Triggered by Airflow DAGs."""

    @property
    def mode(self): return PipelineMode.BATCH

    def _init_spark(self):
        from pyspark.sql import SparkSession
        cfg = settings.spark
        b = SparkSession.builder.appName(f"{cfg.app_name}-Batch").master(cfg.master) \
            .config("spark.serializer", cfg.serializer).config("spark.executor.memory", cfg.executor_memory)
        for k, v in cfg.extra_configs.items():
            b = b.config(k, v)
        spark = b.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _read_sources(self):
        from ingestion.source_factory import SourceReaderFactory
        factory = SourceReaderFactory()
        dfs = []
        for src in self.recipe.sources:
            reader = factory.get_reader(src)
            meta = self.loader.get_dataset_metadata(src)
            df = reader.read(self.spark, meta)
            dfs.append(df)
            logger.info(f"  Source '{src}' read OK — columns: {df.columns[:5]}")
        if not dfs: raise ValueError("No sources")
        result = dfs[0]
        for df in dfs[1:]:
            try: result = result.unionByName(df, allowMissingColumns=True)
            except: result = result.union(df)
        return result

    def _run_validators(self, df):
        from validators.validation_factory import ValidationFactory
        factory = ValidationFactory()
        dlq_dfs = []
        for vname in self.recipe.validators:
            v = factory.get_validator(vname)
            df, bad = v.validate_batch(df)
            if bad is not None: dlq_dfs.append(bad)
        dlq = None
        if dlq_dfs:
            dlq = dlq_dfs[0]
            for d in dlq_dfs[1:]:
                try: dlq = dlq.union(d)
                except: pass
        return df, dlq

    def _run_transforms(self, df):
        from transforms.sql_manager import SqlManager
        mgr = SqlManager(self.spark)
        for t in self.recipe.transforms:
            df = mgr.apply(df, t["sql"], t.get("params", {}))
        return df

    def _run_processors(self, df):
        from processors.processor_factory import ProcessorFactory
        factory = ProcessorFactory()
        for pname in self.recipe.processors:
            p = factory.get_processor(pname)
            result = p.process(df, self.recipe.extra)
            if result.success and result.output is not None:
                df = result.output
            self._metrics[f"proc_{pname}"] = result.metrics
        return df

    def _write_sinks(self, df):
        from sink.sink_factory import SinkWriterFactory
        factory = SinkWriterFactory()
        for sname in self.recipe.sinks:
            w = factory.get_writer(sname)
            w.write(df, self.recipe.extra)
            w.commit()

    def _run_checkpoint(self):
        from checkpoint.checkpoint_manager import CheckpointManager
        cp = CheckpointManager(settings.storage.hdfs_base + "/checkpoints")
        cp.mark_complete(self.run_id)
        for src in self.recipe.sources:
            cp.save_offset(src, {"run_id": self.run_id, "ts": time.time()})
        logger.info(f"Batch {self.run_id} checkpointed")


class StreamingDriver(PipelineDriver):
    """Streaming: readStream → foreachBatch → 24/7 with auto-restart."""

    @property
    def mode(self): return PipelineMode.STREAM

    def _init_spark(self):
        from pyspark.sql import SparkSession
        cfg = settings.spark
        b = SparkSession.builder.appName(f"{cfg.app_name}-Stream").master(cfg.master) \
            .config("spark.serializer", cfg.serializer) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
        for k, v in cfg.extra_configs.items():
            b = b.config(k, v)
        spark = b.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _read_sources(self):
        from config.settings import settings as s
        df = (self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", s.kafka.bootstrap_servers)
            .option("subscribe", ",".join(s.kafka.topics.values()))
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false").load())
        df = df.withWatermark("timestamp", s.watermark_delay)
        return df

    def _run_validators(self, df): return df, None

    def _run_transforms(self, df):
        from transforms.sql_manager import SqlManager
        mgr = SqlManager(self.spark)
        for t in self.recipe.transforms:
            df = mgr.apply_streaming(df, t["sql"], t.get("params", {}))
        return df

    def _run_processors(self, df): return df  # Done inside foreachBatch

    def _write_sinks(self, df):
        from processors.processor_factory import ProcessorFactory
        from sink.sink_factory import SinkWriterFactory
        from self_healing.query_lifecycle_monitor import QueryLifecycleMonitor
        pf = ProcessorFactory(); sf = SinkWriterFactory()
        recipe = self.recipe; run_id = self.run_id
        monitor = QueryLifecycleMonitor(restart_callback=self._on_restart)

        def foreach_batch(batch_df, batch_id):
            logger.info(f"Micro-batch {batch_id}: {batch_df.count()} rows")
            monitor.heartbeat() if hasattr(monitor, 'heartbeat') else None
            processed = batch_df
            for pname in recipe.processors:
                p = pf.get_processor(pname)
                r = p.process(processed, recipe.extra)
                if r.success and r.output is not None:
                    processed = r.output
            for sname in recipe.sinks:
                w = sf.get_writer(sname)
                w.write(processed, recipe.extra); w.commit()

        query = (df.writeStream.foreachBatch(foreach_batch)
            .outputMode("append")
            .option("checkpointLocation", f"{settings.storage.hdfs_base}/checkpoints/{self.run_id}")
            .trigger(processingTime="30 seconds").start())

        try:
            self.spark.streams.addListener(monitor)
        except Exception:
            pass

        logger.info("Streaming query started — awaiting termination")
        query.awaitTermination()

    def _run_checkpoint(self): pass

    def _on_restart(self):
        logger.info("Stream auto-restart triggered")
        time.sleep(30)
        self.processPipeline()

    def _on_failure(self, error):
        logger.warning(f"Stream failure, auto-restart in 30s: {error}")
        time.sleep(30)
        logger.info("Restarting stream pipeline...")
        self.processPipeline()


if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    PipelineDriver.main(mode=mode)
