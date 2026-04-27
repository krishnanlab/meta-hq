"""
Pipeline orchestration for metahq-setup.

Coordinates the multi-stage database build process with checkpointing
and progress tracking.
"""

from collections.abc import Callable
from pathlib import Path

from metahq_setup.combiners.geo import GEO_COMBINED_BSON, GeoCombiner
from metahq_setup.combiners.sample import SAMPLE_COMBINED_BSON, SampleCombiner
from metahq_setup.combiners.sra import SRA_COMBINED_BSON, SraCombiner
from metahq_setup.config.config import OMICIDX_DB, PROCESSED_DIR
from metahq_setup.processors import ProcessorRegistry
from metahq_setup.util.checkpointing import CheckpointManager
from metahq_setup.util.logging import setup_logger


class PipelineOrchestrator:
    """
    Orchestrates the complete MetaHQ database build pipeline.

    Runs processing for all registered sources in alphabetical order, then
    combines GEO, SRA, and sample annotations. Supports checkpoint-based
    resumption so a failed run can be restarted from the last completed stage.
    """

    def __init__(
        self,
        output_dir: Path = PROCESSED_DIR,
        checkpoint_dir: Path = Path(".checkpoints"),
        db_path: Path = OMICIDX_DB,
    ):
        """
        Initialize the pipeline orchestrator.

        Arguments:
            output_dir (Path):
                Directory where processor parquets are written.
                Defaults to ``PROCESSED_DIR`` from config.
            checkpoint_dir (Path):
                Directory for checkpoint state files.
                Defaults to ``.checkpoints`` in the working directory.
            db_path (Path):
                Path to the OmicIDX DuckDB file used by SRA and sample combiners.
                Defaults to ``OMICIDX_DB`` from config.
        """
        self.output_dir = Path(output_dir)
        self.db_path = Path(db_path)
        self.checkpoints = CheckpointManager(checkpoint_dir)
        self.logger = setup_logger("metahq_setup.pipeline")

    def run(
        self,
        start_from: str | None = None,
        end_at: str | None = None,
    ) -> None:
        """
        Execute all pipeline stages in order with checkpointing.

        Already-completed stages (recorded in the checkpoint file) are skipped
        automatically. Use ``start_from`` to ignore everything before a named
        stage (overriding the checkpoint file), or ``end_at`` to stop after a
        named stage.

        Arguments:
            start_from (str | None):
                Stage name to resume from. All earlier stages are skipped
                regardless of checkpoint state.
            end_at (str | None):
                Stage name to stop after. Later stages are not executed.
        """
        stages = self._build_stages()
        reached_start = start_from is None

        for stage_name, stage_fn in stages:
            if not reached_start:
                if stage_name == start_from:
                    reached_start = True
                else:
                    self.logger.info("Skipping stage (before start): %s", stage_name)
                    continue

            if self.checkpoints.is_stage_completed(stage_name):
                self.logger.info("Skipping stage (already completed): %s", stage_name)
            else:
                self.logger.info("Starting stage: %s", stage_name)
                stage_fn()
                self.checkpoints.save_checkpoint(stage_name)
                self.logger.info("Finished stage: %s", stage_name)

            if end_at and stage_name == end_at:
                self.logger.info("Reached end stage, stopping: %s", end_at)
                break

    def _build_stages(self) -> list[tuple[str, Callable]]:
        """Build the ordered list of (stage_name, callable) pairs."""
        stages: list[tuple[str, Callable]] = []

        for source_name in ProcessorRegistry.list_processors():
            processor = ProcessorRegistry.get(source_name)
            stages.append((
                f"process__{source_name}",
                lambda p=processor: p.run(output_dir=self.output_dir),
            ))

        stages.append((
            "combine__geo",
            lambda: GeoCombiner().combine().clean().save(GEO_COMBINED_BSON),
        ))
        stages.append((
            "combine__sra",
            lambda: SraCombiner().combine(db_path=self.db_path).clean().save(SRA_COMBINED_BSON),
        ))
        stages.append((
            "combine__sample",
            lambda: SampleCombiner().combine(
                geo_bson=GEO_COMBINED_BSON,
                sra_bson=SRA_COMBINED_BSON,
                db_path=self.db_path,
            ).clean().save(SAMPLE_COMBINED_BSON),
        ))

        return stages


__all__ = ["PipelineOrchestrator"]
