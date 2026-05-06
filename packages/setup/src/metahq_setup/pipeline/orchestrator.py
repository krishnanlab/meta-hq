"""
Pipeline orchestration for metahq-setup.

Coordinates the multi-stage database build process with checkpointing
and progress tracking.
"""

from collections.abc import Callable

from metahq_setup.builders import (
    DataPackageBuilder,
    MetadataBuilder,
    OntologySearchDbBuilder,
    ShieldEndpointBuilder,
)
from metahq_setup.combiners.geo import GeoCombiner
from metahq_setup.combiners.sample import SampleCombiner
from metahq_setup.combiners.sra import SraCombiner
from metahq_setup.combiners.study import StudyCombiner
from metahq_setup.config import (
    GEO_COMBINED_BSON,
    MONDO_NAMES_SYNONYMS,
    MONDO_OBO,
    MONDO_RELATIONS,
    MONDO_SYSTEMS,
    OMICIDX_SAMPLE_TABLE,
    OMICIDX_SERIES_TABLE,
    ONTOLOGY_SEARCH_DB,
    SAMPLE_COMBINED_BSON,
    SAMPLE_METADATA,
    SERIES_COMBINED_BSON,
    SERIES_METADATA,
    SOURCE_COUNT_SHIELD_OUTDIR,
    SRA_COMBINED_BSON,
    UBERON_CL_NAMES_SYNONYMS,
    UBERON_OBO,
    UBERON_RELATIONS,
    UBERON_SYSTEMS,
)
from metahq_setup.config.schema import DataPackageConfig
from metahq_setup.ontology import Graph
from metahq_setup.processors import ProcessorRegistry
from metahq_setup.util.checkpointing import CheckpointManager
from metahq_setup.util.logging import setup_logger


class PipelineOrchestrator:
    """
    Orchestrates the complete MetaHQ database build pipeline.

    Runs processing for all enabled sources in alphabetical order, then
    combines GEO, SRA, and sample annotations. Supports checkpoint-based
    resumption so a failed run can be restarted from the last completed stage.

    All pipeline behaviour is driven from a ``DataPackageConfig`` loaded from
    ``metahq_setup.yaml``.
    """

    def __init__(self, config: DataPackageConfig):
        self.config = config
        self.specific = config.specific
        self.output_dir = config.data_dir / "processed"
        self.db_path = config.omicidx_path
        self.checkpoints = CheckpointManager(config.checkpoint_dir)
        self.logger = setup_logger("metahq_setup.pipeline")

    def run(
        self,
        start_from: str | None = None,
        end_at: str | None = None,
    ) -> None:
        """
        Execute all pipeline stages in order with checkpointing.

        Already-completed stages (recorded in the checkpoint file) are skipped
        automatically unless the stage has ``use_checkpoint: false`` in config.
        Stages with ``skip: true`` in config are always bypassed. Use
        ``start_from`` to ignore everything before a named stage, or ``end_at``
        to stop after a named stage.

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
                    if end_at and stage_name == end_at:
                        break
                    continue

            stage_cfg = self.config.get_stage_config(stage_name)

            if stage_cfg.skip:
                self.logger.info("Skipping stage (config skip=true): %s", stage_name)
            elif stage_cfg.use_checkpoint and self.checkpoints.is_stage_completed(
                stage_name
            ):
                self.logger.info("Skipping stage (already completed): %s", stage_name)
            else:
                self.logger.info("Starting stage: %s", stage_name)
                stage_fn()
                if stage_cfg.use_checkpoint:
                    self.checkpoints.save_checkpoint(stage_name)
                self.logger.info("Finished stage: %s", stage_name)

            if end_at and stage_name == end_at:
                self.logger.info("Reached end stage, stopping: %s", end_at)
                break

    def _build_stages(self) -> list[tuple[str, Callable]]:
        """Build the ordered list of (stage_name, callable) pairs.

        Processors with ``enabled: false`` in config are omitted entirely.
        Stage names match the keys used in the ``stages`` section of
        ``metahq_setup.yaml``.
        """
        stages: list[tuple[str, Callable]] = []
        stages.append(
            (
                "extract__mondo__relations",
                lambda: Graph.from_obo(MONDO_OBO)
                .relations_matrix()
                .save(MONDO_RELATIONS),
            )
        )
        stages.append(
            (
                "extract__uberon__relations",
                lambda: Graph.from_obo(UBERON_OBO)
                .relations_matrix()
                .save(UBERON_RELATIONS),
            )
        )
        stages.append(
            (
                "build__ontology_search",
                lambda: OntologySearchDbBuilder(
                    mondo=MONDO_NAMES_SYNONYMS,
                    uberon_cl=UBERON_CL_NAMES_SYNONYMS,
                    out_db=ONTOLOGY_SEARCH_DB,
                ).build(),
            )
        )

        for source_name in ProcessorRegistry.list_processors():
            if not self.config.get_processor_config(source_name).enabled:
                self.logger.info("Skipping disabled processor: %s", source_name)
                continue
            processor = ProcessorRegistry.get(source_name)
            stages.append(
                (
                    f"process__{source_name}",
                    lambda p=processor: p.run(output_dir=self.output_dir),
                )
            )

        stages.append(
            (
                "combine__geo",
                lambda: GeoCombiner().combine().clean().save(GEO_COMBINED_BSON),
            )
        )
        stages.append(
            (
                "combine__sra",
                lambda: SraCombiner()
                .combine(db_path=self.db_path)
                .clean()
                .save(SRA_COMBINED_BSON),
            )
        )
        stages.append(
            (
                "combine__sample",
                lambda: SampleCombiner()
                .combine(
                    geo_bson=GEO_COMBINED_BSON,
                    sra_bson=SRA_COMBINED_BSON,
                    db_path=self.db_path,
                )
                .clean(specific=self.specific)
                .save(SAMPLE_COMBINED_BSON),
            )
        )
        stages.append(
            (
                "combine__series",
                lambda: StudyCombiner()
                .combine(
                    sample_combined_bson=SAMPLE_COMBINED_BSON,
                    db_path=self.db_path,
                )
                .clean(specific=self.specific)
                .save(SERIES_COMBINED_BSON),
            )
        )
        stages.append(
            (
                "build__shield_endpoints",
                lambda: ShieldEndpointBuilder(
                    sample_db=SAMPLE_COMBINED_BSON, series_db=SERIES_COMBINED_BSON
                )
                .build()
                .save(SOURCE_COUNT_SHIELD_OUTDIR),
            )
        )

        stages.append(
            (
                "build__metadata",
                lambda: MetadataBuilder(
                    sample_table=OMICIDX_SAMPLE_TABLE, series_table=OMICIDX_SERIES_TABLE
                )
                .build_from_db(
                    sample_db=SAMPLE_COMBINED_BSON, series_db=SERIES_COMBINED_BSON
                )
                .save(sample_outfile=SAMPLE_METADATA, series_outfile=SERIES_METADATA),
            )
        )
        stages.append(
            (
                "build__data_package",
                lambda: DataPackageBuilder(self.config).build(),
            )
        )

        return stages
