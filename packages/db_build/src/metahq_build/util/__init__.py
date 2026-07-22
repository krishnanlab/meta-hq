"""
Utility modules for metahq-build.

Provides logging, progress tracking, checkpointing, and archiving utilities for
long-running pipeline operations.
"""

from metahq_build.util.age_groups import AGE_GROUPS, get_age_group
from metahq_build.util.archive import (
    create_database_archive,
    get_archive_path_from_package,
    should_exclude_file,
)
from metahq_build.util.checkpointing import (
    Checkpoint,
    CheckpointManager,
    create_stage_checkpoint,
    should_skip_stage,
)
from metahq_build.util.logging import PipelineLogger, get_default_log_file, setup_logger
from metahq_build.util.progress import (
    ProgressTracker,
    StageProgress,
    parallel_progress,
    track_progress,
)

__all__ = [
    # Age groups
    "AGE_GROUPS",
    "get_age_group",
    # Archive
    "create_database_archive",
    "get_archive_path_from_package",
    "should_exclude_file",
    # Logging
    "setup_logger",
    "PipelineLogger",
    "get_default_log_file",
    # Progress
    "ProgressTracker",
    "StageProgress",
    "track_progress",
    "parallel_progress",
    # Checkpointing
    "Checkpoint",
    "CheckpointManager",
    "create_stage_checkpoint",
    "should_skip_stage",
]
