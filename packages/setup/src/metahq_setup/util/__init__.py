"""
Utility modules for metahq-setup.

Provides logging, progress tracking, and checkpointing utilities for
long-running pipeline operations.
"""

from metahq_setup.util.age_groups import AGE_GROUPS, get_age_group
from metahq_setup.util.checkpointing import (
    Checkpoint,
    CheckpointManager,
    create_stage_checkpoint,
    should_skip_stage,
)
from metahq_setup.util.logging import PipelineLogger, get_default_log_file, setup_logger
from metahq_setup.util.progress import (
    ProgressTracker,
    StageProgress,
    parallel_progress,
    track_progress,
)

__all__ = [
    # Age groups
    "AGE_GROUPS",
    "get_age_group",
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
