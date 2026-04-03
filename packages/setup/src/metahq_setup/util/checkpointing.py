"""
Checkpointing utilities for pipeline state management.

Enables saving and resuming pipeline execution state, which is critical
for long-running database builds that may fail partway through.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any


class Checkpoint:
    """
    Represents a single checkpoint in pipeline execution.

    Attributes:
        stage_name (str):
            Name of the completed stage
        timestamp (str):
            ISO format timestamp of checkpoint creation
        metadata (dict[str, Any]):
            Additional metadata about the checkpoint
    """

    def __init__(
        self,
        stage_name: str,
        timestamp: str | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """
        Create a checkpoint.

        Arguments:
            stage_name (str):
                Name of the pipeline stage that completed
            timestamp (str | None):
                ISO timestamp. If None, uses current time
            metadata (dict[str, Any] | None):
                Additional metadata to store with checkpoint
        """
        self.stage_name = stage_name
        self.timestamp = timestamp or datetime.now().isoformat()
        self.metadata = metadata or {}

    def to_dict(self) -> dict[str, Any]:
        """
        Convert checkpoint to dictionary.

        Returns:
            (dict[str, Any]): Checkpoint as dictionary
        """
        return {
            "stage_name": self.stage_name,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Checkpoint":
        """
        Create checkpoint from dictionary.

        Arguments:
            data (dict[str, Any]):
                Dictionary containing checkpoint data

        Returns:
            (Checkpoint): Reconstructed checkpoint
        """
        return cls(
            stage_name=data["stage_name"],
            timestamp=data["timestamp"],
            metadata=data.get("metadata", {}),
        )


class CheckpointManager:
    """
    Manages pipeline checkpoints for fault tolerance.

    Handles saving, loading, and querying pipeline execution state
    to enable resuming from failures.

    Attributes:
        checkpoint_dir (Path):
            Directory where checkpoint files are stored
        checkpoint_file (Path):
            Path to the main checkpoint state file
        checkpoints (list[Checkpoint]):
            List of all checkpoints in order
    """

    def __init__(self, checkpoint_dir: Path):
        """
        Initialize checkpoint manager.

        Arguments:
            checkpoint_dir (Path):
                Directory to store checkpoint files
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "pipeline_state.json"
        self.checkpoints: list[Checkpoint] = []
        self._load_checkpoints()

    def save_checkpoint(
        self, stage_name: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """
        Save a checkpoint for a completed stage.

        Arguments:
            stage_name (str):
                Name of the completed stage
            metadata (dict[str, Any] | None):
                Additional metadata to store (e.g., statistics, file paths)
        """
        checkpoint = Checkpoint(stage_name=stage_name, metadata=metadata)
        self.checkpoints.append(checkpoint)
        self._write_checkpoints()

    def get_last_checkpoint(self) -> Checkpoint | None:
        """
        Get the most recent checkpoint.

        Returns:
            (Checkpoint | None): Last checkpoint, or None if no checkpoints exist
        """
        if not self.checkpoints:
            return None
        return self.checkpoints[-1]

    def get_completed_stages(self) -> list[str]:
        """
        Get list of all completed stage names.

        Returns:
            (list[str]): Names of completed stages in order
        """
        return [cp.stage_name for cp in self.checkpoints]

    def is_stage_completed(self, stage_name: str) -> bool:
        """
        Check if a stage has been completed.

        Arguments:
            stage_name (str):
                Name of the stage to check

        Returns:
            (bool): True if stage has a checkpoint, False otherwise
        """
        return stage_name in self.get_completed_stages()

    def get_checkpoint_for_stage(self, stage_name: str) -> Checkpoint | None:
        """
        Get the checkpoint for a specific stage.

        Arguments:
            stage_name (str):
                Name of the stage

        Returns:
            (Checkpoint | None): Checkpoint for the stage, or None if not found
        """
        for checkpoint in self.checkpoints:
            if checkpoint.stage_name == stage_name:
                return checkpoint
        return None

    def clear_checkpoints(self) -> None:
        """Clear all checkpoints and reset state."""
        self.checkpoints = []
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()

    def clear_from_stage(self, stage_name: str) -> None:
        """
        Clear all checkpoints from a specific stage onwards.

        Arguments:
            stage_name (str):
                Name of the stage to start clearing from
        """
        # Find index of the stage
        stage_idx = None
        for idx, cp in enumerate(self.checkpoints):
            if cp.stage_name == stage_name:
                stage_idx = idx
                break

        if stage_idx is not None:
            # Keep only checkpoints before this stage
            self.checkpoints = self.checkpoints[:stage_idx]
            self._write_checkpoints()

    def _load_checkpoints(self) -> None:
        """Load checkpoints from disk."""
        if not self.checkpoint_file.exists():
            return

        try:
            with open(self.checkpoint_file, "r") as f:
                data = json.load(f)
                self.checkpoints = [
                    Checkpoint.from_dict(cp_data) for cp_data in data["checkpoints"]
                ]
        except (json.JSONDecodeError, KeyError) as e:
            # If checkpoint file is corrupted, start fresh
            self.checkpoints = []

    def _write_checkpoints(self) -> None:
        """Write checkpoints to disk."""
        data = {
            "version": "1.0",
            "checkpoints": [cp.to_dict() for cp in self.checkpoints],
        }
        with open(self.checkpoint_file, "w") as f:
            json.dump(data, f, indent=2)

    def export_state(self, output_path: Path) -> None:
        """
        Export checkpoint state to a file.

        Arguments:
            output_path (Path):
                Path to export checkpoint state to
        """
        shutil.copy(self.checkpoint_file, output_path)

    def import_state(self, input_path: Path) -> None:
        """
        Import checkpoint state from a file.

        Arguments:
            input_path (Path):
                Path to import checkpoint state from
        """
        shutil.copy(input_path, self.checkpoint_file)
        self._load_checkpoints()

    def get_summary(self) -> str:
        """
        Get a human-readable summary of checkpoint state.

        Returns:
            (str): Summary of all checkpoints
        """
        if not self.checkpoints:
            return "No checkpoints found"

        lines = [f"Total checkpoints: {len(self.checkpoints)}", ""]
        for i, cp in enumerate(self.checkpoints, 1):
            lines.append(f"{i}. {cp.stage_name}")
            lines.append(f"   Time: {cp.timestamp}")
            if cp.metadata:
                lines.append(f"   Metadata: {cp.metadata}")

        return "\n".join(lines)


def create_stage_checkpoint(
    checkpoint_dir: Path,
    stage_name: str,
    stats: dict[str, Any] | None = None,
) -> None:
    """
    Convenience function to create a checkpoint for a stage.

    Arguments:
        checkpoint_dir (Path):
            Directory where checkpoints are stored
        stage_name (str):
            Name of the completed stage
        stats (dict[str, Any] | None):
            Statistics or metadata about the stage
    """
    manager = CheckpointManager(checkpoint_dir)
    manager.save_checkpoint(stage_name, metadata=stats)


def should_skip_stage(checkpoint_dir: Path, stage_name: str) -> bool:
    """
    Check if a stage can be skipped based on checkpoint.

    Arguments:
        checkpoint_dir (Path):
            Directory where checkpoints are stored
        stage_name (str):
            Name of the stage to check

    Returns:
        (bool): True if stage has been completed and can be skipped
    """
    manager = CheckpointManager(checkpoint_dir)
    return manager.is_stage_completed(stage_name)
