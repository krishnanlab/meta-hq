"""
Progress tracking utilities for long-running operations.

Provides progress bars and tracking for pipeline stages, data processing,
and other operations that process large amounts of data.
"""

from typing import Any, Callable, Iterable

from tqdm import tqdm


class ProgressTracker:
    """
    Wraps tqdm to provide consistent progress tracking across the package.

    Attributes:
        bar (tqdm):
            Underlying tqdm progress bar
        desc (str):
            Description of the operation being tracked
    """

    def __init__(
        self,
        total: int | None = None,
        desc: str = "Processing",
        unit: str = "items",
        disable: bool = False,
    ):
        """
        Initialize progress tracker.

        Arguments:
            total (int | None):
                Total number of items to process. None for unknown total
            desc (str):
                Description to display with the progress bar
            unit (str):
                Unit of items being processed (e.g., "files", "samples")
            disable (bool):
                If True, disable progress bar (useful for silent mode)
        """
        self.desc = desc
        self.bar = tqdm(
            total=total,
            desc=desc,
            unit=unit,
            disable=disable,
            leave=True,
            ncols=100,
        )

    def update(self, n: int = 1) -> None:
        """
        Update progress by n items.

        Arguments:
            n (int):
                Number of items to increment progress by
        """
        self.bar.update(n)

    def set_description(self, desc: str) -> None:
        """
        Update the description text.

        Arguments:
            desc (str):
                New description to display
        """
        self.desc = desc
        self.bar.set_description(desc)

    def set_postfix(self, **kwargs: Any) -> None:
        """
        Set postfix statistics to display.

        Arguments:
            **kwargs:
                Key-value pairs to display (e.g., errors=5, warnings=12)
        """
        self.bar.set_postfix(**kwargs)

    def close(self) -> None:
        """Close the progress bar."""
        self.bar.close()

    def __enter__(self) -> "ProgressTracker":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


def track_progress(
    iterable: Iterable,
    desc: str = "Processing",
    total: int | None = None,
    unit: str = "items",
    disable: bool = False,
) -> Iterable:
    """
    Wrap an iterable with a progress bar.

    Arguments:
        iterable (Iterable):
            Items to iterate over
        desc (str):
            Description to display with the progress bar
        total (int | None):
            Total number of items. If None, tries to determine from iterable
        unit (str):
            Unit of items being processed
        disable (bool):
            If True, disable progress bar

    Returns:
        (Iterable): Wrapped iterable with progress tracking

    Examples:
        >>> for item in track_progress(items, desc="Processing samples"):
        ...     process(item)

        >>> files = ["file1.txt", "file2.txt", "file3.txt"]
        >>> for file in track_progress(files, desc="Reading files", unit="files"):
        ...     read_file(file)
    """
    return tqdm(
        iterable,
        desc=desc,
        total=total,
        unit=unit,
        disable=disable,
        leave=True,
        ncols=100,
    )


def parallel_progress(
    func: Callable,
    items: list,
    desc: str = "Processing",
    n_workers: int = 4,
    disable: bool = False,
) -> list:
    """
    Process items in parallel with a progress bar.

    Arguments:
        func (Callable):
            Function to apply to each item
        items (list):
            List of items to process
        desc (str):
            Description for the progress bar
        n_workers (int):
            Number of parallel workers
        disable (bool):
            If True, disable progress bar

    Returns:
        (list): Results from processing each item

    Examples:
        >>> def process_sample(sample_id):
        ...     # Process sample
        ...     return result
        >>> results = parallel_progress(
        ...     process_sample,
        ...     sample_ids,
        ...     desc="Processing samples",
        ...     n_workers=8
        ... )
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    results = [None] * len(items)
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        # Submit all tasks
        future_to_idx = {
            executor.submit(func, item): idx for idx, item in enumerate(items)
        }

        # Track progress as tasks complete
        with ProgressTracker(
            total=len(items), desc=desc, unit="items", disable=disable
        ) as tracker:
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    results[idx] = e
                tracker.update(1)

    return results


class StageProgress:
    """
    Track progress through multiple pipeline stages.

    Attributes:
        stages (list[str]):
            Names of all pipeline stages
        current_stage_idx (int):
            Index of the current stage
        disable (bool):
            Whether to disable progress output
    """

    def __init__(self, stages: list[str], disable: bool = False):
        """
        Initialize stage progress tracker.

        Arguments:
            stages (list[str]):
                List of stage names in order
            disable (bool):
                If True, disable progress output
        """
        self.stages = stages
        self.current_stage_idx = 0
        self.disable = disable
        self._overall_bar: tqdm | None = None
        self._stage_bar: tqdm | None = None

    def start(self) -> None:
        """Start tracking overall pipeline progress."""
        if not self.disable:
            self._overall_bar = tqdm(
                total=len(self.stages),
                desc="Pipeline Progress",
                unit="stage",
                position=0,
                leave=True,
            )

    def start_stage(self, stage_name: str, total_items: int | None = None) -> None:
        """
        Start a new pipeline stage.

        Arguments:
            stage_name (str):
                Name of the stage being started
            total_items (int | None):
                Number of items to process in this stage
        """
        if not self.disable:
            if self._stage_bar is not None:
                self._stage_bar.close()

            self._stage_bar = tqdm(
                total=total_items,
                desc=f"  {stage_name}",
                unit="items",
                position=1,
                leave=False,
            )

    def update_stage(self, n: int = 1) -> None:
        """
        Update progress within the current stage.

        Arguments:
            n (int):
                Number of items to increment by
        """
        if self._stage_bar is not None:
            self._stage_bar.update(n)

    def end_stage(self) -> None:
        """Complete the current stage and move to the next."""
        if not self.disable:
            if self._stage_bar is not None:
                self._stage_bar.close()
                self._stage_bar = None

            if self._overall_bar is not None:
                self._overall_bar.update(1)

        self.current_stage_idx += 1

    def finish(self) -> None:
        """Finish tracking and close all progress bars."""
        if self._stage_bar is not None:
            self._stage_bar.close()
        if self._overall_bar is not None:
            self._overall_bar.close()

    def __enter__(self) -> "StageProgress":
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.finish()
