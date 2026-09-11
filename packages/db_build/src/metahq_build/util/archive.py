"""
Archive utilities for creating compressed database packages.

Provides functionality to create tar.gz archives with cross-platform
compatibility, filtering out platform-specific metadata files.
"""

import tarfile
from pathlib import Path
from typing import Callable, Optional


def should_exclude_file(file_path: Path) -> tuple[bool, str]:
    """
    Determine if a file should be excluded from the archive.

    Args:
        file_path: Path to check

    Returns:
        Tuple of (should_exclude, reason)
    """
    name = file_path.name

    # macOS resource fork files
    if name.startswith("._"):
        return True, "macOS resource fork"

    # macOS metadata
    if name == ".DS_Store":
        return True, "macOS metadata"

    # macOS extended attributes directory
    if "__MACOSX" in str(file_path):
        return True, "macOS metadata directory"

    # Windows thumbnail cache
    if name == "Thumbs.db":
        return True, "Windows thumbnail cache"

    # Windows folder settings
    if name == "desktop.ini":
        return True, "Windows folder settings"

    # Temporary files
    if name.endswith("~") or name.endswith(".tmp"):
        return True, "temporary file"

    # Hidden files (optional, but commonly excluded)
    # Uncomment if you want to exclude all hidden files
    # if name.startswith(".") and name not in {".gitkeep"}:
    #     return True, "hidden file"

    return False, ""


def create_tar_filter(verbose_callback: Optional[Callable[[str], None]] = None):
    """
    Create a tarfile filter function that excludes unwanted files.

    Args:
        verbose_callback: Optional callback for logging excluded files

    Returns:
        Filter function for tarfile.add()
    """

    def tar_filter(tarinfo):
        """Filter out platform-specific and temporary files."""
        path = Path(tarinfo.name)
        should_exclude, reason = should_exclude_file(path)

        if should_exclude:
            if verbose_callback:
                verbose_callback(f"  Skipping: {tarinfo.name} ({reason})")
            return None

        return tarinfo

    return tar_filter


def create_database_archive(
    package_dir: Path,
    output_path: Path,
    verbose: bool = False,
    verbose_callback: Optional[Callable[[str], None]] = None,
) -> dict:
    """
    Create a compressed tar.gz archive of a database package.

    Args:
        package_dir: Directory containing the database package
        output_path: Path for the output archive file
        verbose: Enable verbose output
        verbose_callback: Optional callback for verbose messages

    Returns:
        Dictionary with archive metadata (path, size_bytes, size_mb)

    Raises:
        FileNotFoundError: If package_dir doesn't exist
        NotADirectoryError: If package_dir is not a directory
        PermissionError: If insufficient permissions
        OSError: If archive creation fails
    """
    # Validate input
    if not package_dir.exists():
        raise FileNotFoundError(f"Package directory does not exist: {package_dir}")

    if not package_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {package_dir}")

    # Create parent directory for output if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create filter with optional callback
    tar_filter = create_tar_filter(
        verbose_callback=verbose_callback if verbose else None
    )

    # Create the tar.gz archive
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(
            package_dir,
            arcname=package_dir.name,
            filter=tar_filter,
        )

    # Get file statistics
    stat = output_path.stat()
    size_bytes = stat.st_size
    size_mb = size_bytes / (1024 * 1024)

    return {
        "path": output_path,
        "size_bytes": size_bytes,
        "size_mb": size_mb,
    }


def get_archive_path_from_package(package_dir: Path) -> Path:
    """
    Generate default archive path from package directory.

    Args:
        package_dir: Package directory path

    Returns:
        Archive path with .tar.gz extension
    """
    return package_dir.with_suffix(".tar.gz")
