"""
Class to build and validate a new MetaHQ data package.
"""

import shutil
import sys

from metahq_setup.config.schema import DataPackageConfig
from metahq_setup.util.logging import setup_logger


class DataPackageBuilder:
    """Builds the MetaHQ data package from a DataPackageConfig."""

    def __init__(self, config: DataPackageConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.logger = setup_logger(
            f"metahq_setup.builders.data_package.{self.__class__.__name__}"
        )
        self._directories_built: bool = False

    def build(self):
        """Run the data package build pipeline."""
        self.logger.info("Starting the data package build...")
        self.config.verify_source_files()
        self.build_package_structure()
        self.cp_source_to_dest()
        self.logger.info("Done!")

    def build_package_structure(self):
        """Check and create the data package directory structure."""
        exit_code = self.check_package_outdir()
        self._resolve_mkdir_exit_code(exit_code)
        self.logger.info("Package structure valid. Making directories...")

        if not self.dry_run:
            self.config.output_dir.mkdir(exist_ok=True, parents=True)
        self.logger.info(
            "Made directory: [medium_purple1]%s[/medium_purple1]", self.config.output_dir
        )

        for entry in self.config.structure:
            dest_parent = entry.destination.resolve().parent
            if not dest_parent.exists():
                self.logger.info(
                    "Made directory: [medium_purple1]%s[/medium_purple1]", dest_parent
                )
                if not self.dry_run:
                    dest_parent.mkdir(exist_ok=True, parents=True)
            else:
                self.logger.info("Directory exists: %s. Skipping...", dest_parent)

        self._directories_built = True

    def cp_source_to_dest(self):
        """Copy source files to their destinations."""
        if not self._directories_built:
            self.logger.error(
                "The package structure is not yet built. "
                "Cannot copy source files to their destinations. Exiting...",
            )
            sys.exit(1)

        for entry in self.config.structure:
            if not self.dry_run:
                shutil.copy(entry.source, entry.destination)
            self.logger.info(
                "Copied [cyan]%s[/cyan] to [medium_purple1]%s[/medium_purple1]",
                entry.source,
                entry.destination,
            )

    def check_package_outdir(self) -> int | None:
        """Check outdir existence against overwrite setting.

        Returns:
            (int): Exit code if the check reveals a conflict.
            (None): If state is valid to proceed.
        """
        if self.config.data_package_path.exists() and self.config.overwrite:
            return None
        if self.config.data_package_path.exists() and not self.config.overwrite:
            return 0
        if not self.config.data_package_path.exists():
            return None
        return 1

    def _resolve_mkdir_exit_code(self, code: int | None):
        if code == 0:
            self.logger.info("Outdir exists, but overwrite is set to false.")
        elif code == 1:
            self.logger.error(
                "Data package directory checks failed unexpectedly. Please check your config.\n"
                "Outdir: %s\nOverwrite: %s",
                self.config.data_package_path,
                self.config.overwrite,
            )
