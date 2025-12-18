"""
Command to setup the metahq configuration.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-12-15 by Parker Hicks
"""

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from metahq_core.util.progress import console
from metahq_core.util.supported import get_config_file_no_check

from metahq_cli.logger import setup_logger

if TYPE_CHECKING:
    import logging


CONFIG_FILE: Path = get_config_file_no_check()


class Config:
    """Class to store and assess and create the meta-hq configuration file.

    Attributes:
        version (str):
            A version name of the MetaHQ database (e.g., `'v0.1.0'`).

        zenodo_doi (str):
            A DOI of the MetaHQ database in Zenodo.

        data_dir (str):
            Path to where the MetaHQ data are stored.

        logs (str):
            Path to where the MetaHQ logs are stored.

        ok_keys (list[str]):
            Acceptable keys in the config.
    """

    def __init__(
        self,
        version,
        zenodo_doi,
        data_dir,
        logs,
        logger=None,
        loglevel=20,
        verbose=True,
    ):
        self.version: str = version
        self.zenodo_doi: str = zenodo_doi
        self.data_dir: str = data_dir
        self.logs: str = logs

        if logger is None:
            logger = setup_logger(
                __name__, level=loglevel, log_dir=logs, console=console
            )
        self.logger: logging.Logger = logger
        self.verbose = verbose

        self.ok_keys: list[str] = ["version", "zenodo_doi", "data_dir", "logs"]

    def check(self):
        """Checks if the meta-hq config exists. Initializes if not."""
        if not CONFIG_FILE.exists():
            if self.verbose:
                self.logger.debug("Initializing new config file %s", CONFIG_FILE)
            CONFIG_FILE.touch()

        else:
            if self.verbose:
                self.logger.debug("Config file exists: %s", CONFIG_FILE)
                self.logger.debug("Running config check...")
            # check existing
            if not self.is_acceptable_config():
                if self.verbose:
                    self.logger.warning(
                        "Incorrect configuration detected. Resetting with defaults."
                    )
                self.set_default()

    def is_acceptable_config(self):
        """Checks if config has correct structure."""
        config = self.load_config()

        if self.verbose:
            self.logger.debug("Existing config: %s", config)

        if config is None:
            if self.verbose:
                self.logger.debug("Existing config is empty.")
            return False

        return sorted(list(config.keys())) == sorted(self.ok_keys)

    def load_config(self) -> dict[str, str]:
        """Loads the meta-hq config file."""
        with open(CONFIG_FILE, "r", encoding="utf-8") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as e:
                sys.exit(str(e))

    def load_config_str(self) -> str:
        """Loads the meta-hq config file."""
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return f.read()

    def make_config(self) -> dict[str, str]:
        """Creates the config dictionary"""
        return {
            "version": self.version,
            "zenodo_doi": self.zenodo_doi,
            "data_dir": self.data_dir,
            "logs": self.logs,
        }

    def save_config(self, config: dict[str, str]):
        """Saves a config file.

        Arguments:
            config (dict[str, str]):
                A config with acceptable keys.

        """
        self.logger.info("Saving MetaHQ config to %s", CONFIG_FILE)
        with open(CONFIG_FILE, "w", encoding="utf-8") as stream:
            try:
                yaml.safe_dump(config, stream)
            except yaml.YAMLError as e:
                sys.exit(str(e))
        self.logger.info("Done!")

    def setup(self):
        """Main setup function."""
        self.check()
        new = self.initialize_config()
        self.save_config(new)

    def set_default(self):
        """Makes a default meta-hq config."""
        self.logger.info("Making config with default arguments.")
        self.make_config()

    def initialize_config(self):
        """Initialize the meta-hq config file."""
        config = {
            "version": self.version,
            "zenodo_doi": self.zenodo_doi,
            "data_dir": str(Path(self.data_dir).resolve()),
            "logs": self.logs,
        }

        if self.verbose:
            self.logger.debug("New configuration: %s", config)

        return config

    @property
    def path(self) -> str:
        """Returns `/path/to/config.yaml`"""
        return str(CONFIG_FILE)
