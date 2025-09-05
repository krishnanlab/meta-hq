"""
Command to setup the metahq configuration.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-05
"""

import sys
from pathlib import Path
from string import Template

import yaml

HOME_DIR = Path.home()
METAHQ_DIR = Path.home() / "meta-hq"
CONFIG = METAHQ_DIR / "config.yaml"


# TODO: Remove test func
class Config:
    def __init__(
        self,
        version="0.1.0",
        zenodo_doi="xxxx1",
        data_dir=str(HOME_DIR / "data"),
        logs=str(HOME_DIR / ".logs" / "metahq.log"),
    ):
        self.version: str = version
        self.zenodo_doi: str = zenodo_doi
        self.data_dir: str = data_dir
        self.logs: str = logs

        self.ok_keys: list[str] = ["version", "zenodo_doi", "data_dir", "logs"]

    def check(self):
        """Checks if the meta-hq config exists. Initializes if not."""
        if not CONFIG.exists():
            self.set_default()

        else:
            # check existing
            if not self.is_acceptable_config():
                print("Incorrect configuration detected. Resetting with defaults.")
                self.set_default()

    def is_acceptable_config(self):
        """Checks if config has correct structure."""
        config = self.load_config()

        return sorted(list(config.keys())) == sorted(self.ok_keys)

    def load_config(self) -> dict[str, str]:
        """Loads the meta-hq config file."""
        with open(CONFIG, "r", encoding="utf-8") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as e:
                sys.exit(str(e))

    def load_config_str(self) -> str:
        """Loads the meta-hq config file."""
        with open(CONFIG, "r", encoding="utf-8") as f:
            return f.read()

    def make_config(self) -> dict[str, str]:
        return {
            "version": self.version,
            "zenodo_doi": self.zenodo_doi,
            "data_dir": self.data_dir,
            "logs": self.logs,
        }

    def set_config(self):
        """Updated the meta-hq config file."""
        config = self.load_config_str()

        if self.data_dir == "default":
            self.data_dir = str(Path.home() / "metahq" / "data")

        new_vars = {
            "VERSION": self.version,
            "ZENODO_DOI": self.zenodo_doi,
            "DATA_DIR": str(Path(self.data_dir).resolve()),
        }

        template = Template(config)
        updated = template.substitute(new_vars)

        return yaml.safe_load(updated)

    def set_default(self):
        """Makes a default meta-hq config."""
        print("making default")
        with open(CONFIG, "w", encoding="utf-8") as stream:
            try:
                yaml.safe_dump(self.make_config(), stream)
            except yaml.YAMLError as e:
                sys.exit(str(e))


def test():
    config = Config()
    config.check()
    new = config.set_config()
    print(new)


if __name__ == "__main__":
    test()
