"""
Downloader for the MetaHQ database stored on Zenodo.
Implemented in the `metahq setup` CLI command.

Author: Parker Hicks
Date: 2025-11-20

Last updated: 2025-11-21 by Parker Hicks
"""

import shutil
import sys
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import click
import requests
from metahq_core.util.io import checkdir
from metahq_core.util.progress import get_console, progress_bar

from metahq_cli.logger import setup_logger
from metahq_cli.util.supported import (
    default_data_dir,
    metahq_dois,
    zenodo_files_dir,
    zenodo_records_url,
)

if TYPE_CHECKING:
    import logging


DEFAULT_OUTDIR: Path = default_data_dir()


@dataclass
class FileConfig:
    """Storage for MetaHQ database record information.

    Attributes
    ----------
    doi: str
        A valid MetaHQ Zenodo DOI.

    filename: str
        The name of the MetaHQ database .tar.gz file in
        the files for the Zenodo DOI.

    url: str
        The full URL to the MetaHQ database file in Zenodo.

    version: str
        The version of the MetaHQ database.

    outdir: Path
        Path to the output directory in the user's system.

    filesize: int | None:
        Size of the databse file in bytes.

    Properties
    ----------
    outfile: Path
        Path to the output file.

    size_mb: float
        File size in MB.

    """

    doi: str
    filename: str
    url: str
    version: str
    outdir: Path
    filesize: int | None = None

    def make_outdir(self):
        """Check that outdir exists."""
        checkdir(self.outdir)

    @property
    def filename_stemmed(self) -> str:
        """Return file name without any extensions."""
        return str(self.filename).split(".", maxsplit=1)[0]

    @property
    def outfile(self) -> Path:
        """Return path to the database file on the user's system."""
        return self.outdir / self.filename

    @property
    def size_mb(self) -> float:
        """Return the filesize in MB."""
        if self.filesize is None:
            raise AttributeError("filesize not set.")

        return round(self.filesize / (1024**2), 2)


class Downloader:
    """Downloader for the MetaHQ database stored on Zenodo.

    Attributes
    ----------
    doi: str
        Zenodo DOI for the MetaHQ databse.

    outdir: str
        Path to the output directory in the user's system.

    logger: logging.Logger
        Logger for process transparency.

    verbose: bool
        Indicates if logs should be passed to stdout.


    Methods
    -------
    extract()
        Extracts the MetaHQ database .tar.gz file.

    get()
        Downloads the MetaHQ database from Zenodo.

    get_stats()
        Check if the Zenodo DOI and metahq.tar.gz file exists
        and retrieve requests stats.

    Example
    -------
    >>> from metahq_cli.downloader import Downloader
    >>> downloader = Downloader('17663087')
    >>> downloader.get()
    >>> downloader.extract()

    """

    def __init__(
        self,
        doi,
        outdir: str | Path = DEFAULT_OUTDIR,
        logger=None,
        loglevel=20,
        logdir=Path("."),
        verbose=True,
    ):
        self.config: FileConfig = self._make_config(doi, outdir)

        if logger is None:
            logger = setup_logger(
                __name__, level=loglevel, log_dir=logdir, console=get_console()
            )
        self.logger: logging.Logger = logger
        self.verbose: bool = verbose

        self._use_progress: bool = True

    def check_outdir_exists(self):
        """Check if the data directory exists."""
        if self.config.outdir.exists():
            if click.confirm(
                f"Data directory: {self.config.outdir} exists. Overwrite?",
                default=False,
            ):
                self.logger.info("Removing existing data directory...")
                shutil.rmtree(self.config.outdir)
                self.config.make_outdir()
            else:
                self.logger.info("Keeping existing data directory.")
                sys.exit("Terminating...")
        else:
            self.config.make_outdir()

    def extract(self):
        """Extract the tar archive."""
        if self.verbose:
            self.logger.info("Decompressing...")

        self._extract()

        tar_dir = self.config.outdir / self.config.filename_stemmed
        self._move_tar_contents(base_dir=self.config.outdir, tar_dir=tar_dir)

        if self.verbose:
            self.logger.info("Saved the MetaHQ database to %s.", self.config.outdir)

    def get(self):
        """Downloads the database .tar.gz file from Zenodo."""
        self.check_outdir_exists()

        try:
            self.get_stats()
            self._download()

        except requests.exceptions.ConnectionError:
            self._raise_connection_error()

        except requests.exceptions.Timeout:
            self._raise_timeout_error()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self._raise_404_error()
            elif e.response.status_code == 403:
                self._raise_403_error()
            else:
                self._raise_http_error(e)

        except PermissionError:
            self._raise_permissions_error()

        except KeyboardInterrupt:
            self._raise_keyboard_interrupt()

        except Exception as e:
            self._raise_general_exception(e)

    def get_stats(self):
        """Check if file exists and retrieve file stats."""
        if self.verbose:
            self.logger.debug("Valid DOI: %s", self.config.doi)
            self.logger.debug("Downloading from URL: %s", self.config.url)
            self.logger.info("Checking file availability...")

        head = requests.head(self.config.url, allow_redirects=True, timeout=10)
        self.config.filesize = int(head.headers.get("content-length", 0))

        if self.verbose:
            self.logger.debug("Request status: %s", head.status_code)
            self.logger.debug("Request headers: %s", dict(head.headers))
            self.logger.debug("Final URL: %s", head.url)
            self.logger.debug("File size: %s MB", self.config.size_mb)

        if self.config.filesize == 0:
            self._use_progress = False

            if self.verbose:
                self.logger.warning("Could not determine file size.")
                self.logger.info("Downloading without progress bar.")

    @property
    def database_version(self) -> str:
        """Return the MetaHQ database version."""
        return self.config.version

    # ========================================
    # ======  downloaders
    # ========================================

    def _download(self):
        if self.verbose:
            self.logger.debug("Show progress bar: %s", self._use_progress)

        if self._use_progress:
            self._download_with_progress()
        else:
            self._download_no_progress()

    def _download_with_progress(self):
        response = requests.get(
            self.config.url, stream=True, allow_redirects=True, timeout=30
        )
        with progress_bar(padding="    ") as progress:
            task = progress.add_task("Downloading", total=self.config.filesize)

            with open(self.config.outfile, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    progress.update(task, advance=len(chunk))

    def _download_no_progress(self):
        response = requests.get(
            self.config.url, stream=True, allow_redirects=True, timeout=30
        )
        with open(self.config.outfile, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    # ========================================
    # ======  tar extractors
    # ========================================

    def _extract(self):
        with tarfile.open(self.config.outfile, mode="r:gz") as tar:
            tar.extractall(
                path=self.config.outdir, members=tar.getmembers(), filter="data"
            )

    def _move_tar_contents(self, base_dir: Path, tar_dir: Path):
        target_dir = base_dir

        if self.verbose:
            self.logger.debug("Moving contents from %s to %s", tar_dir, target_dir)

        for item in tar_dir.iterdir():
            shutil.move(str(item), str(target_dir / item.name))

        if self.verbose:
            self.logger.debug("Deleting %s", tar_dir)

        shutil.rmtree(tar_dir)
        (base_dir / self.config.filename).unlink()

    # ========================================
    # ======  helpers
    # ========================================

    def _make_config(self, doi: str, outdir: str | Path) -> FileConfig:
        info: dict[str, str] = metahq_dois(doi)

        # looks like https://zenodo.org/records/<doi>/files/<filename>
        url: str = self._make_url(doi, info["filename"])

        return FileConfig(
            doi=doi,
            filename=info["filename"],
            outdir=Path(outdir),
            url=url,
            version=info["version"],
        )

    def _make_url(self, doi: str, filename: str) -> str:
        base_url = zenodo_records_url()
        files_dir = zenodo_files_dir()

        return f"{base_url}/{doi}/{files_dir}/{filename}"

    # ========================================
    # ======  error messages
    # ========================================

    def _raise_connection_error(self):
        self.logger.error(
            "Could not connect to Zenodo. Check your internet connection.",
        )
        sys.exit(1)

    def _raise_general_exception(self, e):
        self.logger.error("Unexpected error occurred: %s", e)
        sys.exit(1)

    def _raise_http_error(self, e):
        self.logger.error("HTTP %s: %s", e.response.status_code, e)
        sys.exit(1)

    def _raise_keyboard_interrupt(self):
        self.logger.error("Download cancelled by user.")
        if self.config.outfile.exists():
            self.config.outfile.unlink()
            self.logger.info("Removed partial download: %s", self.config.outfile)
        sys.exit(130)

    def _raise_permissions_error(self):
        self.logger.error(
            "Permission denied writing to '%s'. Check file permissions.",
            self.config.outfile,
        )
        sys.exit(1)

    def _raise_timeout_error(self):
        self.logger.error(
            "Download timed out. The server may be slow or unreachable.",
        )
        sys.exit(1)

    def _raise_403_error(self):
        self.logger.error(
            "Access forbidden (403). The file may be restricted.",
        )
        sys.exit(1)

    def _raise_404_error(self):
        self.logger.error(
            "File not found (404). Check that the URL is correct (hint: use --loglevel debug).",
        )
        sys.exit(1)
