"""
Downloader for the MetaHQ database stored on Zenodo.
Implemented in the `metahq setup` CLI command.

Author: Parker Hicks
Date: 2025-11-20

Last updated: 2025-11-20 by Parker Hicks
"""


class Downloader:
    """Downloader for the MetaHQ database stored on Zenodo.

    Attributes
    ----------

    doi: str
        Zenodo DOI for the MetaHQ databse.

    logger: logging.Logger
        Logger for process transparency.

    verbose: bool
        Indicates if logs should be passed to stdout.


    Methods
    -------

    check_exists()
        Check if the Zenodo DOI and metahq.tar.gz file exit.

    get()
        Main function.

    get_stats()
        Retrieve file stats of the MetaHQ database.

    Raises
    ------

    """

    def __init__(self):
        pass

    def check_exists(self):
        """Check if the Zenodo DOI and file exist."""

    def get(self):
        """Main function."""

    def get_stats(self):
        """Retrieve file stats."""
