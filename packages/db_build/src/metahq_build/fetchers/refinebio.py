"""
fetch_refinebio_ids.py

Fetches all experiment accession codes and internal numeric IDs from the
refine.bio API and writes them to a parquet file.
"""

import sys
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl
import requests

from metahq_build.config import DELIMITER
from metahq_build.config.config import OMICIDX_DB
from metahq_build.metadata.sra_runs import srr_to_geo
from metahq_build.util.logging import setup_logger


def checkdir(path: Path | str, is_file: bool = False) -> Path:
    """Checks if a directory exists. Creates it if not."""
    if not isinstance(path, Path):
        path = Path(path)

    if is_file:
        path = path.resolve().parents[0]

    if not path.exists():
        path.mkdir(exist_ok=True, parents=True)

    return path


@dataclass(slots=True)
class AccessionEntry:
    """Stores metadata for a single accession entry in refine.bio"""

    internal_id: str
    samples: list[str]


class RefineBioRecords:
    """Stores records fetched from refine.bio."""

    def __init__(self):
        self._records: dict[str, AccessionEntry] = {}

    def update(self, accession: str, internal_id: str, samples: list[str]) -> None:
        """Adds a new accession, internal ID, and samples entry."""
        entry = AccessionEntry(internal_id, samples)
        self._records[accession] = entry

    def pl(self) -> pl.DataFrame:
        """Returns records as a polars DataFrame."""
        return pl.DataFrame(
            {
                "accession": list(self._records.keys()),
                "internal_id": [e.internal_id for e in self._records.values()],
                "samples": [e.samples for e in self._records.values()],
            }
        ).with_columns(pl.col("samples").list.join(DELIMITER).alias("samples"))

    def update_batch(
        self, accessions: set[str], internal_ids: set[str], samples: list[list[str]]
    ) -> None:
        """Update records in batch."""
        if not len(accessions) == len(internal_ids) == len(samples):
            raise ValueError("All entry sizes should match for batch records update.")

        for accession, internal_id, sample_list in zip(
            accessions, internal_ids, samples
        ):
            self._records[accession] = AccessionEntry(internal_id, sample_list)

    def __contains__(self, item):
        return item in self._records

    def __len__(self):
        return len(self._records)


class RefineBioFetcher:
    """Fetcher for refine.bio sample and study (experiment) IDs."""

    def __init__(
        self,
        base_url="https://api.refine.bio/v1/experiments/",
        page_size=100,
        total=60_000,  # states over 40k in refinebio. Pad to ensure updates are collected.
        timeout=60,
        retry_wait=5,
        max_retries=5,
    ):

        self.base_url: str = base_url
        self.page_size: int = page_size
        self.total: int = total
        self.timeout: int = timeout
        self.retry_wait: int = retry_wait
        self.max_retries: int = max_retries

        self._records = RefineBioRecords()
        self.logger = setup_logger(__name__)

    def get_page(self, session, offset):
        """Fetches a single page of study IDs through refine.bio's REST API."""
        params = {"limit": self.page_size, "offset": offset}
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = session.get(self.base_url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    self.logger.error(
                        "Failed after %d attempts: %s", self.max_retries, exc
                    )
                    sys.exit(1)
                self.logger.warning(
                    "Attempt %d failed: %s. Retrying in %d",
                    attempt,
                    exc,
                    self.retry_wait,
                )
                time.sleep(self.retry_wait)

        self.logger.error("Failed after %d attempts.", self.max_retries)
        sys.exit(1)

    def fetch(self, offset: int = 0, headers: dict | None = None) -> "RefineBioFetcher":
        """Executes the fetch pipeline."""
        session = requests.Session()

        if isinstance(headers, dict):
            session.headers.update(headers)

        fetched = 0
        na = 0
        existing = 0
        empty = 0
        while offset < self.total:
            data = self.get_page(session, offset)

            if data is None:
                continue

            for exp in data["results"]:

                if ("accession_code" not in exp) or ("processed_samples" not in exp):
                    fetched += 1
                    na += 1
                    continue

                if exp["accession_code"] in self._records:
                    fetched += 1
                    existing += 1
                    continue

                if len(exp["processed_samples"]) > 0:
                    self._records.update(
                        exp["accession_code"],
                        exp["id"],
                        exp["processed_samples"],
                    )
                else:
                    empty += 1

                fetched += 1

            offset += self.page_size
            self.logger.info("Fetched %d/%d", fetched, self.total)

        self.logger.info("Fetched %d total entries", fetched)
        self.logger.info("Recorded %d valid entries", len(self._records))
        self.logger.info("Skipped %d existing entries", existing)
        self.logger.info("Skipped %d na entries", na)
        self.logger.info("Skipped %d empty entries", empty)

        return self

    def expand_geo(self, db_path: Path = OMICIDX_DB) -> pl.DataFrame:
        """Map SRA IDs in refine.bio to GEO IDs through OmicIDX."""
        order = ["refinebio_sample", "refinebio_experiment", "gsm", "gse"]

        df = self._records.pl()

        sra_ids = (
            df.filter(~pl.col("samples").str.starts_with("GSE"))
            .with_columns(pl.col("samples").str.split(DELIMITER).alias("samples"))
            .explode("samples")
            .filter(~pl.col("samples").str.starts_with("GSM"))["samples"]
            .to_list()
        )

        self.logger.info(
            "Expanding SRA IDs to GEO IDs. This may take a couple minutes."
        )
        sra2geo = srr_to_geo(sra_ids, db_path).drop("srx")

        sra2geo = sra2geo.rename(
            {"srr": "refinebio_sample", "srp": "refinebio_experiment"}
        ).select(order)

        geo = (
            df.select(["accession", "samples"])
            .filter(pl.col("accession").str.starts_with("GSE"))
            .with_columns(pl.col("samples").str.split(DELIMITER).alias("samples"))
            .explode("samples")
            .rename(
                {"accession": "refinebio_experiment", "samples": "refinebio_sample"}
            )
            .with_columns(
                [
                    pl.col("refinebio_experiment").alias("gse"),
                    pl.col("refinebio_sample").alias("gsm"),
                ]
            )
            .select(order)
        )

        return pl.concat([geo, sra2geo], how="vertical")

    def save(self, outfile: Path | str):
        """Save fetched IDs to parquet."""
        # get unique
        _ = checkdir(outfile, is_file=True)
        df = self._records.pl()
        df.write_parquet(outfile)

        self.logger.info("Done. %d experiment IDs written to %s", df.height, outfile)

    @property
    def records(self) -> RefineBioRecords:
        """Return records."""
        return self._records

    @records.setter
    def records(self, val: RefineBioRecords) -> None:
        """Records setter."""
        if not isinstance(val, RefineBioRecords):
            raise TypeError(f"Expected RefineBioRecords. Got {type(val)}")
        self._records = val

    @classmethod
    def from_parquet(
        cls,
        file: Path | str,
        accession_col: str = "accession",
        internal_id_col: str = "internal_id",
        samples_col: str = "samples",
        delimiter: str = DELIMITER,
        **kwargs,
    ):
        """Initialize records from existing parquet.

        Example:

            >>> existing = pl.read_parquet('ids.parquet')
            >>> existing
            ┌─────────────┬─────────────┬─────────────────────────────────┐
            │ accession   ┆ internal_id ┆ samples                         │
            │ ---         ┆ ---         ┆ ---                             │
            │ str         ┆ i64         ┆ str                             │
            ╞═════════════╪═════════════╪═════════════════════════════════╡
            │ GSE114447   ┆ 6153        ┆ ERR2534078|ERR2534079|ERR25340… │
            │ GSE63129    ┆ 26635       ┆ GSM154585|GSM154576|GSM154613|… │
            │ SRP010036   ┆ 63518       ┆ SRR2149299|SRR2149297|SRR21493… │
            │ GSE66925    ┆ 38947       ┆ GSM957748|GSM957759|GSM957758|… │
            │ SRP036053   ┆ 22567       ┆ GSM1056879|GSM1056882|GSM10568… │
            │ E-MEXP-2246 ┆ 6193        ┆ E-TABM-113-OWB_R1|E-TABM-113-O… │
            │ GSE14863    ┆ 26685       ┆ SRR7869060|SRR7869045|SRR78690… │
            │ GSE3143     ┆ 38974       ┆ SRR1274691|SRR1274692|SRR12746… │
            │ GSE51438    ┆ 38975       ┆ GSM782982|GSM783033|GSM783070|… │
            │ GSE58887    ┆ 38977       ┆ GSM190733|GSM190735|GSM190747|… │
            └─────────────┴─────────────┴─────────────────────────────────┘
            >>> fetcher = RefineBioFetcher.from_parquet(existing)
        """
        df = pl.read_parquet(file)
        accessions = set(df[accession_col].to_list())
        internal_ids = set(df[internal_id_col].to_list())
        samples = df[samples_col].str.split(delimiter).to_list()

        records = RefineBioRecords()
        records.update_batch(
            accessions=accessions,
            internal_ids=internal_ids,
            samples=samples,
        )

        fetcher = cls(**kwargs)
        fetcher.records = records

        fetcher.logger.info("Initialized with %d records", len(records))

        return fetcher
