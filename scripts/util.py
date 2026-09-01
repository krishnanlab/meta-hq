"""Helper functions only applicable to scripts in this directory."""

from typing import Any

import polars as pl
from metahq_build.config import (
    COL_ACCESSION,
    COL_SOURCE,
    COL_TERM_ID,
    DELIMITER,
    ID_KEY,
)
from metahq_core.util.alltypes import Attribute


def dict_db_to_df(db: dict[str, dict[str, Any]], attribute: Attribute) -> pl.DataFrame:
    """Transform the database from dictionary to polars.DataFrame
    where one row represents a single annotation for a particular
    attribute from a particular source.
    """
    _db = {COL_ACCESSION: [], COL_SOURCE: [], COL_TERM_ID: []}
    for entry, records in db.items():
        if attribute.value not in records:
            continue

        for source, anno in records[attribute.value].items():
            _db[COL_ACCESSION].append(entry)
            _db[COL_SOURCE].append(source)
            _db[COL_TERM_ID].append(anno[ID_KEY])

    return (
        pl.LazyFrame(_db)
        .with_columns(pl.col(COL_TERM_ID).str.split(DELIMITER))
        .explode(COL_TERM_ID, empty_as_null=False)
        .collect(engine="streaming")
    )
