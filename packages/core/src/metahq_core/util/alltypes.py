"""
Custom types for the metahq package

Last updated: 2026-08-12 by Parker Hicks
"""

from collections.abc import KeysView
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

from metahq_core.config import EXTERNAL_LINKS_COL, SOURCES_COL

type FilePath = Path | str
type DictKeys = KeysView

type StringArray = npt.NDArray[np.str_] | list[str]
type IntArray = npt.NDArray[np.int_] | list[int]
type IdArray = StringArray | IntArray

# numpy specific types
type NpStringArray = npt.NDArray[np.str_]
type NpIntArray = npt.NDArray[np.int_]
type NpIntMatrix = npt.NDArray[np.int_]
type NpIdArray = NpStringArray | NpIntArray

# Gemma annotations
type RawGemma = list[dict[str, Any]]
type ParsedGemma = dict[str, dict[str, dict[str, list[str]]]]


class Level(Enum):
    """Supported annotation levels."""

    SAMPLE = "sample"
    SERIES = "series"


class MetadataField(Enum):
    """All supported metadata fields."""

    # levels
    SAMPLE = "sample"
    SERIES = "series"
    # sample-level
    DESCRIPTION = "description"
    SOURCE_NAME_CH1 = "source_name_ch1"
    CHARACTERISTICS_CH1 = "characteristics_ch1"
    SOURCE_NAME_CH2 = "source_name_ch2"
    CHARACTERISTICS_CH2 = "characteristics_ch2"
    # series-level
    TITLE = "title"
    SUMMARY = "summary"
    OVERALL_DESIGN = "overall_design"
    SAMPLE_ID = "sample_id"
    # id fields
    PLATFORM = "platform"
    SRA_RUN = "srr"
    SRA_EXPERIMENT = "srx"
    SRA_SAMPLE = "srs"
    SRA_PROJECT = "srp"
    REFINEBIO_SAMPLE = "refinebio_sample"
    REFINEBIO_EXPERIMENT = "refinebio_experiment"
    # required
    SOURCES = SOURCES_COL
    EXTERNAL_LINKS = EXTERNAL_LINKS_COL


SAMPLE_METADATA_FIELDS = frozenset(
    {
        MetadataField.DESCRIPTION,
        MetadataField.SOURCE_NAME_CH1,
        MetadataField.CHARACTERISTICS_CH1,
        MetadataField.SOURCE_NAME_CH2,
        MetadataField.CHARACTERISTICS_CH2,
    }
)

SERIES_METADATA_FIELDS = frozenset(
    {
        MetadataField.TITLE,
        MetadataField.SUMMARY,
        MetadataField.OVERALL_DESIGN,
        MetadataField.DESCRIPTION,
        MetadataField.SAMPLE_ID,
    }
)

# ID fields that apply to sample level
SAMPLE_ID_FIELDS = frozenset(
    {
        MetadataField.PLATFORM,
        MetadataField.SRA_RUN,
        MetadataField.SRA_EXPERIMENT,
        MetadataField.SRA_SAMPLE,
        MetadataField.SRA_PROJECT,
        MetadataField.REFINEBIO_SAMPLE,
        MetadataField.REFINEBIO_EXPERIMENT,
    }
)

# ID fields that apply to series level
SERIES_ID_FIELDS = frozenset(
    {
        MetadataField.PLATFORM,
        MetadataField.SRA_PROJECT,
        MetadataField.REFINEBIO_EXPERIMENT,
    }
)

# All ID fields (kept for backward compatibility)
ID_FIELDS = SAMPLE_ID_FIELDS | SERIES_ID_FIELDS

REQUIRED_FIELDS = frozenset({MetadataField.SOURCES, MetadataField.EXTERNAL_LINKS})

# conversions
LEVEL_TO_FIELDS: dict[Level, frozenset[MetadataField]] = {
    Level.SAMPLE: SAMPLE_METADATA_FIELDS | SAMPLE_ID_FIELDS,
    Level.SERIES: SERIES_METADATA_FIELDS | SERIES_ID_FIELDS,
}

LEVEL_TO_INDEX_FIELD: dict[Level, MetadataField] = {
    Level.SAMPLE: MetadataField.SAMPLE,
    Level.SERIES: MetadataField.SERIES,
}
