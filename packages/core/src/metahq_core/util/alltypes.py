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
    
class SampleMetadataField(Enum):
    """Supported sample-level metadata fields."""
    DESCRIPTION = "description"
    SOURCE_NAME_CH1 = "source_name_ch1"
    CHARACTERISTICS_CH1 = "characteristics_ch1"
    SOURCE_NAME_CH2 = "source_name_ch2"
    CHARACTERISTICS_CH2 = "characteristics_ch2"

class SeriesMetadataField(Enum):
    """Supported series-level metadata fields."""
    TITLE = "title"
    SUMMARY = "summary"
    OVERALL_DESIGN = "overall_design"
    DESCRIPTION = "description"
    SAMPLE_ID = "sample_id"

class IDField(Enum):
    """Supported ID fields."""
    PLATFORM = "platform"
    SRA_EXPERIMENT = "srx"
    SRA_SAMPLE = "srs"
    SRA_PROJECT = "srp"
    REFINEBIO_SAMPLE = "refinebio_sample"
    REFINEBIO_EXPERIMENT = "refinebio_experiment"

class RequiredField(Enum):
    """Fields required in queries from MetaHQ."""
    SOURCES = SOURCES_COL
    EXTERNAL_LINKS = EXTERNAL_LINKS_COL


# Supported metadata fields
MetadataField = Enum(
    "MetadataField", {
        **{e.name: e.value for e in SampleMetadataField},
        **{e.name: e.value for e in SeriesMetadataField},
        **{e.name: e.value for e in IDField},
    }
)
