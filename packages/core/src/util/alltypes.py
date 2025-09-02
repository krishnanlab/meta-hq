from collections.abc import KeysView
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

type FilePath = Path | str
type DictKeys = KeysView

type StringArray = npt.NDArray[np.str_] | list[str]
type IntArray = npt.NDArray[np.int_] | list[int]
type IdArray = StringArray | IntArray

# numpy specific types
type NpStringArray = npt.NDArray[np.str_]
type NpIntArray = npt.NDArray[np.int_]
type IntMatrix = npt.NDArray[np.int_]
type NpIdArray = NpStringArray | NpIntArray

# Gemma annotations
type RawGemma = list[dict[str, Any]]
type ParsedGemma = dict[str, dict[str, dict[str, list[str]]]]
