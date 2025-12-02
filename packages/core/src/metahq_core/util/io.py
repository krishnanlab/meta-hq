"""
Input/output functions.

Author: Parker Hicks
Date: 2025-04

Last updated: 2025-11-28 by Parker Hicks
"""

import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml
from bson import BSON

from metahq_core.util.alltypes import StringArray


def checkdir(path: str | Path, is_file: bool = False) -> Path:
    """Check if directory exists. If not, creates it.

    Arguments:
        path (str | Path):
            A path to a directory or file.
        is_file (bool):
            If `True` will check the parent of the file path.

    Returns:
        A `pathlib.Path` object of `path`.
    """
    if isinstance(path, str):
        path = Path(path)
    if is_file:
        path = path.resolve().parents[0]

    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    return path


def load_bson(file: str | Path, **kwargs) -> dict[str, Any]:
    """Load dictionary from compressed bson.

    Arguments:
        file (str | Path):
            Path to file.bson to load.
    """
    with open(file, "rb") as bf:
        return BSON(bf.read()).decode(**kwargs)


def load_json(file: str | Path, encoding: str = "utf-8") -> dict[str, Any]:
    """Load dictionary from JSON.

    Arguments:
        file (str | Path):
            Path to file.json to load.
    """
    with open(file, "r", encoding=encoding) as jf:
        return json.load(jf)


def load_txt(
    file: str | Path,
    cols: int = 1,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> list[str]:
    """Loads a txt file.

    Arguments:
        file (str | Path):
            Path to file.txt to load.
        cols (int):
            The number of columns in `file`.
        delimiter (str | None):
            Character to separate entries if the `cols>1`.
        encoding (str):
            Text encoding format.
    Returns:
        An list of strings.
    """
    out = []
    with open(file, "r", encoding=encoding) as f:
        for line in f.readlines():
            l = line.strip()

            if cols > 1:
                l = l.split(delimiter)

            out.append(l)

    return out


def load_txt_sections(file: str | Path, delimiter: str, encoding="utf-8") -> list[str]:
    """Load a .txt file in sections.

    Arguments:
        file (str | Path):
            Path to .txt file to load in sections.
        delimiter (str):
            Pattern to split entries in the .txt file.
        encoding (str):
            Text encoding format.

    Returns:
        Sections of the .txt file separated by the specified delimiter.

    """
    with open(file, "r", encoding=encoding) as f:
        text = f.read()

    return re.split(delimiter, text.strip())


def load_yaml(file: str | Path, encoding: str = "utf-8") -> dict[str, Any]:
    """Load a yaml dictionary.

    Arguments:
        file (str | Path):
            Path to .yaml file to load.
        encoding (str):
            Text encoding format.
    """
    with open(file, "r", encoding=encoding) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as e:
            sys.exit(str(e))


def save_bson(data: dict, file: str | Path, **kwargs):
    """Save dictionary to compressed bson.

    Arguments:
        data (dict):
            Dictionary to compress and save.
        file (str | Path):
            Path to file.txt to save `data`.
    """
    with open(file, "wb") as bf:
        bf.write(BSON.encode(data, **kwargs))


def save_json(data: dict, file: str | Path, encoding: str = "utf-8"):
    """Saves a dictionary to file in JSON format.

    Arguments:
        data (dict):
            Dictionary to save.
        file (str | Path):
            Path to file.txt to save `data`.
        encoding (str):
            Text encoding format.
    """
    with open(file, "w", encoding=encoding) as jf:
        json.dump(data, jf, indent=4)


def save_txt(
    data: StringArray | list[str],
    file: str | Path,
    delimiter: str | None = None,
    encoding: str = "utf-8",
):
    """Save an array or list to a `.txt` file.

    Arguments:
        data (StringArray | list[str]):
            An array or list of string entries.
        file (str | Path):
            Path to file.txt to save `data`.
        delimiter (str | None):
            Allows for multidimensional arrays to be saves as
                single dimension arrays by concatenating each
                element in each row by the passed delimiter.
        encoding (str):
            Text encoding format.
    """
    if delimiter:
        data = [delimiter.join(entry) for entry in data]

    with open(file, "w", encoding=encoding) as f:
        for entry in data:
            f.write(f"{entry}\n")
