import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

import yaml
from bson import BSON

from metahq_core.util.alltypes import FilePath, StringArray


def checkdir(path: FilePath, is_file: bool = False):
    if isinstance(path, str):
        path = Path(path)
    if is_file:
        path = path.resolve().parents[0]

    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    return path


def load_bson(file: FilePath, **kwargs) -> dict[str, Any]:
    """Load dictionary from compressed bson."""
    with open(file, "rb") as bf:
        return BSON(bf.read()).decode(**kwargs)


def load_json(file: FilePath, encoding: str = "utf-8") -> dict[str, Any]:
    with open(file, "r", encoding=encoding) as jf:
        return json.load(jf)


def load_txt(
    file: FilePath,
    cols: int = 1,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> list[str]:
    """Loads a txt file."""
    out = []
    with open(file, "r", encoding=encoding) as f:
        for line in f.readlines():
            l = line.strip()

            if cols > 1:
                l = l.split(delimiter)

            out.append(l)

    return out


def load_txt_sections(file: FilePath, delimiter: str, encoding="utf-8") -> list[str]:
    """
    Generator to load a .txt file in sections.

    Args
    ----
    file: FilePath
        Path to .txt file to load in sections.

    delimter: rstring
        Pattern to split entries in the .txt file.

    Returns
    ------
    Sections of the .txt file separated by the specified delimiter.

    """
    with open(file, "r", encoding=encoding) as f:
        text = f.read()

    return re.split(delimiter, text.strip())


def load_yaml(file: FilePath, encoding: str = "utf-8") -> dict[str, Any]:
    """Load a yaml dictionary."""
    with open(file, "r", encoding=encoding) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as e:
            sys.exit(str(e))


def save_bson(data: dict, file: FilePath, **kwargs):
    """Save dictionary to compressed bson."""
    with open(file, "wb") as bf:
        bf.write(BSON.encode(data, **kwargs))


def save_json(data: dict, file: FilePath, encoding: str = "utf-8"):
    with open(file, "w", encoding=encoding) as jf:
        json.dump(data, jf, indent=4)


def save_txt(
    data: StringArray | list[str],
    file: FilePath,
    delimiter: Optional[str] = None,
    encoding="utf-8",
):
    if delimiter:
        data = [delimiter.join(entry) for entry in data]

    with open(file, "w", encoding=encoding) as f:
        for entry in data:
            f.write(f"{entry}\n")


def run_subprocess(command: str) -> subprocess.CompletedProcess[str] | int:
    try:
        result = subprocess.run(
            ["bash", "-c", command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )
        return result

    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"{e}\n")
        return 1
