"""
This script stores file path constants and functions to retrieve those paths.

Author: Parker Hicks
Date: 2025-04-15

Last updated: 2025-09-10
"""

from pathlib import Path
from typing import Literal

from metahq_core.util.io import load_yaml

# Root dir of metahq package
ROOT: Path = Path(__file__).resolve().parents[5]

# Root dir of config
HOME = Path.home()
METAHQ = HOME / "metahq"
CONFIG_FILE = METAHQ / "config.yaml"


SUPPORTED_DATABASES: list[str] = ["geo", "sra", "archs4"]
SUPPORTED_LEVELS: list[str] = ["sample", "series"]
SUPPORTED_ONTOLOGIES: list[str] = ["uberon", "mondo"]
SUPPORTED_SAMPLE_METADATA: list[str] = [
    "sample",
    "series",
    "platform",
    "description",
    "srr",
    "srx",
    "srs",
    "srp",
]
SUPPORTED_SERIES_METADATA: list[str] = [
    "series",
    "platform",
    "description",
    "srr",
    "srx",
    "srs",
    "srp",
]
SUPPORTED_TECHNOLOGIES: list[str] = ["microarray", "rnaseq"]


def get_config():
    return load_yaml(CONFIG_FILE)


def get_data_dir():
    return Path(get_config()["data_dir"])


def get_annotations(level: Literal["sample", "series"]) -> Path:
    _databases: Path = get_data_dir() / "annotations"
    return _databases / f"combined__level-{level}.bson"


def get_archs4() -> Path:
    return Path(get_config()["data_dir"] / "databases/archs4")


def get_metadata_path() -> Path:
    return get_data_dir() / "metadata"


def get_technologies() -> Path:
    metadata: Path = get_data_dir() / "metadata"
    return metadata / "technologies.parquet"


def geo_metadata(level: Literal["sample", "series"]) -> Path:
    _supported = ["sample", "series"]
    if level in _supported:
        return get_metadata_path() / f"metadata__level-{level}.parquet"
    raise ValueError(f"Expected level in {_supported}, got {level}.")


LEVEL_IDS: dict[str, list[str]] = {
    "index": ["GSM", "SRS", "SRR", "SRX"],
    "group": ["GSE", "SRX", "GDS", "SRP", "DRP"],
    "platform": ["GPL"],
}
DATABASE_IDS: dict[str, list[str]] = {
    "geo": ["gsm", "gse"],
    "sra": ["srr", "srs", "srx", "srp"],
}


##### Ontology obo files #####


def get_ontology_dirs(onto: str) -> Path:
    _ontologies: Path = get_data_dir() / "ontology"
    opt = {
        "mondo": _ontologies / "mondo",
        "uberon": _ontologies / "uberon_ext",
    }

    return opt[onto]


def get_ontology_files(onto: str) -> Path:
    mondo = get_ontology_dirs("mondo")
    uberon = get_ontology_dirs("uberon")
    opt = {
        "mondo": mondo / "mondo.obo",
        "uberon": uberon / "uberon_ext.obo",
    }

    return opt[onto]


def get_onto_families(onto: str) -> dict[str, Path]:
    mondo = get_ontology_dirs("mondo")
    uberon = get_ontology_dirs("uberon")
    opt = {
        "mondo": {
            "relations": mondo / "relations.parquet",
            "ids": mondo / "id.txt",
            "systems": mondo / "systems.txt",
        },
        "uberon": {
            "relations": uberon / "relations.parquet",
            "ids": uberon / "id.txt",
            "systems": uberon / "systems.txt",
        },
    }

    return opt[onto]


##### Evidence codes #####
ECODES: list[str] = ["expert-curated", "semi-curated", "predicted", "any"]


##### Attributes ######
ATTRIBUTES = [
    "tissue",
    "disease",
    "sex",
    "age",
    "developmental stage",
    "organism",
]

ORGANISMS = [
    "homo sapiens",
    "mus musculus",
    "rattus norvegicus",
    "danio rerio",
    "caenorhabditis eligans",
    "drosopila melanogaster",
]

# tmp fix. Need to find out why these are included in anno
NA_ENTITIES = ["na", "", "NA"]  # annotations to not include


def attributes(query: str) -> str:
    """Returns default keys to collect attribute values."""
    _supported = supported("attributes")
    if query in _supported:
        return query
    raise ValueError(f"Expected attributes in {_supported}, got {query}.")


def databases(query: str) -> Path:
    """Returns path to the file storing annotations for a queried database."""
    _supported = supported("databases")
    if query in _supported:
        return get_databases(query)
    raise ValueError(f"Expected database in {_supported}, got {query}.")


def ecodes(query: list[str] | str) -> list[str]:
    """Checks if query is in the supported evidence codes."""

    def check_ecode(_ecode: str):
        if not _ecode in ECODES:
            raise ValueError(f"Expected ecode in {ECODES}, got {_ecode}.")

    if query == "any":
        _ecodes = ECODES.copy()
        _ecodes.remove("any")
        return _ecodes

    if isinstance(query, str):
        query = [query]

    for ecode in query:
        check_ecode(ecode)
    return query


def level_ids(level: str) -> list[str]:
    return LEVEL_IDS[level]


def levels() -> list[str]:
    return list(LEVEL_IDS.keys())


def technologies(query: str) -> str:
    if query in SUPPORTED_TECHNOLOGIES:
        return query
    raise ValueError(f"Expected technology in {SUPPORTED_TECHNOLOGIES}, got {query}.")


def database_ids(query: str) -> list[str]:
    """Retrieve supported accession IDs for SRA or GEO."""
    _supported = list(DATABASE_IDS.keys())
    if query in _supported:
        return DATABASE_IDS[query]
    raise ValueError(f"Expected query in {_supported}, got {query}.")


def metadata_fields(level: str) -> list[str]:
    if level == "sample":
        return SUPPORTED_SAMPLE_METADATA
    if level == "series":
        return SUPPORTED_SERIES_METADATA
    raise ValueError(f"Expected level in [sample, series], got {level}.")


def organisms(query: str) -> str:
    _supported = supported("organisms")
    if query in _supported:
        return query
    raise ValueError(f"Expected organism in {_supported}, got {query}.")


def ontologies(query: str) -> Path:
    """Returns the path to a queried ontology."""
    _supported = supported("ontologies")
    if query in _supported:
        return get_ontology_files(query)
    raise ValueError(f"Expected ontology in {_supported}, got {query}.")


def onto_relations(query: str, relatives: str) -> Path:
    """Returns the path to a queried ontology."""
    _supported = supported("relations")
    if query in _supported:
        if relatives in get_onto_families(query).keys():
            return get_onto_families(query)[relatives]
        raise ValueError(f"Relatives for {query} do not exist.")
    raise ValueError(f"Expected ontology in {_supported}, got {query}.")


def supported(entity: str) -> list[str]:
    _supported = {
        "ontologies": SUPPORTED_ONTOLOGIES,
        "attributes": ATTRIBUTES,
        "ecodes": ECODES,
        "levels": SUPPORTED_LEVELS,
        "databases": SUPPORTED_DATABASES,
        "relations": SUPPORTED_ONTOLOGIES,
        "technologies": SUPPORTED_TECHNOLOGIES,
        "species": list(ORGANISMS),
    }
    return _supported[entity]
