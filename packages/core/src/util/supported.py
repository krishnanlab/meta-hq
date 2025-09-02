"""
This script stores file path constants and functions to retrieve those paths.

Author: Parker Hicks
Date: 2025-04-15
"""

from pathlib import Path

ROOT: Path = Path(__file__).resolve().parents[3]

##### Databases #####
DATABASES: Path = ROOT / "data/annotations"
GEO: Path = DATABASES / "geo.bson"
SRA: Path = DATABASES / "sra.bson"
ARCHS4: Path = DATABASES / "archs4.bson"
SAMPLE_LASSO_MICRO: Path = DATABASES / "sampleLASSO_microarray.bson"
SAMPLE_LASSO_RNASEQ: Path = DATABASES / "sampleLASSO_rnaseq.bson"

# Arg map
DATABASE_FILES: dict[str, Path] = {
    "geo": GEO,
    "sra": SRA,
    "archs4": ARCHS4,
    "sampleLASSO-microarray": SAMPLE_LASSO_MICRO,
    "sampleLASSO-rnaseq": SAMPLE_LASSO_RNASEQ,
}
LEVEL_IDS: dict[str, list[str]] = {
    "index": ["GSM", "SRS", "SRR", "SRX"],
    "group": ["GSE", "SRX", "GDS", "SRP", "DRP"],
    "platform": ["GPL"],
}
DATABASE_IDS: dict[str, list[str]] = {
    "geo": ["GSM", "GSE", "GDS"],
    "sra": ["SRR", "SRS", "SRX", "SRP", "DRP"],
}

##### Ontology obo files #####
ONTOLOGIES: Path
MONDO: Path = ROOT / "data/ontology/mondo"
UBERON: Path = ROOT / "data/ontology/uberon_ext"
CL: Path = ROOT / "data/ontology/cl"
BTO: Path = ROOT / "data/ontology/bto"

ONTO_FILES: dict[str, Path] = {
    "mondo": MONDO / "mondo.obo",
    "uberon": UBERON / "uberon_ext.obo",
    "cl": CL / "cl.obo",
    "bto": BTO / "bto.obo",
}

ONTO_FAMILY: dict[str, dict[str, Path]] = {
    "mondo": {
        "ancestors": MONDO / "ancestors.parquet",
        "descendants": MONDO / "descendants.parquet",
        "ids": MONDO / "id.txt",
        "systems": MONDO / "systems.txt",
    },
    "uberon": {
        "ancestors": UBERON / "ancestors.parquet",
        "descendants": UBERON / "descendants.parquet",
        "ids": UBERON / "id.txt",
        "systems": UBERON / "systems.txt",
    },
}

##### Evidence codes #####
ECODES: list[str] = ["expert-curated", "semi-curated", "predicted", "any"]


##### Attributes ######
ATTRIBUTES = {
    "tissue": "id",
    "disease": "id",
    "sex": "value",
    "age": "value",
    "developmental stage": "value",
    "organism": "value",
}

ORGANISMS = ["homo sapiens", "mus musculus"]


def attributes(query: str) -> str:
    """Returns default keys to collect attribute values."""
    if query in supported("attributes"):
        return query
    raise ValueError(f"Expected attributes in {supported}, got {query}.")


def databases(query: str) -> Path:
    """Returns path to the file storing annotations for a queried database."""
    if query in supported("databases"):
        return DATABASE_FILES[query]
    raise ValueError(f"Expected database in {supported}, got {query}.")


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


def database_ids() -> dict[str, list[str]]:
    return DATABASE_IDS


def organisms(query: str) -> str:
    _supported = supported("organisms")
    if query in _supported:
        return query
    raise ValueError(f"Expected organism in {_supported}, got {query}.")


def ontologies(query: str) -> Path:
    """Returns the path to a queried ontology."""
    if query in supported("ontologies"):
        return ONTO_FILES[query]
    raise ValueError(f"Expected ontology in {supported('ontologies')}, got {query}.")


def onto_relations(query: str, relatives: str) -> Path:
    """Returns the path to a queried ontology."""
    if query in supported("relations"):
        if relatives in ONTO_FAMILY[query].keys():
            return ONTO_FAMILY[query][relatives]
        raise ValueError(f"Relatives for {query} do not exist.")
    raise ValueError(f"Expected ontology in {supported}, got {query}.")


def supported(entity: str) -> list[str]:
    _supported = {
        "ontologies": list(ONTO_FILES.keys()),
        "attributes": list(ATTRIBUTES.keys()),
        "ecodes": ECODES,
        "databases": list(DATABASE_FILES.keys()),
        "relations": list(ONTO_FAMILY.keys()),
        "organisms": list(ORGANISMS),
    }

    return _supported[entity]
