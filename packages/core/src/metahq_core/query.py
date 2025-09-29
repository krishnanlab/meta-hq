"""
Class to query the annotations dictionary.

Author: Parker Hicks
Date: 2025-03

Last updated: 2025-09-05 by Parker Hicks
"""

from typing import Any, Literal

import polars as pl

from metahq_core.curations.annotations import Annotations
from metahq_core.util.helpers import reverse_dict
from metahq_core.util.io import load_bson
from metahq_core.util.supported import (
    _ecodes,
    attributes,
    get_annotations,
    get_technologies,
    na_entities,
    species_map,
    technologies,
)


class AccessionIDs:
    """
    Stores accession IDs for entries in the annotations dictionary.
    Exists to support modularity and readibility within the Query class.
    """

    def __init__(self, fields):
        self.fields: tuple[str, ...] = fields
        self.ids: dict[str, list] = {field: [] for field in fields}

    def add(self, new: dict[str, str]):
        """Add an entry. Args can be 'NA'."""
        for key, value in new.items():
            if key in self.fields:
                self.ids[key].append(value)

    def to_polars(self) -> pl.DataFrame:
        """Converts object to a Polars DataFrame."""
        return pl.DataFrame(self.ids)


class ParsedEntries:
    """
    Dataclass to store parsed entries from the annotations dictionary.
    Exists to support modularity and readibility within the Query class.
    """

    def __init__(self, fields):
        self.accessions = AccessionIDs(fields)
        self.entries = {"id": [], "value": []}

    def add(self, id_: str, value: str, accessions: dict[str, str]):
        """Adds an annotation with an ID, value, and accession IDs. Args can be 'NA'."""
        self.entries["id"].append(id_)
        self.entries["value"].append(value)
        self.accessions.add(accessions)

    def to_polars(self) -> pl.DataFrame:
        """Converts object to a Polars DataFrame."""
        return pl.DataFrame(self.entries).hstack(self.accessions.to_polars())


class LongAnnotations:
    """
    Annotations in long format.
    Exists to support modularity and readibility within the Query class.

    Attributes
    ----------
    annotations: pl.DataFrame
        DataFrame with columns storing accession IDs with an `id` and `value` column storing
        multiple annotations for a single entry.

    Methods
    -------
    column_intersection_with()
        Finds the intersection between a list of strings and the annotations columns.

    filter_na()
        Removes rows that contain NA values.

    pivot_wide()
        Converts the annotations in long format to one-hot-encoded wide format.

    stage_anchor()
        Removes NA values from the `id` or `values` columns.

    stage_level()
        Filters the annotations that have missing IDs.

    stage()
        Prepares the annotations for conversion to wide format.

    """

    def __init__(self, annotations, id_cols):
        self.annotations: pl.DataFrame = annotations
        self.id_cols: list[str] = id_cols

    def column_intersection_with(self, cols: list[str]) -> list[str]:
        """Find intersection between cols and annotations columns."""
        return list(set(cols) & set(self.annotations.columns))

    def filter_na(self, col: str):
        """Removes entries in a column that are NA."""
        self.annotations = self.annotations.filter(~pl.col(col).is_in(na_entities()))

    def stage_anchor(self, anchor: str):
        """Filters NA values from the anchor annotations column."""
        self.filter_na(anchor)

    def stage_level(self, level: str):
        """
        Filters NA values from the specified ID level column. If level
        is 'group', then it will also remove annotations with index IDs.
        """
        supported = ["sample", "series"]
        if not level in supported:
            raise ValueError(f"Expected level in {supported}, got {level}.")

        if level == "group":
            self.annotations = self.annotations.filter(pl.col("sample") == "NA").drop(
                "index"
            )

        self.filter_na(level)

    def stage(self, level: str, anchor: str):
        """Stages the annotations DataFrame to be converted to wide format."""
        self.stage_level(level)
        self.stage_anchor(anchor)

    def pivot_wide(self, level: str, anchor: str) -> pl.DataFrame:
        """
        Pivots the to wide annotations with one-hot-encoded binary entries for
        each annotation.

        Args
        ----
        level: str
            ID level of the annotations. Either `index` or `group`.
        anchor: str
            Base of the annotations. Either `id` or `value`. Using `id` will return annotations
            to ontology terms for tissue and disease attributes, M/F for the sex attribute, or
            predetermined age groups for the age attribute. Using `value` will return annotations
            with the free text names noted by the annotators.

        Returns
        -------
        Annotations in one-hot-encoded wide format with the accession IDs for each annotation.

        """
        # remove unused entries
        self.stage(level, anchor)

        # prepare accession IDs DataFrame
        id_cols = self.column_intersection_with(self.id_cols)
        ids = self.annotations.select(id_cols)

        # remove unused columns for pivoting
        id_cols.remove(level)
        self.annotations = self.annotations.drop(id_cols)

        # pivot to wide format
        exploded = (
            self.annotations.with_columns(pl.col(anchor).str.split("|").alias(anchor))
            .explode(anchor)
            .unique(maintain_order=True)
        )

        one_hot = (
            exploded.pivot(
                index=level,
                on=anchor,
                values=anchor,
                aggregate_function=pl.count(),
            )
            .fill_null(0)
            .with_columns(pl.exclude(level).cast(pl.Int32))
        )

        return ids.join(one_hot, on=level, how="inner")


class UnParsedEntry:
    """
    Stores and extracts items from a single annotation entry of the annotations dictionary.
    Exists to support modularity and readibility within the Query class.

    Attrubtes
    ---------
    entry: dict[str, dict[str, dict[str, str]]]
        Nested dictionary of annotations in the following structure:
            ID: {
                attribute: {
                    source: {
                        id: "standardized ID",
                        "value": "common name",
                    } ...
                } ...

    attribute: str
        Attribute to extract annotations for.

    ecodes: str
        Permitted evidence codes for annotations.

    Methods
    -------
    get_annotations():
        Retrieves all available annotations that match the specified parameters.

    is_acceptable():
        Determines if an entry has annotations available for the attribute.

    get_id_value():
        Extracts ID and value entries for an individual source within a single entry.

    """

    def __init__(self, _entry, _attribute, _ecodes, _species):
        self.entry: dict[str, dict[str, dict[str, str]]] = _entry
        self.attribute: str = _attribute
        self.ecodes: list[str] = _ecodes
        self.species: str = _species

    def get_annotations(self) -> tuple[str, str]:
        """
        Retrieves the ID and value annotations for a single entry.

        Returns
        -------
        ID and value annotations for a given attribute. If there are multiple annotations
        across sources, then they are concatenated with a `|` delimiter.

        """
        if not self.is_acceptable():
            return ("NA", "NA")

        # add attribute annotations across sources
        ids: set[str] = set()
        values: set[str] = set()
        for source in self.entry[self.attribute].values():
            if source["ecode"] not in self.ecodes:
                continue

            id_, value = self.get_id_value(source)
            ids.add(id_)
            values.add(value)

        return "|".join(ids), "|".join(values)

    def is_acceptable(self) -> bool:
        """Checks if an attribute annotation exists."""
        attr_exists = self.attribute in self.entry
        is_correct_species = self.entry["organism"] == self.species
        is_populated = len(self.entry) > 0

        return attr_exists and is_populated and is_correct_species

    @staticmethod
    def get_id_value(source_anno):
        """
        Extracts the ID and value for an annotation.

        Args
        ----
        source_anno: dict[str, str]
            Annotations from a single source. Has keys ['id', 'value', 'ecode'].

        Returns
        -------
        Tuple of the ID and value for the attribute annotation from a single source.

        """
        if "id" in source_anno:
            id_ = source_anno["id"]
        else:
            id_ = "NA"

        if "value" in source_anno:
            value = source_anno["value"]
        else:
            value = "NA"
        return id_, value


class Query:
    """
    Class to query the annotations dictionary.

    Attributes
    ----------
    database: str
        Database to query annotations.

    _annotations: dict
        Nested dictionary of annotations.

    attribute: str
        Attribute to collect annotations for (e.g., tissue, disease, sex, age)

    ecodes: list[str]
        Acceptable evidence codes for annotations.

    Methods
    -------
    annotations()
        Primary function to extract formatted annotations from the annotations dictionary.
        Can be propagated to labels if in wide format.

    compile_annotations()
       Backend function of `annotations()`. Does the actual extracting.

    get_accession_ids()
       Retrives and structures accession IDs for a given entry.

    get_valid_annotations()
        Retrives all valid annotatiosn for a given entry.

    _load_database()
        Loads the annotations dictionary.

    Example
    -------
    >>> from metahq import Query
    >>> query = Query("geo", "tissue", "expert-curated")

    """

    def __init__(
        self,
        database,
        attribute,
        level="sample",
        ecode="expert-curated",
        species="human",
        technology="rnaseq",
    ):
        self.database: str = database
        self.attribute: str = attributes(attribute)
        self.level: Literal["sample", "series"] = level
        self.ecodes: list[str] = self._load_ecode(ecode)
        self.species: str = self._load_species(species)
        self.technology: str = technologies(technology)

        self._annotations: dict[str, Any] = self._load_annotations()

    def annotations(self, anchor: str = "id"):
        """
        Retrieve annotations from the databse annotations dictionary.

        Args
        ----
        level: str
            Level of annotations. Can be `index` or `group`.

        anchor: str
            Base of the annotations. Either `id` or `value`. Using `id` will return annotations
            to ontology terms for tissue and disease attributes, M/F for the sex attribute, or
            predetermined age groups for the age attribute. Using `value` will return annotations
            with the free text names noted by the annotators.

        Returns
        -------
        An `Annotations` object with one-hot-encoded annotations to the specified attribute.

        Example
        -------
        >>> from metahq import Query
        >>> query = Query('geo', 'tissue', 'expert-curated', 'homo-sapiens')
        >>> query.annotations(level='index', anchor='id')
        ┌──────────┬───────────┬──────────┬────────────────┬───┬────────────────┐
        │ group    ┆ index     ┆ platform ┆ UBERON:0002113 ┆ … ┆ UBERON_0000057 │
        │ ---      ┆ ---       ┆ ---      ┆ ---            ┆   ┆ ---            │
        │ str      ┆ str       ┆ str      ┆ i32            ┆   ┆ i32            │
        ╞══════════╪═══════════╪══════════╪════════════════╪═══╪════════════════╡
        │ GSE11151 ┆ GSM281311 ┆ GPL570   ┆ 1              ┆ … ┆ 0              │
        │ GSE11151 ┆ GSM281312 ┆ GPL570   ┆ 1              ┆ … ┆ 0              │
        │ GSE18969 ┆ GSM469548 ┆ NA       ┆ 1              ┆ … ┆ 0              │
        │ GSE18969 ┆ GSM469549 ┆ NA       ┆ 1              ┆ … ┆ 0              │
        │ GSE18969 ┆ GSM469550 ┆ NA       ┆ 1              ┆ … ┆ 0              │
        │ …        ┆ …         ┆ …        ┆ …              ┆ … ┆ …              │
        │ GSE2109  ┆ GSM152666 ┆ NA       ┆ 0              ┆ … ┆ 0              │
        │ GSE2109  ┆ GSM179804 ┆ NA       ┆ 0              ┆ … ┆ 0              │
        │ GSE2109  ┆ GSM353890 ┆ NA       ┆ 0              ┆ … ┆ 0              │
        │ GSE2109  ┆ GSM102435 ┆ NA       ┆ 0              ┆ … ┆ 0              │
        │ GSE2109  ┆ GSM353891 ┆ NA       ┆ 0              ┆ … ┆ 0              │
        └──────────┴───────────┴──────────┴────────────────┴───┴────────────────┘

        """
        index, groups = self.assign_index_groups()
        fields = [index] + list(groups)
        attr_anno = self.compile_annotations(fields).pivot_wide(self.level, anchor)
        na_cols = list(set(attr_anno.columns) & set(na_entities()))

        return Annotations.from_df(
            attr_anno.drop(na_cols),
            index_col=index,
            group_cols=groups,
        )

    def assign_index_groups(self):
        if self.level == "series":
            return "series", tuple(["platform"])

        if self.level == "sample":
            return "sample", tuple(["series", "platform"])

        raise ValueError(f"Expected level in [sample, study], got {self.level}.")

    def compile_annotations(self, fields: list[str]) -> LongAnnotations:
        """
        Extract attribute annotations and accession IDs from the annotations dictionary.

        Returns
        -------
        Polars DataFrame of all annotations in the annotations dictionary for a single
        attribute.

        """
        parsed = ParsedEntries(fields)
        for entry in self._annotations:
            accessions = self.get_accession_ids(entry)
            id_, value = self.get_valid_annotations(entry)
            parsed.add(id_, value, accessions)

        parsed = parsed.to_polars()
        parsed = parsed.filter(
            pl.col("platform").is_in(self._load_platforms())
        )  # filter platforms just once for speed

        if parsed.height == 0:
            raise RuntimeError(
                f"""Unable to identify with provided parameters: [ATTRIBUTE: {self.attribute},
                SPECIES: {self.species}, ECODES: {self.ecodes}, TECHNOLOGY: {self.technology}]"""
            )

        return LongAnnotations(parsed, fields)

    def get_accession_ids(self, entry: str) -> dict[str, str]:
        """
        Updates an AccessionIDs object with index, group, and platform
        IDs from an annotations entry.

        Args
        ----
        entry: str
            Top key of the annotations dictionary to extract accession IDs for.

        Returns
        -------
        Tuple of index, group, and platform ID for a given entry in the annotations
        dictionary.

        """
        if self.level == "sample":
            accessions = {"sample": "NA", "series": "NA", "platform": "NA"}
        else:
            accessions = {"series": "NA", "platform": "NA"}

        for id_ in accessions:
            if id_ in self._annotations[entry]["accession_ids"]:
                accessions[id_] = self._annotations[entry]["accession_ids"][id_]

        return accessions

    def get_valid_annotations(self, entry: str) -> tuple[str, str]:
        """
        Extract id and value annotations for each source of annotations in an entry.

        Args
        ----
        entry: str
            A top-level key of the annotations dictionary.

        Returns
        -------
        Tuple of the annotation IDs and values.

        """
        return UnParsedEntry(
            self._annotations[entry],
            self.attribute,
            self.ecodes,
            self.species,
        ).get_annotations()

    def _load_annotations(self):
        """Loads the annotations dictionary for the specified database."""
        anno = load_bson(get_annotations(self.level))

        return anno

    def _load_platforms(self) -> list[str]:
        return (
            pl.scan_parquet(get_technologies())
            .filter(pl.col("technology") == self.technology)
            .collect()["id"]
            .to_list()
        )

    def _load_ecode(self, ecode: str) -> list[str]:
        map_ = _ecodes()

        if ecode == "any":
            __ecodes = list(map_.values()).copy()
            __ecodes.remove("any")
            return __ecodes

        if ecode in map_:
            return [map_[ecode]]
        if ecode in map_.values():
            return [ecode]
        raise ValueError(
            f"Invalid ecode query: {ecode}. Run metahq supported ecodes for available options."
        )

    def _load_species(self, species: str) -> str:
        map_ = species_map()
        if species in map_:
            return map_[species]
        if species in map_.values():
            return species
        raise ValueError(
            f"Invalid species query: {species}. Run metahq supported species for available options."
        )
