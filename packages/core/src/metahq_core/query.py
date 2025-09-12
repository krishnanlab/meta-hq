"""
Class to query the annotations dictionary.

Author: Parker Hicks
Date: 2025-03

Last updated: 2025-09-05 by Parker Hicks
"""

from dataclasses import dataclass, field
from typing import Any

import polars as pl

from metahq_core.curations.annotations import Annotations
from metahq_core.util.io import load_bson
from metahq_core.util.supported import NA_ENTITIES, attributes, databases, ecodes


@dataclass
class AccessionIDs:
    """
    Dataclass to store accession IDs for entries in the annotations dictionary.
    Exists to support modularity and readibility within the Query class.
    """

    indices: list[str] = field(default_factory=list)
    groups: list[str] = field(default_factory=list)
    platforms: list[str] = field(default_factory=list)

    def add(self, index: str, group: str, platform: str):
        """Add an entry. Args can be 'NA'."""
        self.indices.append(index)
        self.groups.append(group)
        self.platforms.append(platform)

    def to_polars(self) -> pl.DataFrame:
        """Converts object to a Polars DataFrame."""
        return pl.DataFrame(
            {"index": self.indices, "group": self.groups, "platform": self.platforms}
        )


@dataclass
class ParsedEntries:
    """
    Dataclass to store parsed entries from the annotations dictionary.
    Exists to support modularity and readibility within the Query class.
    """

    accessions: AccessionIDs = field(default_factory=lambda: AccessionIDs())
    ids: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)

    def add(self, id_: str, value: str, accessions: tuple[str, str, str]):
        """Adds an annotation with an ID, value, and accession IDs. Args can be 'NA'."""
        self.ids.append(id_)
        self.values.append(value)
        self.accessions.add(*accessions)

    def to_polars(self) -> pl.DataFrame:
        """Converts object to a Polars DataFrame."""
        return pl.DataFrame({"id": self.ids, "value": self.values}).hstack(
            self.accessions.to_polars()
        )


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

    def __init__(self, annotations):
        self.annotations: pl.DataFrame = annotations

    def column_intersection_with(self, cols: list[str]) -> list[str]:
        """Find intersection between cols and annotations columns."""
        return list(set(cols) & set(self.annotations.columns))

    def filter_na(self, col: str):
        """Removes entries in a column that are NA."""
        self.annotations = self.annotations.filter(~pl.col(col).is_in(NA_ENTITIES))

    def stage_anchor(self, anchor: str):
        """Filters NA values from the anchor annotations column."""
        self.filter_na(anchor)

    def stage_level(self, level: str):
        """
        Filters NA values from the specified ID level column. If level
        is 'group', then it will also remove annotations with index IDs.
        """
        supported = ["index", "group"]
        if not level in supported:
            raise ValueError(f"Expected level in {supported}, got {level}.")

        if level == "group":
            self.annotations = self.annotations.filter(pl.col("index") == "NA").drop(
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
        id_cols = self.column_intersection_with(["index", "group", "platform"])
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
    _database: str
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
        database: str,
        attribute: str,
        ecode: str = "expert-curated",
        species: str = "homo sapiens",
    ):
        self._database: str = database
        self.attribute: str = attributes(attribute)
        self.ecodes: list[str] = ecodes(ecode)
        self.species: str = species

        self._annotations: dict[str, Any] = self._load_database(database)

    def annotations(self, level: str = "index", anchor: str = "id"):
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
        attr_anno: LongAnnotations = self.compile_annotations()

        attr_anno_wide = attr_anno.pivot_wide(level, anchor)
        na_cols = list(set(attr_anno_wide.columns) & set(NA_ENTITIES))

        return Annotations.from_df(
            attr_anno_wide.drop(na_cols),
            index_col="index",
            group_cols=("group", "platform"),
        )

    def compile_annotations(self) -> LongAnnotations:
        """
        Extract attribute annotations and accession IDs from the annotations dictionary.

        Returns
        -------
        Polars DataFrame of all annotations in the annotations dictionary for a single
        attribute.

        """
        parsed = ParsedEntries()
        for entry in self._annotations:
            accessions = self.get_accession_ids(entry)
            id_, value = self.get_valid_annotations(entry)
            parsed.add(id_, value, accessions)

        parsed = parsed.to_polars()

        if parsed.height == 0:
            raise RuntimeError(
                f"""Unable to identify with provided parameters: [DATABASE: {self._database},
            ATTRIBUTE: {self.attribute}, ECODES: {self.ecodes}"""
            )

        return LongAnnotations(parsed)

    def get_accession_ids(self, entry: str) -> tuple[str, str, str]:
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
        accessions = {"sample": "NA", "series": "NA", "platform": "NA"}
        for id_ in accessions:
            if id_ in self._annotations[entry]["accession_ids"]:
                accessions[id_] = self._annotations[entry]["accession_ids"][id_]

        return accessions["sample"], accessions["series"], accessions["platform"]

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
            self._annotations[entry], self.attribute, self.ecodes, self.species
        ).get_annotations()

    def _load_database(self, query: str):
        """Loads the annotations dictionary for the specified database."""
        anno_map = {"geo": "geo", "sra": "sra", "archs4": "geo"}
        return load_bson(databases(anno_map[query]))
