"""
Class to query the annotations dictionary.

Author: Parker Hicks
Date: 2025-03

Last updated: 2025-12-19 by Parker Hicks
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from metahq_core.curations.annotations import Annotations
from metahq_core.logger import setup_logger
from metahq_core.util.exceptions import NoResultsFound
from metahq_core.util.io import load_bson
from metahq_core.util.supported import (
    _ecodes,
    attributes,
    get_annotations,
    get_technologies,
    na_entities,
    species_map,
    supported,
    technologies,
)

if TYPE_CHECKING:
    import logging


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
    """Annotations in long format.

    Exists to support modularity and readibility within the Query class.

    Attributes:
        annotations (pl.DataFrame):
            DataFrame with columns storing accession IDs with an `id` and `value` column storing
            multiple annotations for a single entry.
    """

    def __init__(self, annotations):
        self.annotations: pl.DataFrame = annotations

    def column_intersection_with(self, columns: list[str]) -> list[str]:
        """Find intersection between `columns` and the columns in the `annotations` attribute.

        Arguments:
            columns (list[str]):
                Any list of potential columns in the DataFrame.

        Returns:
            The intersection of columns.
        """
        return list(set(columns) & set(self.annotations.columns))

    def filter_na(self, column: str):
        """Removes entries in a column that are NA-like values (e.g., 'NA' or 'none').
        Updates the annotations attribute in place.

        Arguments:
            column (str):
                The name of a column in the DataFrame.
        """
        self.annotations = self.annotations.filter(~pl.col(column).is_in(na_entities()))

    def stage_anchor(self, anchor: Literal["id", "value"]):
        """Filters NA values from the anchor annotations column.

        Arguments:
            anchor (Literal["id", "value"]):
                The column storing desired format of annotations.
        """
        self.filter_na(anchor)

    def stage_level(self, level: Literal["sample", "series"]):
        """Filters NA values from the specified ID level column. If level
        is 'group', then it will also remove annotations with index IDs.

        Arguments:
            level (Literal['sample', 'series']):
                Annotation level.
        """
        if not level in supported("levels"):
            raise ValueError(f"Expected level in {supported("levels")}, got {level}.")

        if level == "series":
            self.annotations = self.annotations.filter(pl.col(level) != "NA")

            if "sample" in self.annotations.columns:
                self.annotations = self.annotations.drop("sample")

        self.filter_na(level)

    def stage(self, level: Literal["sample", "series"], anchor: Literal["id", "value"]):
        """Stages the annotations DataFrame to be converted to wide format. Mutates the
        annotations attribute in place.

        Arguments:
            level (Literal['sample', 'series']):
                Annotation level.

            anchor (Literal["id", "value"]):
                The column storing desired format of annotations.

        """
        self.stage_level(level)
        self.stage_anchor(anchor)

    def pivot_wide(
        self,
        level: Literal["sample", "series"],
        anchor: Literal["id", "value"],
        id_cols: list[str],
    ) -> pl.DataFrame:
        """Pivots the to wide annotations with one-hot-encoded binary entries for
        each annotation.

        Arguments:
            level (Literal['sample', 'series']):
                Annotation level.

            anchor (Literal["id", "value"]):
                The column storing desired format of annotations.

            id_cols (list[str]):
                Columns to keep as IDs when pivoting.

        Returns:
            Annotations in one-hot-encoded wide format with the accession IDs for each annotation.

        Examples:
            >>> from metahq_core.query import LongAnnotations
            >>> anno = pl.DataFrame({
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'platform': ['GPL1', 'GPL2', 'GPL2'],
                    'id': ['UBERON:0000948|UBERON:0002349', 'UBERON:0002113', 'UBERON:0000955'],
                    'value': ['heart|myocardium', 'kidney', 'brain'],
                })
            >>> anno = LongAnnotations(anno)
            >>> anno.pivot_wide(
                    level='sample', anchor='id', id_cols=['sample', 'series']
                )
            ┌────────┬────────┬────────────────┬────────────────┬────────────────┬────────────────┐
            │ series ┆ sample ┆ UBERON:0000948 ┆ UBERON:0002349 ┆ UBERON:0002113 ┆ UBERON:0000955 │
            │ ---    ┆ ---    ┆ ---            ┆ ---            ┆ ---            ┆ ---            │
            │ str    ┆ str    ┆ i32            ┆ i32            ┆ i32            ┆ i32            │
            ╞════════╪════════╪════════════════╪════════════════╪════════════════╪════════════════╡
            │ GSE1   ┆ GSM1   ┆ 1              ┆ 1              ┆ 0              ┆ 0              │
            │ GSE1   ┆ GSM2   ┆ 0              ┆ 0              ┆ 1              ┆ 0              │
            │ GSE2   ┆ GSM3   ┆ 0              ┆ 0              ┆ 0              ┆ 1              │
            └────────┴────────┴────────────────┴────────────────┴────────────────┴────────────────┘

        """
        # remove unused entries
        self.stage(level, anchor)

        # prepare accession IDs DataFrame
        id_cols = self.column_intersection_with(id_cols)
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
                aggregate_function=pl.len(),
            )
            .fill_null(0)
            .with_columns(pl.exclude(level).cast(pl.Int32))
        )

        return ids.join(one_hot, on=level, how="inner")


class UnParsedEntry:
    """Stores and extracts items from a single annotation entry of the annotations dictionary.

    Exists to support modularity and readibility within the Query class.

    Attributes:
        entry (dict[str, dict[str, dict[str, str] | str]]):
            Annotations for a single entry in the database.

        attribute (str):
            Attribute to extract annotations for.

        ecodes (list[str]):
            Permitted evidence codes for annotations.

        species (str):
            Species for which to extract annotations for.

    Examples:
        >>> from metahq_core.query import UnParsedEntry
        >>> entry = {
                'GSM281311': {
                    'organism': 'homo sapiens',
                    'tissue': {
                        'ursa': {
                            'id': 'UBERON:0002113', 'value': 'kidney', 'ecode': 'expert-curated'
                        }
                    }
                }
            }
        >>> UnParsedEntry(
                entry,
                attribute='tissue',
                ecodes=['expert-curated'],
                'homo sapiens'
            )
    """

    def __init__(self, entry, attribute, ecodes, species):
        self.entry: dict[str, dict[str, dict[str, str]]] = entry
        self.attribute: str = attribute
        self.ecodes: list[str] = ecodes
        self.species: str = species

    def get_annotations(self) -> tuple[str, str]:
        """
        Retrieves the ID and value annotations for a single entry.

        Returns:
            ID and value annotations for a given attribute. If there are multiple annotations
                across sources, then they are concatenated with a `|` delimiter. If no ID or
                value annotations exist, `NA` is returned.

        Examples:
            >>> from metahq_core.query import UnParsedEntry
            >>> entry = {
                    'GSM281311': {
                        'organism': 'homo sapiens',
                        'tissue': {
                            'ursa': {
                                'id': 'UBERON:0002113', 'value': 'kidney', 'ecode': 'expert-curated'
                            }
                        }
                    }
                }
            >>> unparsed = UnParsedEntry(
                    entry,
                    attribute='tissue',
                    ecodes=['expert-curated'],
                    'homo sapiens'
                )
            >>> unparsed.get_annotations()
            ('UBERON:0002113', 'kidney')

        """
        if not self.is_acceptable():
            return ("NA", "NA")

        # add attribute annotations across sources
        ids: set[str] = set()
        values: set[str] = set()
        sources: set[str] = set()
        for source, annotations in self.entry[self.attribute].items():
            if annotations["ecode"] not in self.ecodes:
                continue

            id_, value = self.get_id_value(annotations)
            ids.add(id_)
            values.add(value)
            sources.add(source)

            # print(self.entry)
            # print(id_)
            # print(value)
            # print(source)
            # exit()

        return "|".join(ids), "|".join(values)

    def is_acceptable(self) -> bool:
        """Checks if the entry is not empty and is an acceptable annotation given the
        passed attribute, ecode, and species.

        Returns:
            True or False given the specified attributes.

        Examples:
            >>> from metahq_core.query import UnParsedEntry
            >>> entry = {
                    'GSM281311': {
                        'organism': 'homo sapiens',
                        'tissue': {
                            'ursa': {
                                'id': 'UBERON:0002113', 'value': 'kidney', 'ecode': 'expert-curated'
                            }
                        }
                    }
                }
            >>> unparsed = UnParsedEntry(
                    entry,
                    attribute='tissue',
                    ecodes=['expert-curated'],
                    'homo sapiens'
                )
            >>> unparsed.is_acceptable()
            True

            If an attribute doesn't exist, it will return False.

            >>> entry = {
                    'GSM315993': {
                        'organism': 'homo sapiens',
                        'sex': {
                            'Johnson 2023': {
                                'id': 'F', 'ecode': 'expert-curated'
                            }
                        }
                    }
                }
            >>> unparsed = UnParsedEntry(
                    entry,
                    attribute='tissue',
                    ecodes=['expert-curated'],
                    'homo sapiens'
                )
            >>> unparsed.is_acceptable()
            False
        """
        attr_exists = self.attribute in self.entry
        is_populated = len(self.entry) > 0

        if "organism" in self.entry:
            is_correct_species = self.entry["organism"] == self.species
        else:
            is_correct_species = False

        return attr_exists and is_populated and is_correct_species

    @staticmethod
    def get_id_value(source_anno) -> tuple[str, str]:
        """Extracts the ID and value for an annotation.

        Arguments:
            source_anno (dict[str, str]):
                Annotations from a single source. Has keys ['id', 'value', 'ecode'].

        Returns:
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
    """Class to query the MetaHQ database.

    Attributes:
        attribute (str):
            Attribute to collect annotations for (e.g., tissue, disease, sex, age)

        level (Literal['sample', 'series']):
            Level of annotations to query.

        ecodes (list[str]):
            Acceptable evidence codes for annotations.

        species (str):
            Species for which to query annotations.

        technology (str):
            Technology of the queried samples.

        _annotations (dict):
            Nested dictionary of annotations.

    Examples:
        >>> from metahq_core.query import Query
        >>> query = Query(
                "tissue",
                level="sample",
                ecodes=["expert-curated"],
                species="homo sapiens",
                technology="rnaseq",
            )
    """

    def __init__(
        self,
        database,
        attribute,
        level,
        ecode,
        species,
        technology,
        logger=None,
        loglevel=20,
        logdir=Path("."),
        verbose=True,
    ):
        self.database: str = database
        self.attribute: str = attributes(attribute)
        self.level: Literal["sample", "series"] = level
        self.ecodes: list[str] = self._load_ecode(ecode)
        self.species: str = self._load_species(species)
        self.technology: str = technologies(technology)

        self._annotations: dict[str, Any] = self._load_annotations()

        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.log: logging.Logger = logger
        self.verbose: bool = verbose

    def annotations(self, anchor: Literal["id", "value"] = "id") -> Annotations:
        """Retrieve annotations from the MetaHQ database.

        Arguments:
            anchor (Literal['id', 'value']):
                Base of the annotations. Either `id` or `value`. Using `id` will return annotations
                to ontology terms for tissue and disease attributes, M/F for the sex attribute, or
                predetermined age groups for the age attribute. Using `value` will return
                annotations with the free text names for each id.

        Returns:
            An `Annotations` object with one-hot-encoded annotations to the specified attribute.

        Examples:
            >>> from metahq_core.query import Query
            >>> query = Query(
                "tissue",
                level="sample",
                ecodes=["expert-curated"],
                species="homo sapiens",
                technology="rnaseq",
            )
            >>> query.annotations(anchor='id')
        """
        # get ID column names
        index, groups = self._assign_index_groups()
        id_cols = [index] + list(groups)

        # construct the annotations
        attr_anno = self.compile_annotations(id_cols)
        attr_anno = LongAnnotations(attr_anno).pivot_wide(self.level, anchor, id_cols)

        na_cols = list(set(attr_anno.columns) & set(na_entities()))

        return Annotations.from_df(
            attr_anno.drop(na_cols),
            index_col=index,
            group_cols=groups,
            logger=self.log,
            verbose=self.verbose,
        )

    def compile_annotations(self, id_cols: list[str]) -> pl.DataFrame:
        """Extract attribute annotations and accession IDs from the database.

        Arguments:
            id_cols (list[str]):
                Accession IDs

        Returns:
            Polars DataFrame of all annotations in the annotations dictionary for a single
                attribute.

        Raises:
            NoResultsFound: If no attribute annotations can be found.

        """
        parsed = ParsedEntries(id_cols)
        for entry in self._annotations:
            accessions = self.get_accession_ids(entry)
            id_, value = self.get_valid_annotations(entry)
            parsed.add(id_, value, accessions)

        parsed = parsed.to_polars()
        parsed = parsed.filter(
            pl.col("platform").is_in(self._load_platforms())
        )  # filter platforms just once for speed

        if parsed.height == 0:
            msg = (
                """Unable to identify with provided parameters: [ATTRIBUTE: %s,
                SPECIES: %s, ECODES: %s, TECHNOLOGY: %s]""",
                self.attribute,
                self.species,
                self.ecodes,
                self.technology,
            )
            if self.verbose:
                self.log.error(msg)
            raise NoResultsFound(msg)

        return parsed

    def get_accession_ids(self, entry: str) -> dict[str, str]:
        """Updates an AccessionIDs object with index, group, and platform
        IDs from an annotations entry.

        Arguments:
            entry (str):
                An ID with annotations in the database (i.e., one of the top level keys of
                    the database.)

        Returns:
            accessions (dict[str, str]):
                A populated dictionary of accession IDs and values for the passed entry.

        Examples:
            >>> from metahq_core.query import Query
            >>> query = Query(
                "tissue",
                level="sample",
                ecodes=["expert-curated"],
                species="homo sapiens",
                technology="rnaseq",
            )
            >>> query.get_accession_ids('GSM281311')
            {'sample': 'GSM281311', 'series': 'GSE11151', 'platform': 'GPL570'}


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
        """Extract id and value annotations for each source of annotations in an entry.

        Arguments:
            entry: str
                A top-level key of the annotations dictionary.

        Returns:
            Tuple of the annotation IDs and values.

        """
        return UnParsedEntry(
            self._annotations[entry],
            self.attribute,
            self.ecodes,
            self.species,
        ).get_annotations()

    def _assign_index_groups(self):
        if self.level == "series":
            return "series", tuple(["platform"])

        if self.level == "sample":
            return "sample", tuple(["series", "platform"])

        raise ValueError(f"Expected level in [sample, study], got {self.level}.")

    def _load_annotations(self):
        """Loads the MetaHQ database for the specified level."""
        anno = load_bson(get_annotations(self.level))

        return anno

    def _load_platforms(self) -> list[str]:
        return list(
            pl.scan_parquet(get_technologies())
            .filter(pl.col("technology") == self.technology)
            .collect()["id"]
        )

    def _load_ecode(self, ecode: str) -> list[str]:
        map_ = _ecodes()

        if ecode == "any":
            __ecodes = list(map_.values()).copy()
            __ecodes.remove("any")
            return __ecodes

        if ecode in map_:
            return [map_[ecode]]  # provided shorthand
        if ecode in map_.values():
            return [ecode]
        raise ValueError(
            f"Invalid ecode query: {ecode}. Run metahq supported ecodes for available options."
        )

    def _load_species(self, species: str) -> str:
        map_ = species_map()
        if species in map_:
            return map_[species]  # provided shorthand
        if species in map_.values():
            return species
        raise ValueError(
            f"Invalid species query: {species}. Run metahq supported species for available options."
        )
