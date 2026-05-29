import gzip
import warnings
from pathlib import Path
from typing import Literal, TypeAlias, get_args

import polars as pl

from metahq_build.ontology._obo_entry import OboEntry, XRef
from metahq_build.util.logging import setup_logger

XRefLevel: TypeAlias = Literal[
    "equivalentTo", "relatedTo", "otherHierarchy", "Redundant", "shared-umls-xref"
]
XREF_LEVELS: tuple[XRefLevel, ...] = get_args(XRefLevel)
DEFAULT_XREF_LEVELS: tuple[XRefLevel, ...] = (
    "equivalentTo",
    "relatedTo",
    "shared-umls-xref",
)


class XRefMappings:
    """Structured mappings between ontology terms.

    Attributes:
        anchor (str):
            The prefix of the main ontology for which xrefs were collected.
        to (str):
            The prefix of the ontology mapped from the anchor.
        mapping (dict[str, list[str]]):
            A mapping between anchor terms and their cross references.
    """

    def __init__(self, anchor, to, mapping):
        self.anchor: str = anchor
        self.to: str = to
        self.mapping: dict[str, list[str]] = mapping

    def pl(self, explode: bool = False):
        """Export xref mappings to a polars DataFrame."""
        df = pl.DataFrame(
            {
                self.anchor: list(self.mapping.keys()),
                self.to: list(self.mapping.values()),
            }
        ).sort(self.anchor)
        if explode:
            return df.explode(self.to).select([self.anchor, self.to])

        return df.select([self.anchor, self.to])

    def reverse(self) -> dict[str, str]:
        """Export the mappings as a to: anchor dictionary."""
        df = self.pl(explode=True)
        return {row[1]: row[0] for row in df.iter_rows()}

    def add(self, new: dict[str, list[str]]) -> None:
        """Add a new key and value to the mapping."""
        for k, v in new.items():
            if k in self.mapping:
                self.add_existing(k, v)
            else:
                self.mapping[k] = v

    def add_existing(self, key: str, val: list[str]) -> None:
        """Add another mapping to an existing anchor term."""
        if key in self.mapping:
            existing: list[str] = self.mapping[key]
            existing.extend(val)
            self.mapping.update({key: list(set(existing))})

        else:
            warnings.warn(
                "Attempted to add value to XRef mapping, but key does not exist. Skipping..."
            )


class XRefExtractor:
    """Ontology cross reference extractor.

    Attributes:
        prefix (str):
            The ontology prefix for IDs (e.g., MONDO for MONDO:0004994).
        entries (list[OboEntry]):
            A list of OboEntry objects.
    """

    def __init__(self, entries, levels):
        self.logger = setup_logger("metahq_build.ontology.relations.XRef")

        self.entries: list[OboEntry] = entries
        self.levels: set[XRefLevel] = self._parse_levels(
            levels, valid_levels=XREF_LEVELS
        )

    def get(self, ref: str, keep_anchors: list[str] | None = None) -> XRefMappings:
        """Extract cross references from a set of obo entries.

        Arguments:
            ref (str):
                The prefix of an ontology to map to (e.g., MONDO, DOID, MESH).
            keep_anchors (list[str] | None):
                Prefixs of the anchor ontologies (e.g., ['UBERON', 'CL']). This exists
                    because ontology OBO files can contain entries for terms that are imported
                    from other ontologies. If left as 'None', all mappings will be returned,
                    otherwise only mappings between the 'keep_anchors' values and ref will be
                    returned.

        Returns:
            (XRefMappings): Structured mappings between the anchor ontology and ref.

        """
        xrefs = {}
        for entry in self.entries:
            entry_xrefs = [xref for xref in entry.xrefs if xref.ref_id.startswith(ref)]
            if len(entry_xrefs) == 0:
                continue

            levels = {f"{entry.id_prefix}:{level}" for level in self.levels}
            mappings = self._resolve_entry_xrefs(entry_xrefs, levels)
            if len(mappings) > 0:
                xrefs[entry.id] = mappings

        if isinstance(keep_anchors, list):
            xrefs = {k: v for k, v in xrefs if k.split(":") in keep_anchors}

        xrefs = XRefMappings(anchor="anchor", to=ref, mapping=xrefs)
        return xrefs

    def _resolve_entry_xrefs(self, xrefs: list[XRef], levels: set[str]) -> list[str]:
        """Idnetify acceptable xrefs given a set of acceptable levels."""
        mappings: list[str] = []
        for xref in xrefs:
            if len(set(xref.sources) & levels) > 0:
                mappings.append(xref.ref_id)

        return mappings

    def _parse_levels(
        self, levels: list[str] | set[str], valid_levels: tuple[XRefLevel, ...]
    ) -> set[XRefLevel]:
        """Check passed levels attribute values."""
        accepted = set()
        for level in levels:
            if level not in valid_levels:
                self.logger.warning("%s not in supported levels. Skipping...", level)
            accepted.add(level)

        if len(accepted) == 0:
            raise ValueError(f"XRef levels must be in {valid_levels}")

        self.logger.info("Using xref levels: %s", accepted)
        return accepted


class Ontology:
    """This class contains functionalities for working with ontologies. Currently only supports
    ontologies stored in obo files.

    Attributes:
        entries (list[str]):
            Entries from the ontology that begin with the pattern [Term].

        _class_dict (dict[str, str]):
            Term ID to term name mapping (e.g., {MONDO:0006858: 'mouth disorder'}).

    Example:

        >>> from  metahq_build.ontology import Ontology
        >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")
    """

    def __init__(self):
        self._entries: list[OboEntry] = []
        self._class_dict: dict[str, str] = {}

        self.logger = setup_logger("metahq_build.ontology")

    def get_class_dict(self):
        """
        Fills the _class_dict attribute with id: name pairs.

        Arguments:
            verbose (bool):
                If True, will print redundant terms.

        """
        for entry in self.entries:
            self._class_dict[entry.id] = entry.name.lower()

    def xref(
        self,
        ref: str,
        levels: list[XRefLevel] | tuple[XRefLevel, ...] = DEFAULT_XREF_LEVELS,
    ) -> XRefMappings:
        """Finds cross references to other ontology terms.

        If there are cross references, this function will just choose the first
        one it comes across.

        Arguments:
            ref (str):
                The cross referenced ontology (e.g., MESH).

        Returns:
            _map (dict[str, str]):
                Mapping between terms in the ontology and the xref ontology.

        Example:

            >>> from metahq_build.ontology import Ontology
            >>> op = Ontology.from_obo("mondo.obo")
            >>> op.xref("MESH").pop("MONDO:0100340")
            ['MESH:C565561']
        """

        extractor = XRefExtractor(self.entries, levels=levels)
        mapping = extractor.get(ref)

        return mapping

    def read(self, file: Path | str, reader: str = "obo") -> None:
        """
        Loads and reads an ontology file.

        Arguments:
            file (str | Path):
                Path to ontology file.
            reader (str):
                File type to read from.

        Example:

            >>> from  metahq_build.ontology import Ontology
            >>> op = Ontology()
            >>> op.read("mondo.obo", reader="obo")
            >>> op.entries[0]
            OboEntry(
                id="MONDO:0000001",
                name="disease",
                def="A diease is a disposition to ...",
                ...,
                xrefs=[XRef(...), ...],
            )
        """
        if isinstance(reader, str):
            if reader == "obo":
                _reader = self.obo_reader
                loaded = _reader(file)
                self.entries = self.get_entries(loaded)
            else:
                raise ValueError(
                    f"Unknown reader {reader!r}, available options are [obo]",
                )
        self.logger.info("Built ontology from %s", file)

    def id_map(self, struct: str = "polars") -> dict[str, str] | pl.DataFrame:
        """Returns class_dict as specified data structure."""
        supported = ["polars", "dict"]

        if struct not in supported:
            raise ValueError(f"Expected struct in {supported}, got {struct}.")

        if struct == "polars":
            return self._id_map_to_polars()

        return self.class_dict

    def _id_map_to_polars(self):
        """Convert self.class_dict to polars DataFrame."""
        d = {"id": list(self.class_dict.keys()), "name": list(self.class_dict.values())}
        return pl.DataFrame(d)

    @property
    def class_dict(self) -> dict[str, str]:
        """Returns the dictionary storing terms IDs and their names."""
        if len(self._class_dict) == 0:
            self.get_class_dict()

        return self._class_dict

    @property
    def entries(self) -> list[OboEntry]:
        """Returns entries from the ontology."""
        return self._entries

    @entries.setter
    def entries(self, val):
        """Sets self.entries value."""
        if not isinstance(val, list):
            raise TypeError(f"Expected list, not {type(val)}.")

        for entry in val:
            if not isinstance(entry, OboEntry):
                raise TypeError(f"Expected OboEntry. Got {type(entry)}.")

        self._entries = val

    @staticmethod
    def get_entries(obo_text: str) -> list[OboEntry]:
        """Returns a list of entries from entries combined by \n\n"""
        entries = [
            OboEntry.from_text(entry)
            for entry in obo_text.split("\n\n")
            if (entry.startswith("[Term]")) and ("is_obsolete: true" not in entry)
        ]

        return entries

    @staticmethod
    def obo_reader(obo: Path | str) -> str:
        """Reads text from an obo file (plain or gzipped)."""
        obo = Path(obo)
        if obo.suffix == ".gz":
            with gzip.open(obo, "rt", encoding="utf-8") as f:
                text = f.read()
        else:
            with open(obo, "r", encoding="utf-8") as f:
                text = f.read()
        return text

    @staticmethod
    def reverse_dict(d: dict) -> dict:
        """Sets values as keys and keys as values."""
        _d = {}
        for key, val in d.items():
            _d[val] = key
        return _d

    @staticmethod
    def select_from_xref(term: str, _map: dict) -> str:
        """Pulls the xref for a query term."""
        if term in _map.keys():
            _id = _map[term]
        else:
            _id = "NA"

        return _id

    @classmethod
    def from_obo(cls, obo: Path | str):
        """Create Ontology class from an obo file."""
        parser = cls()
        parser.read(obo, reader="obo")
        return parser


def get_id_map(obo_file: Path) -> pl.DataFrame:
    """Return a term ID to name mapping."""
    onto = Ontology.from_obo(obo_file)
    return onto.id_map(struct="polars")


def get_xref(obo_file: Path, to: str, **kwargs):
    """Return a XRefMappings object with cross-references to ontology 'to'."""
    onto = Ontology.from_obo(obo_file)
    return onto.xref(to, **kwargs)
