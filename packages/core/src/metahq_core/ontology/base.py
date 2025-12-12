"""
Operations for ontologies. Currently, only UBERON, CL, and MONDO are supported.

Authors: Parker Hicks, Hao Yuan
Date: 2025-01-16

Last updated: 2025-09-01 by Parker Hicks
"""

import numpy as np
import polars as pl

from metahq_core.util.alltypes import FilePath
from metahq_core.util.helpers import reverse_dict


class Ontology:
    """
    This class contains functionalities for working with ontologies. Currently only supports
    ontologies stored in obo files.

    Attributes
    ----------
    ontology: str
        Name of the ontology (e.g., mondo, MONDO, uberon, CL).

    entries: list
        Entries from the ontology that begin with the pattern [Term].

    _class_dict: dict
    Term ID to term name mapping (e.g., {MONDO:0006858: 'mouth disorder'}).

    Methods
    -------
    get_class_dict()
        Retrieves ID: name pairs for terms in the ontology.

    xref()
        Retrives cross referenced terms from another ontology that are equivalent to terms
        in self.ontology.

    mesh_to_mondo()
        Given a MESH ID and an xref map, will find the corresponding MONDO ID.

    read()
        Opens and reads the ontology file.

    Static Methods
    --------------
    get_entries()
        Finds ontology term entries from lines in the ontology file.

    map_terms()
        Given a list of terms from another ontology, will map those to self.ontology terms.

    obo_reader()
        Opens and read the .obo file.

    Class Methods
    -------------
    mapping_func()
        Assigns the correct mapping function for `map_terms`.

    from_obo()
        Initializes the class by loading the ontology from an .obo file.

    Example
    -------
    >>> from ontology.ontology import Ontology
    >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")

    """

    def __init__(self, ontology: str):
        self.ontology = ontology
        self.entries = []
        self._class_dict = {}

    def get_class_dict(self, verbose=False) -> None:
        """
        Fills the _class_dict attribute with id: name pairs.

        :param verbose: If True, will print redundant terms.
        """
        for entry in self.entries:
            lines = entry.split("\n")
            for line in lines:
                line = line.rstrip()
                if line.startswith("id:"):
                    _id = line.split("id: ")[1]
                elif line.startswith("name:"):
                    name = line.split("name: ")[1]
                    if _id not in self._class_dict:
                        self._class_dict[_id] = name.lower()
                    elif verbose:
                        print(f"{_id} showing up more than once")

    def xref(self, ref: str, reverse=False, verbose=False) -> dict:
        """
        Finds cross references to other ontology terms within self.ontology.

        If there are cross references, this function will just choose the first
        one it comes across.

        :param ref: The cross referenced ontology (e.g., MESH).
        :param reverse: If True, will return {xref: term} instead of {term: xref}.
        :param verbose: If True, will print redundant cross references.
        :returns _map: Mapping between terms from self.ontology and the xref ontology.

        Example
        -------
        >>> from ontology.ontology import Ontology
        >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")
        >>> op.xref("MESH").pop("MONDO:0100340")
        MESH:C565561

        >>> op.xref("MESH", verbose=True)
        MESH:D006966 showing up more than once (duplicate: MONDO:0024305)
        MESH:C537897 showing up more than once (duplicate: MONDO:0043176)
        ...
        MESH:D000086382 showing up more than once (duplicate: MONDO:0100096)

        """
        _map = {}
        for entry in self.entries:
            lines = entry.split("\n")
            for line in lines:
                line = line.rstrip()
                if line.startswith("id:"):
                    _id = line.split("id: ")[1]
                elif line.startswith(f"xref: {ref}:"):
                    __id = line.split(" ")[1]
                    if _id not in _map:
                        _map[_id] = __id
                    elif verbose:
                        print(f"{__id} showing up more than once (duplicate: {_id})")
        if reverse:
            _map = reverse_dict(_map)

        return _map

    def map_terms(self, terms: np.ndarray, _from: str, _to: str) -> dict:
        """
        Maps term IDs from a list of terms to another ontology.

        :param terms: Array of term IDs to map.
        :param _from: The ontology type of the IDs in terms.
        :param _to: The ontology type you want to map those terms to.
        :returns mapped: Mapping between terms in terms to _to.

        Example
        -------
        >>> from ontology.ontology import Ontology
        >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")
        >>> op.map_terms(mesh, _from="MESH", _to="MONDO").pop("MESH:D007680")
        MONDO:0002367

        >>> op.map_terms(mesh, _from="MONDO", _to="MESH").pop("MONDO:0002367")
        MESH:D007680

        """
        if _to == self.ontology:
            reverse = True
            ref = _from
        else:
            reverse = False
            ref = _to

        _map = self.xref(ref=ref, reverse=reverse)

        mapped = {}
        mapper = self.mapping_func(_from, _to, self.ontology)
        for term in terms:
            mapped[term] = mapper(term, _map)

        return mapped

    def read(self, file: FilePath, reader="obo") -> None:
        """
        Loads and reads an ontology file.

        :param file: Path to ontology file.
        :param reader: File type to read from.

        Example
        -------
        >>> from ontology.ontology import Ontology
        >>> op = Ontology(ontology="mondo")
        >>> op.read("mondo.obo", reader="obo")
        >>> op.entries[0]
        [Term]
        id: MONDO:0000001
        name: disease
        def: "A diease is a disposition to ..."
        ...
        property_value: exactMatch Orphannet:377788

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

    def id_map(self, fmt: str = "polars") -> dict[str, str] | pl.DataFrame:
        """Returns class_dict as specified data structure."""
        supported = ["polars", "dict"]

        if fmt not in supported:
            raise ValueError(f"Expected struct in {supported}, got {fmt}.")

        if fmt == "polars":
            return self._id_map_to_polars()

        return self.class_dict

    def _id_map_to_polars(self):
        """Convert self.class_dict to polars DataFrame."""
        d = {"id": list(self.class_dict.keys()), "name": list(self.class_dict.values())}
        return pl.DataFrame(d)

    @property
    def class_dict(self) -> dict:
        """Returns the dictionary storing terms IDs and their names."""
        if len(self._class_dict) == 0:
            self.get_class_dict()

        return self._class_dict

    @property
    def entries(self) -> list:
        """Returns entries from the ontology."""
        return self._entries

    @entries.setter
    def entries(self, val):
        """Sets self.entries value."""
        if not isinstance(val, list):
            raise TypeError(f"Expected list, not {type(val)}.")
        self._entries = val

    @staticmethod
    def get_entries(obo_text: str) -> list:
        """Returns a list of entries from entries combined by \n\n"""
        entries = [
            entry
            for entry in obo_text.split("\n\n")
            if (entry.startswith("[Term]")) and ("is_obsolete: true" not in entry)
        ]

        return entries

    @staticmethod
    def doid_to_mondo(doid: str, _map: dict[str, str]) -> str:
        """
        Maps a single DOID to MONDO id.

        Args
        ----
        doid: str
            DOID id (e.g. D000324)
        _map: dict[str, str]
            Dict with DOID keys and MONDO values

        Returns
        -------
        _id:  str
            Mapped id

        Example
        -------
        >>> from ontology_parser import Ontology
        >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")
        >>> _map = op.xref("DOID")
        >>> op.mesh_to_mondo("DOID:299", _map)
        MONDO:0004970

        """
        if doid == "DOID:0000000":
            _id = "control"
        elif doid in _map.keys():
            _id = _map[doid]
        else:
            _id = "NA"

        return _id

    @staticmethod
    def mesh_to_mondo(mesh: str, _map: dict) -> str:
        """
        Maps a single MESH to MONDO id.
        :param mesh: MESH id (e.g. D000324)
        :param _map: Dict with MESH keys and MONDO values
        :return _id: Mapped id
        Example
        -------
        >>> from ontology_parser import Ontology
        >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")
        >>> _map = op.xref("MESH")
        >>> op.mesh_to_mondo("MESH:D007680", _map)
        MONDO:0002367
        """
        if mesh == "MESH:D000000":
            _id = "control"
        elif mesh in _map.keys():
            _id = _map[mesh]
        else:
            _id = "NA"

        return _id

    @staticmethod
    def umls_to_mondo(umls: str, _map: dict) -> str:
        """
        Maps a single UMLS to MONDO id.
        :param mesh: UMLS id (e.g. D000324)
        :param _map: Dict with UMLS keys and MONDO values
        :return _id: Mapped id
        Example
        -------
        >>> from ontology_parser import Ontology
        >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")
        >>> _map = op.xref("UMLS")
        >>> op.mesh_to_mondo("UMLS:C2673913", _map)
        MONDO:0000104
        """
        if umls == "UMLS:C0000000":
            _id = "MONDO:0000000"
        elif umls in _map.keys():
            _id = _map[umls]
        else:
            _id = "NA"

        return _id

    @staticmethod
    def obo_reader(obo: FilePath) -> str:
        """Reads text from an obo file."""
        with open(obo, "r", encoding="utf-8") as f:
            text = f.read()
        return text

    @staticmethod
    def select_from_xref(term: str, _map: dict) -> str:
        """Pulls the xref for a query term."""
        if term in _map.keys():
            _id = _map[term]
        else:
            _id = "NA"

        return _id

    @classmethod
    def from_obo(cls, obo: FilePath, ontology: str):
        """Create Ontology class from an obo file."""
        parser = cls(ontology=ontology.upper())
        parser.read(obo, reader="obo")
        return parser

    @classmethod
    def mapping_func(cls, _from: str, _to: str, ontology: str) -> object:
        """Assigns the correct mapping function for mapping xref terms."""
        if _from == "MESH" and _to == "MONDO" and ontology == "MONDO":
            return cls.mesh_to_mondo
        if _from == "UMLS" and _to == "MONDO" and ontology == "MONDO":
            return cls.umls_to_mondo
        if _from == "MONDO" and _to == "MESH" and ontology == "MONDO":
            return cls.select_from_xref
        if _from == "DOID" and _to == "MONDO" and ontology == "MONDO":
            return cls.doid_to_mondo

        raise NotImplementedError()
