import gzip
from pathlib import Path

import polars as pl

from metahq_build.util.logging import setup_logger


class Ontology:
    """This class contains functionalities for working with ontologies. Currently only supports
    ontologies stored in obo files.

    Attributes:
        ontology (str):
            Name of the ontology (e.g., mondo, MONDO, uberon, CL). Default is 'none'. If ontology
            is left as 'none' cross-reference functions will not be available.

        entries (list[str]):
            Entries from the ontology that begin with the pattern [Term].

        _class_dict (dict[str, str]):
            Term ID to term name mapping (e.g., {MONDO:0006858: 'mouth disorder'}).

    Example:
        >>> from ontology.ontology import Ontology
        >>> op = Ontology.from_obo("mondo.obo", ontology="mondo")
    """

    def __init__(self):
        self._entries: list[str] = []
        self._class_dict: dict[str, str] = {}

        self.logger = setup_logger("metahq_build.ontology")

    def get_class_dict(self, verbose: bool = False):
        """
        Fills the _class_dict attribute with id: name pairs.

        Arguments:
            verbose (bool):
                If True, will print redundant terms.

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

    def xref(
        self, ref: str, reverse: bool = False, verbose: bool = False
    ) -> dict[str, str]:
        """Finds cross references to other ontology terms.

        If there are cross references, this function will just choose the first
        one it comes across.

        Arguments:
            ref (str):
                The cross referenced ontology (e.g., MESH).

            reverse (bool):
                If True, will return {xref: term} instead of {term: xref}.

            verbose (bool):
                If True, will print redundant cross references.

        Returns:
            _map (dict[str, str]):
                Mapping between terms in the ontology and the xref ontology.

        Example:
            >>> from txt2onto.ontology import Ontology
            >>> op = Ontology.from_obo("mondo.obo")
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
            _map = self.reverse_dict(_map)

        return _map

    def map_terms(
        self, terms: list[str], ontology: str, _from: str, _to: str
    ) -> dict[str, str]:
        """Maps term IDs from a list of terms to another ontology.

        Arguments:
            terms (list[str]):
                Array of term IDs to map.

            ontology (str):
                Name of the ontology (e.g., MONDO, UBERON, CL). Required to choose the appropriate
                mapping function.

             _from (str):
                The ontology type of the IDs in terms.

             _to (str):
                The ontology type you want to map those terms to.

        Returns:
            mapped (dict[str, str]):
                Mapping between terms in terms to _to.

        Example:
            >>> from txt2onto.ontology import Ontology
            >>> op = Ontology.from_obo("mondo.obo")
            >>> op.map_terms(mesh, ontology="MONDO", _from="MESH", _to="MONDO").pop("MESH:D007680")
            MONDO:0002367
            >>> op.map_terms(mesh, ontology="MONDO", _from="MONDO", _to="MESH").pop("MONDO:0002367")
            MESH:D007680
        """
        if _to == ontology:
            reverse = True
            ref = _from
        else:
            reverse = False
            ref = _to

        _map = self.xref(ref=ref, reverse=reverse)

        mapped = {}
        mapper = self.mapping_func(_from, _to, ontology)
        for term in terms:
            mapped[term] = mapper(term, _map)

        return mapped

    def read(self, file: Path | str, reader: str = "obo") -> None:
        """
        Loads and reads an ontology file.

        Arguments:
            file (str | Path):
                Path to ontology file.
            reader (str):
                File type to read from.

        Example:
            >>> from txt2onto.ontology import Ontology
            >>> op = Ontology()
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
    def entries(self) -> list[str]:
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
    def doid_to_mondo(doid: str, _map: dict) -> str:
        """Maps a single DOID to MONDO id.

        Arguments:
            mesh (str):
                DOID id (e.g. DOID:0050700).

            _map (dict[str, str]):
                Dict with DOID keys and MONDO values.

        Returns:
            _id (str)
                Mapped id.

        Example:
            >>> from txt2onto.ontology import Ontology
            >>> op = Ontology.from_obo("mondo.obo")
            >>> _map = op.xref("DOID")
            >>> op.mesh_to_mondo("DOID:0050700", _map)
            MONDO:0004994
        """
        if doid == "DOID:0000000":
            _id = "MONDO:0000000"
        elif doid in _map.keys():
            _id = _map[doid]
        else:
            _id = "NA"

        return _id

    @staticmethod
    def mesh_to_mondo(mesh: str, _map: dict) -> str:
        """Maps a single MESH to MONDO id.

        Arguments:
            mesh (str):
                MESH id (e.g. D000324).

            _map (dict[str, str]):
                Dict with MESH keys and MONDO values.

        Returns:
            _id (str)
                Mapped id.

        Example:
            >>> from txt2onto.ontology import Ontology
            >>> op = Ontology.from_obo("mondo.obo")
            >>> _map = op.xref("MESH")
            >>> op.mesh_to_mondo("MESH:D007680", _map)
            MONDO:0002367
        """
        if mesh == "MESH:D000000":
            _id = "MONDO:0000000"
        elif mesh in _map.keys():
            _id = _map[mesh]
        else:
            _id = "NA"

        return _id

    @staticmethod
    def umls_to_mondo(umls: str, _map: dict) -> str:
        """Maps a single UMLS to MONDO id.

        Arguments:
            mesh (str):
                UMLS id (e.g. D000324).

            _map (dict[str, str]):
                Dict with UMLS keys and MONDO values.

        Returns:
            _id (str):
                Mapped id.

        Example:
            >>> from txt2onto.ontology import Ontology
            >>> op = Ontology.from_obo("mondo.obo")
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

    @classmethod
    def mapping_func(cls, _from: str, _to: str, ontology: str) -> object:
        """Assigns the correct mapping function for mapping xref terms."""
        if _from == "MESH" and _to == "MONDO" and ontology == "MONDO":
            return cls.mesh_to_mondo

        if _from == "UMLS" and _to == "MONDO" and ontology == "MONDO":
            return cls.umls_to_mondo

        if _from == "DOID" and _to == "MONDO" and ontology == "MONDO":
            return cls.doid_to_mondo

        if _from == "MONDO" and _to == "MESH" and ontology == "MONDO":
            return cls.select_from_xref

        raise NotImplementedError("No mapping function for {_from}->{_to}.")


def get_id_map(obo_file: Path) -> pl.DataFrame:
    """Return a term ID to name mapping."""
    onto = Ontology.from_obo(obo_file)
    return onto.id_map(struct="polars")
