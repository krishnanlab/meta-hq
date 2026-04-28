"""
Operations for ontologies. Currently, only UBERON, CL, and MONDO are supported.

Authors: Parker Hicks, Hao Yuan
Date: 2026-04-03

Last updated: 2026-04-27 by Parker Hicks
"""

import gzip
import re
from pathlib import Path

import networkx as nx
import numpy as np
import polars as pl
from numpy.typing import DTypeLike, NDArray
from tqdm import tqdm


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


class Graph(Ontology):
    """This class provides functionalities for creating and operating on ontology knowledge graphs.
    See Ontology documentation for inherited attributes and methods.

    Example:
        >>> from txt2onto.ontology import Graph
        >>> ontograph = Graph.from_obo("mondo.obo")
        >>> ontograph.graph
        DiGraph with 23314 nodes and 35351 edges

        >>> ontograph.nodes
        ['MONDO:0002816' 'MONDO:0000004' 'MONDO:0021034' ... 'MONDO:8000019'
         'MONDO:8000023' 'MONDO:8000024']

        >>> ontograph.leaves
        ['MONDO:0000082' 'MONDO:0000138' 'MONDO:0000208' ... 'MONDO:8000019'
         'MONDO:8000023' 'MONDO:8000024']

        >>> ontograph.class_dict["MONDO:0021054"]
        bone sarcoma
    """

    def __init__(self):
        """Initialize Graph object as a child of Ontology"""
        super().__init__()
        self._graph = nx.DiGraph()
        self._nodes: list[str] = []

    def construct_graph(self):
        """Constructs an ontology graph from entries from an ontology file.

        A simple cycle occurs between 2 nodes UBERON:8000009 and UBERON:0002354
        (cardiac Purkinje fiber network and cardiac Purkinje fiber)
        They are both parents and children of eachother, so to preserve the
        directed acyclic structure of the edgelist, we intentionally keep only one
        edge (fiber network is parent of fiber) on Line 100.
        """
        # ID entries have at least 1 capital letter, a colon, and at least 1 digit
        id_pattern = re.compile(r"[A-Za-z]+:\S+")

        for entry in self.entries:
            if "is_obsolete: true" in entry:
                continue  # skip obsolete entries

            lines = entry.split("\n")
            for line in lines:

                # Get ID of the term
                if line.startswith("id:"):
                    _id = line.split("id: ")[1]
                    if ("UBERON" in _id) or ("CL" in _id) or ("MONDO" in _id):
                        pass
                    else:
                        break

                # Get is_a connection from the reference term to another
                elif line.startswith("is_a:"):
                    parent = id_pattern.search(line).group(0)
                    if ("UBERON" in parent) or ("CL" in parent) or ("MONDO" in _id):
                        self._graph.add_edge(parent, _id)

                # Get part_of connections
                # Ignoring 'develops from' and 'related to'
                elif line.startswith("relationship: part_of"):
                    parent = id_pattern.search(line).group(0)
                    if ("UBERON" in parent) or ("CL" in parent) or ("MONDO" in _id):
                        # If parent is the fiber and child is the fiber network, then leave that edge out
                        if _id == "UBERON:8000009" and parent == "UBERON:0002354":
                            continue
                        self._graph.add_edge(parent, _id)

    def descendants_from(
        self, nodes: list[str], verbose: bool = False
    ) -> dict[str, list[str]]:
        """Retrieves descendants from an array of parent nodes.

        Arguments:
            nodes (list[str]):
                IDs in self.nodes for which to find desendants.
            verbose (bool):
                If True, will print nodes not in the graph.

        Returns:
            _map (dict[str, list[str]]):
                Mapping between parents (keys) and their children (values).

        Example:
            >>> from txt2onto.ontology import Graph
            >>> ontograph = Graph.from_obo("mondo.obo")
            >>> ontograph.descendants_from(['MONDO:0005071', 'MONDO:0043543'])
            {'MONDO:0005071': ['MONDO:0019438' ... 'MONDO:0100070'],
             'MONDO:0043543': ['MONDO:0043544' ... 'MONDO:0005188']}
        """
        _map = {}
        for node in nodes:
            if node in self.nodes:
                _map[node] = list(nx.descendants(self.graph, node))
            elif verbose:
                print(f"{node} not in graph.")

        return _map

    def ancestors_from(
        self, nodes: list[str], verbose: bool = False
    ) -> dict[str, list[str]]:
        """Retrieves ancestors from an array of parent nodes.

        Arguments:
            nodes (list[str]):
                IDs in self.nodes for which to find ancestors.
            verbose (bool):
                If True, will print nodes not in the graph.

        Returns:
            _map (dict[str, list[str]]):
                Mapping between parents (keys) and their children (values).

        Example:
            >>> from txt2onto.ontology import Graph
            >>> ontograph = Graph.from_obo("mondo.obo")
            >>> ontograph.ancestors_from(['MONDO:0008791', 'MONDO:0043209'])
            {'MONDO:0008791': ['MONDO:0019042' ... 'MONDO:0021147'],
             'MONDO:0043209': ['MONDO:0700096' ... 'MONDO:0004736']}
        """

        _map = {}
        for node in nodes:
            if node in self.nodes:
                _map[node] = list(nx.ancestors(self.graph, node))
            elif verbose:
                print(f"{node} not in graph.")

        return _map

    def relations_matrix(self, dtype: DTypeLike = np.int8) -> tuple[NDArray, NDArray]:
        """Construct a term x term matrices defining ancestor and descendant relationships.

        You may interpret the output matrix as the following: For any row, column pair, if the
        value is 1, then the term representing that particular row is a descendant of the term
        representing that particular column. If the value is 0, then there is no relationship
        between the terms.

        Arguments:
            dtype (DTypeLike):
                A string representing a dtype that can be coerced into a np.dtype. Can be a
                    numpy dtype (e.g., np.int8, np.int32) a python builtin dtype (e.g., int, float),
                    or a string (e.g., 'int8', ''>i4').


        Returns:
            (NDArray): A terms x terms matrix defining their relationships in the ontology.
            (NDArray): An array of terms representing the columns and rows of the family matrix.
        """
        # extract node relationships
        ancestor_relationships = self.ancestors_from(self.nodes)
        propagated_relations = np.identity(len(self.nodes), dtype=np.dtype(dtype))

        # convert iterables to numpy arrays for quick search
        np_nodes = np.array(self.nodes)
        ancestor_relationships = {
            k: np.array(v) for k, v in ancestor_relationships.items()
        }

        # construct the relations matrix
        for i, relatives in tqdm(
            enumerate(ancestor_relationships.values()),
            total=len(ancestor_relationships),
            desc="Constructing family tree",
        ):
            # relatives = ancestor_relationships[node]
            related = np.where(np.isin(np_nodes, relatives))[0]
            propagated_relations[i, related] = 1

        return propagated_relations, np_nodes

    def deepest_node(self, query: list[str]) -> str:
        """Find the deepest node using breadth first search from root nodes.

        Arguments:
            query (list[str])
                An array of nodes for which to find the deepest node.

        Returns:
            deepest (str): The deepest node out of all query nodes.

        """
        subset_nodes = set(node for node in query if node in self.graph)

        # Find roots
        roots = [node for node in self.graph if self.graph.in_degree(node) == 0]
        if not roots:
            roots = list(self.graph.nodes())[0:1]

        # Track depths from all roots
        depths = {node: 0 for node in self.graph}

        # Run BFS from each root
        for root in roots:
            visited = {node: False for node in self.graph}
            visited[root] = True
            queue = [(root, 0)]  # (node, depth)

            while queue:
                current, depth = queue.pop(0)
                depths[current] = max(depths[current], depth)

                for neighbor in self.graph.successors(current):
                    if not visited[neighbor]:
                        visited[neighbor] = True
                        queue.append((neighbor, depth + 1))

        # Find deepest node from the subset
        max_depth = -1
        deepest = None

        for node in subset_nodes:
            if depths[node] > max_depth:
                max_depth = depths[node]
                deepest = node

        return deepest

    def propagate_term(self, query: str, ref_term: str) -> int:
        """Sets label for ontology terms"""
        if query in self.descendants(ref_term):
            return 1
        elif query in self.ancestors(ref_term):
            return 0
        else:
            return -1

    def ancestors(self, term: str) -> list:
        """Gets ancestors of a single term"""
        return list(nx.ancestors(self.graph, term))

    def descendants(self, term: str) -> list:
        """Gets descendants of a single term"""
        return list(nx.descendants(self.graph, term))

    @property
    def graph(self) -> nx.DiGraph:
        """Return the ontology DiGraph"""
        if self._graph.number_of_nodes() == 0:
            self.construct_graph()

        return self._graph

    @property
    def nodes(self) -> list[str]:
        """Return the IDs of the graph nodes"""
        if len(self._nodes) == 0:
            self._nodes = sorted(list(self.graph.nodes()))

        return self._nodes

    @property
    def leaves(self) -> list[str]:
        """Return leaf nodes of the ontology"""
        return [node for node in self.nodes if self.graph.out_degree(node) == 0]


def get_id_map(obo_file: Path) -> pl.DataFrame:
    """Return a term ID to name mapping."""
    onto = Ontology.from_obo(obo_file)
    return onto.id_map(struct="polars")


def get_system_descendants(systems_file: Path, obo_file: Path) -> frozenset[str]:
    """Return all valid term IDs that are descendants of system-level terms.

    Loads the ontology graph from an OBO file, then collects all descendants
    of every term listed in the systems file. The system terms themselves are
    excluded from the returned set (only their descendants are included).

    Arguments:
        systems_file (Path):
            Path to a plain-text file with one ontology term ID per line
            (e.g. ``data/ontology/mondo/systems.txt``).
        obo_file (Path):
            Path to the OBO or gzipped OBO file for the ontology.

    Returns:
        (frozenset[str]): All term IDs that are descendants of system-level
            terms (excluding the system-level terms themselves).
    """
    systems = Path(systems_file).read_text().strip().splitlines()
    graph = Graph.from_obo(obo_file)
    descendants_map = graph.descendants_from(systems)
    valid: set[str] = set()
    for desc_list in descendants_map.values():
        valid.update(desc_list)
    return frozenset(valid)
