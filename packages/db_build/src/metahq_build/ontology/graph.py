import re
from pathlib import Path

import networkx as nx
import numpy as np
from numpy.typing import DTypeLike
from tqdm import tqdm

from metahq_build.ontology.ontology import Ontology
from metahq_build.ontology.relations import RelationsMatrix
from metahq_build.util.logging import setup_logger


class Graph(Ontology):
    """This class provides functionalities for creating and operating on ontology knowledge graphs.
    See Ontology documentation for inherited attributes and methods.

    Example:

        >>> from  metahq_build.ontology import Graph
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

        self.logger = setup_logger("metahq_build.ontology.graph.Graph")

    def construct_graph(self):
        """Constructs an ontology graph from entries from an ontology file.

        A simple cycle occurs between 2 nodes UBERON:8000009 and UBERON:0002354
        (cardiac Purkinje fiber network and cardiac Purkinje fiber)
        They are both parents and children of eachother, so to preserve the
        directed acyclic structure of the edgelist, we intentionally keep only one
        edge (fiber network is parent of fiber) on Line 100.
        """
        self.logger.info("Constructing the ontology graph...")

        for entry in self.entries:
            if (
                ("UBERON" not in entry.id)
                and ("CL" not in entry.id)
                and ("MONDO" not in entry.id)
            ):
                continue

            # Get is_a connection from the reference term to another
            for parent in entry.is_a:
                if ("UBERON" in parent) or ("CL" in parent) or ("MONDO" in entry.id):
                    self._graph.add_edge(parent, entry.id)

            # Get part_of connections
            # Ignoring 'develops from' and 'related to'
            for parent in entry.part_of:
                if ("UBERON" in parent) or ("CL" in parent) or ("MONDO" in entry.id):
                    # If parent is the fiber and child is the fiber network, then leave that edge out
                    if entry.id == "UBERON:8000009" and parent == "UBERON:0002354":
                        continue
                    self._graph.add_edge(parent, entry.id)

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

            >>> from  metahq_build.ontology import Graph
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

            >>> from  metahq_build.ontology import Graph
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

    def relations_matrix(self, dtype: DTypeLike = np.int8) -> "RelationsMatrix":
        """Construct a term x term matrices defining ancestor and descendant relationships.

        You may interpret the output matrix as the following: For any row, column pair, if the
        value is 1, then the term representing that particular row is an ancestor of the term
        representing that particular column. If the value is 0, then there is no relationship
        between the terms.

        Arguments:
            dtype (DTypeLike):
                A string representing a dtype that can be coerced into a np.dtype. Can be a
                    numpy dtype (e.g., np.int8, np.int32) a python builtin dtype (e.g., int, float),
                    or a string (e.g., 'int8', ''>i4').

        Returns:
            (RelationsMatrix): A RelationsMatrix object storing the relationships matrix and terms
                representing the rows and columns.
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

        return RelationsMatrix(matrix=propagated_relations.T, terms=np_nodes)

    def deepest_node(self, query: list[str]) -> str:
        """Find the deepest node using breadth first search from root nodes.

        Arguments:
            query (list[str]):
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
