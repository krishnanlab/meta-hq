"""
Operations for creating and using directed acyclic graphs (DAGs) constructed from ontologies.

Authors: Parker Hicks, Hao Yuan
Date: 2025-01-18

Last updated: 2025-09-01 by Parker Hicks
"""

import re

import networkx as nx
import numpy as np

from ontology.base import Ontology
from util.alltypes import IdArray


class Graph(Ontology):
    """
    This class provides functionalities for creating and operating on ontology knowledge graphs.
    See Ontology documentation for inherited attributes and methods.

    Methods
    -------
    ancestors()
        Returns the ancestors of a single node.

    ancestors_from()
        Returns ancestors for each node in a list of nodes.

    construct_graph()
        Constructs the ontology directed acyclic graph.

    descendants()
        Returns the descendants of a single node.

    descendants_from()
        Returns descendants from a list of nodes.

    propagate_term()
        Will return a -1,0,1 label given a query term and reference term.

    Example
    -------
    >>> from ontology.graph import Graph
    >>> ontograph = Graph.from_obo("mondo.obo", ontology="mondo")
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

    def __init__(self, ontology: str):
        """Initialize Graph object as a child of Ontology"""
        super().__init__(ontology=ontology)
        self._graph = nx.DiGraph()

    def construct_graph(self):
        """
        Constructs an ontology graph from entries from an ontology file.

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

    def descendants_from(self, nodes: np.ndarray | list, verbose=False) -> dict:
        """
        Retrieves descendants from an array of parent nodes.

        :param nodes: IDs in self.nodes for which to find desendants.
        :param verbose: If True, will print nodes not in the graph
        :returns _map: Mapping between parents (keys) and their children (values).

        Example
        -------
        >>> from ontology.graph import Graph
        >>> ontograph = Graph.from_obo("mondo.obo", ontology="mondo")
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

    def ancestors_from(self, nodes: np.ndarray | list, verbose=False) -> dict:
        """
        Retrieves ancestors from an array of parent nodes.

        :param nodes: IDs in self.nodes for which to find desendants.
        :param verbose: If True, will print nodes not in the graph
        :returns _map: Mapping between parents (keys) and their children (values).

        Example
        -------
        >>> from ontology.graph import Graph
        >>> ontograph = Graph.from_obo("mondo.obo", ontology="mondo")
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

    def deepest_node(self, query: IdArray) -> str:
        """
        Find the deepest node using breadth first search from root nodes.

        Args
        ----
        query: (IdArray)
            An array of nodes for which to find the deepest node.

        Returns
        -------
        deepest: (str)
            The deepest node out of all query nodes.

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
    def nodes(self) -> np.ndarray:
        """Return the IDs of the graph nodes"""
        return np.array(self.graph.nodes())

    @property
    def leaves(self) -> np.ndarray:
        """Return leaf nodes of the ontology"""
        return np.array(
            [node for node in self.nodes if self.graph.out_degree(node) == 0]
        )
