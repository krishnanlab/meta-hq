"""
Tools for using ontologies, constructing ontology graphs, and precomputing ontology relations.
"""

from metahq_setup.ontology.graph import Graph, get_system_descendants
from metahq_setup.ontology.ontology import Ontology, get_id_map
from metahq_setup.ontology.relations import RelationsMatrix

__all__ = [
    "Graph",
    "Ontology",
    "RelationsMatrix",
    "get_id_map",
    "get_system_descendants",
]
