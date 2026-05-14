"""
Tools for using ontologies, constructing ontology graphs, and precomputing ontology relations.
"""

from metahq_build.ontology.graph import Graph, get_system_descendants
from metahq_build.ontology.ontology import Ontology, get_id_map
from metahq_build.ontology.relations import RelationsLazyFrame, RelationsMatrix

__all__ = [
    "Graph",
    "Ontology",
    "RelationsMatrix",
    "RelationsLazyFrame",
    "get_id_map",
    "get_system_descendants",
]
