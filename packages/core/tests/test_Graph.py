"""
This script contains a series of tests of the `Graph` child class of `Ontology`.

Author: Parker Hicks
Date: 2025-02-11

Last updated: 2025-09-02 by Parker Hicks
"""

from pathlib import Path

import numpy as np
import pytest

from ontology.graph import Graph

ROOT_DIR = Path(__file__).resolve().parents[3]
MONDO_OBO = ROOT_DIR / "data/ontology/mondo/mondo.obo"
UBERON_OBO = ROOT_DIR / "data/ontology/uberon_ext/uberon_ext.obo"

OBO_FILES: list[tuple[str, Path]] = [
    ("mondo", MONDO_OBO),
    ("uberon", UBERON_OBO),
]


class TestGraph:
    """
    Class to test functionalities of Graph. Functionalities inherited from the
    `Ontology` class are tested in `test_Ontology.py` and will not be tested here.
    """

    # Test cases for `descendants_from`
    @pytest.mark.parametrize(
        "inputs",
        [
            {
                "obo": OBO_FILES[0],
                "query": ["MONDO:0043544", "MONDO:0045024"],
                "true": {
                    "MONDO:0043544": [
                        "MONDO:0016778",  # Most specific
                        "MONDO:0034976",  # Most specific
                        "MONDO:0005188",  # Most specific
                    ],
                    "MONDO:0045024": [
                        "MONDO:0005043",  # Direct child of high-level condition
                        "MONDO:0000963",  # Most specific
                        "MONDO:0004420",  # Most specific
                    ],
                },
            },  # MONDO test
            {
                "obo": OBO_FILES[1],
                "query": [
                    "UBERON:0000948",
                    "UBERON:0001113",
                ],
                "true": {
                    "UBERON:0000948": [
                        "UBERON:0002098",  # Direct child
                        "UBERON:0002135",  # Very specific
                        "UBERON:0011745",  # Most specific
                    ],
                    "UBERON:0001113": [
                        "UBERON:0001115",  # Direct child
                        "UBERON:0009548",  # Moderate specificity
                        "UBERON:0011737",  # Most specific
                    ],
                },
            },  # UBERON test
        ],
    )
    def test_descendants_from(
        self,
        inputs: dict,
    ):
        """Checks descendant identification."""
        obo_init = inputs["obo"]

        graph = Graph.from_obo(obo=obo_init[1], ontology=obo_init[0])
        descendants = graph.descendants_from(inputs["query"])

        for ancestor, children in descendants.items():
            assert np.all(
                np.isin(inputs["true"][ancestor], children)
            ), f"Test children for {ancestor} not in descendants."

    # Test cases for `ancestors_from`
    @pytest.mark.parametrize(
        "inputs",
        [
            {
                "obo": OBO_FILES[0],
                "query": [
                    "MONDO:0022424",
                    "MONDO:0004652",
                ],
                "true": {
                    "MONDO:0022424": [
                        "MONDO:0009561",
                        "MONDO:0019251",
                        "MONDO:0019755",
                    ],  # In decreasing orer of specificity
                    "MONDO:0004652": [
                        "MONDO:0043905",
                        "MONDO:0005275",
                        "MONDO:0005087",
                    ],
                },
            },
            {
                "obo": OBO_FILES[1],
                "query": [
                    "UBERON:0001982",
                    "UBERON:0000162",
                ],
                "true": {
                    "UBERON:0001982": [
                        "UBERON:8410081",
                        "UBERON:0004535",
                        "UBERON:0000061",
                    ],
                    "UBERON:0000162": [
                        "UBERON:0001555",
                        "UBERON:0001007",
                        "UBERON:0000465",
                    ],
                },
            },
        ],
    )
    def test_ancestors_from(
        self,
        inputs: dict,
    ):
        """Test function to get ancestors from a list of queries"""
        obo_init = inputs["obo"]

        graph = Graph.from_obo(obo=obo_init[1], ontology=obo_init[0])
        ancestors = graph.ancestors_from(inputs["query"])

        for child, extended_family in ancestors.items():
            assert np.all(
                np.isin(inputs["true"][child], extended_family)
            ), f"Test children for {child} not in ancestors."

    def test_propagate_term(self):
        pass
