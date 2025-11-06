"""
This script tests functionalities for the Ontology class.

Author: Parker Hicks
Date: 2025-02-11

Last updated: 2025-11-05 by Parker Hicks
"""

import numpy as np
import pytest

from metahq_core.ontology.base import Ontology
from metahq_core.util.supported import get_ontology_files, supported

test_entries = {
    2: """[Term]\nid: MONDO:0000005\nname: alopecia, isolated\nxref: OMIMPS:203655 {source="MONDO:equivalentTo"}\nis_a: MONDO:0021034 ! hereditary alopecia\nproperty_value: exactMatch https://omim.org/phenotypicSeries/PS203655""",
    4: "[Term]\nid: MONDO:0000014\nname: colorblindness, partial\nis_a: MONDO:0001703 ! color vision disorder",
}

ONTOLOGIES = supported("ontologies")


class TestOntology:
    """
    Class to test functionalities from the Ontology class.
    """

    @pytest.mark.parametrize("ontology", ONTOLOGIES)
    def test_from_obo(self, ontology: str):
        """Tests that no errors arise when initializing the Graph."""
        _ = Ontology.from_obo(obo=get_ontology_files(ontology), ontology=ontology)
        # No error should be raised

    def test_get_entries(self, ontology: str = "mondo"):
        """
        Check expected entries. Just test with MONDO.

        Output should be a list of term entries where each key, value of
        the term entry is separated by the \n character and new term entries
        are separated by the \n\n characters.
        """
        op = Ontology.from_obo(obo=get_ontology_files(ontology), ontology=ontology)

        for idx, entry in test_entries.items():
            assert op.entries[idx] == entry

    @pytest.mark.parametrize("ontology", ONTOLOGIES)
    @pytest.mark.parametrize("val", ["1", "a", 1, {}])
    def test_entries_setter(self, ontology: str, val):
        """
        Checks type checking of entries setter.

        It's expected that an error will be raised if self.entries is not
        type: list.
        """
        op = Ontology.from_obo(obo=get_ontology_files(ontology), ontology=ontology)
        with pytest.raises(TypeError):
            op.entries = val

    @pytest.mark.parametrize(
        "pair",
        [
            ("MONDO:0000009", "inherited bleeding disorder, platelet-type"),
            ("MONDO:0008779", "arthrogryposis"),
            ("MONDO:0016829", "familial visceral myopathy"),
        ],
    )
    def test_class_dict(self, pair):
        """
        Checks id: name dictionary entries.

        It's expected that self.class_dict returns the correct ID to name mappings
        for all terms in the ontology.
        """
        op = Ontology.from_obo(obo=get_ontology_files("mondo"), ontology="mondo")

        _id, name = pair

        assert op.class_dict[_id] == name

    @pytest.mark.parametrize(
        "pair",
        [
            ("MONDO:0016830", "MESH:D020389"),
            ("MONDO:0018477", "MESH:D007647"),
            ("MONDO:0003039", "MESH:D000849"),
        ],
    )
    def test_xref(self, pair):
        """
        Tests the cross-reference function to find associated terms from other ontologies.

        It's expected that xref correctly identifies cross references between IDs.
        """
        op = Ontology.from_obo(obo=get_ontology_files("mondo"), ontology="mondo")

        _map = op.xref(ref="MESH")

        _from, _to = pair

        assert _map[_from] == _to

    @pytest.mark.parametrize(
        "pair",
        [
            {
                "terms": ["MONDO:0016390", "MONDO:0018477"],
                "correct": ["MESH:C537156", "MESH:D007647"],
                "from": "MONDO",
                "to": "MESH",
            },
            {
                "terms": ["MESH:C537156", "MESH:D007647"],
                "correct": ["MONDO:0016390", "MONDO:0018477"],
                "from": "MESH",
                "to": "MONDO",
            },
        ],
    )
    def test_map_terms(self, pair):
        """Tests that term mappings from MESH to MONDO are correct."""
        op = Ontology.from_obo(obo=get_ontology_files("mondo"), ontology="mondo")

        mapped = op.map_terms(terms=pair["terms"], _from=pair["from"], _to=pair["to"])

        assert np.all(list(mapped.values()) == pair["correct"])
