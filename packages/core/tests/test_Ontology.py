"""
This script tests functionalities for the Ontology class.

Author: Parker Hicks
Date: 2025-02-11

Last updated: 2025-09-02
"""

from pathlib import Path

import numpy as np
import pytest

from ontology.base import Ontology

test_entries = {
    2: """[Term]\nid: MONDO:0000005\nname: alopecia, isolated\nxref: OMIMPS:203655 {source="MONDO:equivalentTo"}\nis_a: MONDO:0021034 ! hereditary alopecia\nproperty_value: exactMatch https://omim.org/phenotypicSeries/PS203655""",
    4: "[Term]\nid: MONDO:0000014\nname: colorblindness, partial\nis_a: MONDO:0001703 ! color vision disorder",
}

ROOT_DIR = Path(__file__).resolve().parents[3]
MONDO_OBO = ROOT_DIR / "data/ontology/mondo/mondo.obo"
UBERON_OBO = ROOT_DIR / "data/ontology/uberon_ext/uberon_ext.obo"

OBO_FILES: list[tuple[str, Path]] = [
    ("mondo", MONDO_OBO),
    ("uberon", UBERON_OBO),
]


class TestOntology:
    """
    Class to test functionalities from the Ontology class.
    """

    @pytest.mark.parametrize("obo_file", OBO_FILES)
    def test_from_obo(self, obo_file):
        """Tests that no errors arise when initializing the Graph."""
        _ = Ontology.from_obo(obo=obo_file[1], ontology=obo_file[0])
        # No error should be raised

    def test_get_entries(self):
        """
        Check expected entries.

        Output should be a list of term entries where each key, value of
        the term entry is separated by the \n character and new term entries
        are separated by the \n\n characters.
        """
        mondo_obo = OBO_FILES[0]
        op = Ontology.from_obo(obo=mondo_obo[1], ontology=mondo_obo[0])

        for idx, entry in test_entries.items():
            assert op.entries[idx] == entry

    @pytest.mark.parametrize("obo_file", OBO_FILES)
    @pytest.mark.parametrize("val", ["1", "a", 1, {}])
    def test_entries_setter(self, obo_file, val):
        """
        Checks type checking of entries setter.

        It's expected that an error will be raised if self.entries is not
        type: list.
        """
        op = Ontology.from_obo(obo=obo_file[1], ontology=obo_file[0])
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
        mondo_obo = OBO_FILES[0]
        op = Ontology.from_obo(obo=mondo_obo[1], ontology=mondo_obo[0])

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
        mondo_obo = OBO_FILES[0]
        op = Ontology.from_obo(obo=mondo_obo[1], ontology=mondo_obo[0])

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
        mondo_obo = OBO_FILES[0]
        op = Ontology.from_obo(obo=mondo_obo[1], ontology=mondo_obo[0])

        mapped = op.map_terms(terms=pair["terms"], _from=pair["from"], _to=pair["to"])

        assert np.all(list(mapped.values()) == pair["correct"])
