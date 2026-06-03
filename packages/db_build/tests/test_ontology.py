"""Unit tests for the metahq_build.ontology package."""

import gzip

import numpy as np
import polars as pl
import pytest

from metahq_build.ontology._obo_entry import GraphConnections, OboEntry, Synonym, XRef
from metahq_build.ontology.graph import Graph
from metahq_build.ontology.ontology import Ontology, XRefExtractor, XRefMappings
from metahq_build.ontology.relations import RelationsLazyFrame, RelationsMatrix

# ---------------------------------------------------------------------------
# Synthetic OBO fixtures
# ---------------------------------------------------------------------------

# Minimal 3-node chain: MONDO:0000001 -> MONDO:0000002 -> MONDO:0000003
SIMPLE_OBO = """\
[Term]
id: MONDO:0000001
name: root disease
def: "The root of all diseases." [NCIT:ROOT1]
synonym: "origin disease" EXACT []

[Term]
id: MONDO:0000002
name: intermediate disease
xref: DOID:1234 {source="MONDO:equivalentTo"}
xref: MESH:D001234
is_a: MONDO:0000001 ! root disease

[Term]
id: MONDO:0000003
name: leaf disease
xref: DOID:5678 {source="MONDO:relatedTo"}
is_a: MONDO:0000002 ! intermediate disease
"""

# Single entry modeled on real MONDO:0004994 (cardiomyopathy)
CARDIOMYOPATHY_ENTRY = """\
[Term]
id: MONDO:0004994
name: cardiomyopathy
def: "A disease of the heart muscle." [NCIT:C34830]
synonym: "Cardiomyopathies" EXACT [DOID:0050700, MESH:D009202]
xref: DOID:0050700 {source="MONDO:equivalentTo", source="EFO:0000318"}
xref: MESH:D009202 {source="MONDO:equivalentTo"}
is_a: MONDO:0003939 ! muscle tissue disorder
relationship: disease_has_location UBERON:0001133 ! cardiac muscle tissue
"""

# Relations matrix for the 3-node chain (row = ancestor, col = descendant)
# Sorted terms: MONDO:0000001, MONDO:0000002, MONDO:0000003
SIMPLE_TERMS = np.array(["MONDO:0000001", "MONDO:0000002", "MONDO:0000003"])
SIMPLE_MATRIX = np.array(
    [
        [1, 1, 1],  # M:1 is ancestor of M:1, M:2, M:3
        [0, 1, 1],  # M:2 is ancestor of M:2 and M:3
        [0, 0, 1],  # M:3 is ancestor of only itself
    ],
    dtype=np.int8,
)


@pytest.fixture(scope="session")
def simple_obo_file(tmp_path_factory):
    f = tmp_path_factory.mktemp("obo") / "simple.obo"
    f.write_text(SIMPLE_OBO)
    return f


@pytest.fixture(scope="session")
def simple_ontology(simple_obo_file):
    return Ontology.from_obo(simple_obo_file)


@pytest.fixture(scope="session")
def simple_graph(simple_obo_file):
    return Graph.from_obo(simple_obo_file)


@pytest.fixture(scope="session")
def simple_relations_parquet(tmp_path_factory):
    f = tmp_path_factory.mktemp("parquet") / "relations.parquet"
    RelationsMatrix(matrix=SIMPLE_MATRIX, terms=SIMPLE_TERMS).save(f)
    return f


@pytest.fixture(scope="session")
def simple_relations_lf(simple_relations_parquet):
    return RelationsLazyFrame.from_parquet(simple_relations_parquet)


# ---------------------------------------------------------------------------
# OboEntry
# ---------------------------------------------------------------------------


class TestOboEntry:

    def test_basic_fields(self):
        entry = OboEntry.from_text(CARDIOMYOPATHY_ENTRY)
        assert entry.id == "MONDO:0004994"
        assert entry.name == "cardiomyopathy"
        assert entry.definition == "A disease of the heart muscle."
        assert entry.def_sources == ["NCIT:C34830"]

    def test_synonym_parsed(self):
        entry = OboEntry.from_text(CARDIOMYOPATHY_ENTRY)
        assert len(entry.synonyms) == 1
        syn = entry.synonyms[0]
        assert syn.name == "Cardiomyopathies"
        assert syn.scope == "EXACT"
        assert syn.sources == ["DOID:0050700", "MESH:D009202"]

    def test_xrefs_parsed(self):
        entry = OboEntry.from_text(CARDIOMYOPATHY_ENTRY)
        assert len(entry.xrefs) == 2
        doid = next(x for x in entry.xrefs if x.ref_id == "DOID:0050700")
        assert "MONDO:equivalentTo" in doid.sources
        assert "EFO:0000318" in doid.sources
        mesh = next(x for x in entry.xrefs if x.ref_id == "MESH:D009202")
        assert mesh.sources == ["MONDO:equivalentTo"]

    def test_is_a_parsed(self):
        entry = OboEntry.from_text(CARDIOMYOPATHY_ENTRY)
        assert entry.is_a == ["MONDO:0003939"]

    def test_part_of_parsed(self):
        entry = OboEntry.from_text(CARDIOMYOPATHY_ENTRY)
        assert entry.part_of == ["UBERON:0001133"]

    def test_id_prefix(self):
        entry = OboEntry.from_text(CARDIOMYOPATHY_ENTRY)
        assert entry.id_prefix == "MONDO"

    def test_no_optional_fields_defaults(self):
        text = "[Term]\nid: MONDO:0000001\nname: bare term\n"
        entry = OboEntry.from_text(text)
        assert entry.definition is None
        assert entry.synonyms == []
        assert entry.xrefs == []
        assert entry.is_a == []
        assert entry.part_of == []

    def test_missing_id_warns_and_returns_empty_strings(self):
        with pytest.warns(RuntimeWarning):
            entry = OboEntry.from_text("[Term]\nname: orphan\n")
        assert entry.id == ""
        assert entry.name == ""

    def test_xref_without_source_annotation(self):
        text = "[Term]\nid: MONDO:0000002\nname: test\nxref: MESH:D001234\n"
        entry = OboEntry.from_text(text)
        assert len(entry.xrefs) == 1
        assert entry.xrefs[0].ref_id == "MESH:D001234"
        assert entry.xrefs[0].sources == []

    def test_synonym_empty_source_list(self):
        text = '[Term]\nid: MONDO:0000001\nname: root\nsynonym: "origin" EXACT []\n'
        entry = OboEntry.from_text(text)
        assert entry.synonyms[0].sources == []


# ---------------------------------------------------------------------------
# Dataclasses (Synonym, XRef, GraphConnections)
# ---------------------------------------------------------------------------


class TestDataclasses:

    def test_synonym_fields(self):
        syn = Synonym(name="alt name", scope="RELATED", sources=["SRC:1", "SRC:2"])
        assert syn.name == "alt name"
        assert syn.scope == "RELATED"
        assert syn.sources == ["SRC:1", "SRC:2"]

    def test_xref_fields(self):
        xref = XRef(ref_id="DOID:0001", sources=["MONDO:equivalentTo"])
        assert xref.ref_id == "DOID:0001"
        assert xref.sources == ["MONDO:equivalentTo"]

    def test_graph_connections_fields(self):
        gc = GraphConnections(is_a=["MONDO:0000001"], part_of=["UBERON:0001234"])
        assert gc.is_a == ["MONDO:0000001"]
        assert gc.part_of == ["UBERON:0001234"]


# ---------------------------------------------------------------------------
# XRefExtractor
# ---------------------------------------------------------------------------


class TestXRefExtractor:

    @pytest.fixture(scope="class")
    def entries(self, simple_obo_file):
        return Ontology.from_obo(simple_obo_file).entries

    @pytest.fixture(scope="class")
    def extractor(self, entries):
        return XRefExtractor(entries)

    def test_get_no_filter_returns_all_matching_prefix(self, extractor):
        result = extractor.get("DOID")
        assert "DOID:1234" in result.mapping["MONDO:0000002"]
        assert "DOID:5678" in result.mapping["MONDO:0000003"]

    def test_get_no_filter_excludes_non_matching_prefix(self, extractor):
        result = extractor.get("DOID")
        # MESH:D001234 on MONDO:0000002 should not appear
        for values in result.mapping.values():
            assert not any(v.startswith("MESH") for v in values)

    def test_get_with_equivalentTo_filter(self, extractor):
        result = extractor.get("DOID", source_keys=["equivalentTo"])
        assert "MONDO:0000002" in result.mapping
        assert "DOID:1234" in result.mapping["MONDO:0000002"]
        assert "MONDO:0000003" not in result.mapping

    def test_get_with_relatedTo_filter(self, extractor):
        result = extractor.get("DOID", source_keys=["relatedTo"])
        assert "MONDO:0000003" in result.mapping
        assert "DOID:5678" in result.mapping["MONDO:0000003"]
        assert "MONDO:0000002" not in result.mapping

    def test_get_absent_prefix_returns_empty(self, extractor):
        result = extractor.get("HP")
        assert result.mapping == {}

    def test_get_returns_xref_mappings_instance(self, extractor):
        result = extractor.get("DOID")
        assert isinstance(result, XRefMappings)
        assert result.to == "DOID"

    def test_get_empty_source_keys_raises(self, extractor):
        with pytest.raises(ValueError):
            extractor.get("DOID", source_keys=[])


# ---------------------------------------------------------------------------
# XRefMappings
# ---------------------------------------------------------------------------


class TestXRefMappings:

    @pytest.fixture
    def mapping(self):
        return XRefMappings(
            anchor="MONDO",
            to="DOID",
            mapping={
                "MONDO:0000002": ["DOID:1234"],
                "MONDO:0000003": ["DOID:5678", "DOID:9999"],
            },
        )

    def test_attributes(self, mapping):
        assert mapping.anchor == "MONDO"
        assert mapping.to == "DOID"
        assert isinstance(mapping.mapping, dict)

    def test_pl_returns_dataframe(self, mapping):
        df = mapping.pl()
        assert isinstance(df, pl.DataFrame)
        assert set(df.columns) == {"MONDO", "DOID"}
        assert df.shape[0] == 2

    def test_pl_explode_row_count(self, mapping):
        df = mapping.pl(explode=True)
        assert df.shape[0] == 3

    def test_reverse(self, mapping):
        rev = mapping.reverse()
        assert rev["DOID:1234"] == "MONDO:0000002"
        assert rev["DOID:5678"] == "MONDO:0000003"

    def test_add_new_key(self):
        m = XRefMappings(anchor="MONDO", to="DOID", mapping={"MONDO:A": ["DOID:1"]})
        m.add({"MONDO:B": ["DOID:2"]})
        assert m.mapping["MONDO:B"] == ["DOID:2"]

    def test_add_extends_existing_key(self):
        m = XRefMappings(anchor="MONDO", to="DOID", mapping={"MONDO:A": ["DOID:1"]})
        m.add({"MONDO:A": ["DOID:2"]})
        assert "DOID:1" in m.mapping["MONDO:A"]
        assert "DOID:2" in m.mapping["MONDO:A"]

    def test_add_existing_appends_value(self):
        m = XRefMappings(anchor="MONDO", to="DOID", mapping={"MONDO:A": ["DOID:1"]})
        m.add_existing("MONDO:A", ["DOID:99"])
        assert "DOID:99" in m.mapping["MONDO:A"]

    def test_add_existing_deduplicates(self):
        m = XRefMappings(anchor="MONDO", to="DOID", mapping={"MONDO:A": ["DOID:1"]})
        m.add_existing("MONDO:A", ["DOID:1"])
        assert m.mapping["MONDO:A"].count("DOID:1") == 1

    def test_add_existing_missing_key_warns(self):
        m = XRefMappings(anchor="MONDO", to="DOID", mapping={})
        with pytest.warns(UserWarning):
            m.add_existing("MONDO:9999999", ["DOID:0000"])

    def test_repr_contains_class_name(self):
        m = XRefMappings(anchor="MONDO", to="DOID", mapping={})
        assert "XRefMappings" in repr(m)


# ---------------------------------------------------------------------------
# Ontology
# ---------------------------------------------------------------------------


class TestOntology:

    def test_entry_count(self, simple_ontology):
        assert len(simple_ontology.entries) == 3

    def test_entry_ids(self, simple_ontology):
        ids = {e.id for e in simple_ontology.entries}
        assert ids == {"MONDO:0000001", "MONDO:0000002", "MONDO:0000003"}

    def test_class_dict_values(self, simple_ontology):
        assert simple_ontology.class_dict["MONDO:0000001"] == "root disease"
        assert simple_ontology.class_dict["MONDO:0000002"] == "intermediate disease"
        assert simple_ontology.class_dict["MONDO:0000003"] == "leaf disease"

    def test_id_map_polars_columns_and_shape(self, simple_ontology):
        df = simple_ontology.id_map(struct="polars")
        assert isinstance(df, pl.DataFrame)
        assert set(df.columns) == {"id", "name"}
        assert df.shape[0] == 3

    def test_id_map_dict(self, simple_ontology):
        d = simple_ontology.id_map(struct="dict")
        assert isinstance(d, dict)
        assert len(d) == 3

    def test_id_map_invalid_struct_raises(self, simple_ontology):
        with pytest.raises(ValueError):
            simple_ontology.id_map(struct="json")

    def test_xref_no_filter_returns_all_matching_prefix(self, simple_ontology):
        result = simple_ontology.xref("DOID")
        assert "DOID:1234" in result.mapping["MONDO:0000002"]
        assert "DOID:5678" in result.mapping["MONDO:0000003"]

    def test_xref_equivalentTo_excludes_relatedTo(self, simple_ontology):
        result = simple_ontology.xref("DOID", source_keys=["equivalentTo"])
        assert "MONDO:0000002" in result.mapping
        assert "DOID:1234" in result.mapping["MONDO:0000002"]
        # MONDO:0000003's DOID:5678 is relatedTo only — must be absent
        assert "MONDO:0000003" not in result.mapping

    def test_xref_relatedTo_excludes_equivalentTo(self, simple_ontology):
        result = simple_ontology.xref("DOID", source_keys=["relatedTo"])
        assert "MONDO:0000003" in result.mapping
        assert "DOID:5678" in result.mapping["MONDO:0000003"]
        # MONDO:0000002's DOID:1234 is equivalentTo only — must be absent
        assert "MONDO:0000002" not in result.mapping

    def test_xref_absent_prefix_returns_empty_mapping(self, simple_ontology):
        result = simple_ontology.xref("HP")
        assert result.mapping == {}

    def test_entries_setter_rejects_non_list(self):
        onto = Ontology()
        with pytest.raises(TypeError):
            onto.entries = "not a list"

    def test_entries_setter_rejects_non_obo_entry_elements(self):
        onto = Ontology()
        with pytest.raises(TypeError):
            onto.entries = ["not an OboEntry"]

    def test_obsolete_entries_are_excluded(self, tmp_path):
        obo_text = (
            "[Term]\nid: MONDO:0000001\nname: active term\n\n"
            "[Term]\nid: MONDO:0000002\nname: obsolete term\nis_obsolete: true\n"
        )
        f = tmp_path / "test.obo"
        f.write_text(obo_text)
        onto = Ontology.from_obo(f)
        assert len(onto.entries) == 1
        assert onto.entries[0].id == "MONDO:0000001"

    def test_gzipped_obo_is_readable(self, tmp_path):
        f = tmp_path / "test.obo.gz"
        with gzip.open(f, "wt", encoding="utf-8") as fh:
            fh.write(SIMPLE_OBO)
        onto = Ontology.from_obo(f)
        assert len(onto.entries) == 3

    def test_unknown_reader_raises(self, simple_obo_file):
        onto = Ontology()
        with pytest.raises(ValueError, match="Unknown reader"):
            onto.read(simple_obo_file, reader="xml")


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------


class TestGraph:

    def test_nodes_count(self, simple_graph):
        assert len(simple_graph.nodes) == 3

    def test_nodes_are_sorted(self, simple_graph):
        assert simple_graph.nodes == sorted(simple_graph.nodes)

    def test_nodes_content(self, simple_graph):
        assert set(simple_graph.nodes) == {
            "MONDO:0000001",
            "MONDO:0000002",
            "MONDO:0000003",
        }

    def test_leaves(self, simple_graph):
        assert simple_graph.leaves == ["MONDO:0000003"]

    def test_descendants_from_root(self, simple_graph):
        result = simple_graph.descendants_from(["MONDO:0000001"])
        assert set(result["MONDO:0000001"]) == {"MONDO:0000002", "MONDO:0000003"}

    def test_descendants_from_intermediate(self, simple_graph):
        result = simple_graph.descendants_from(["MONDO:0000002"])
        assert set(result["MONDO:0000002"]) == {"MONDO:0000003"}

    def test_descendants_from_leaf_is_empty(self, simple_graph):
        result = simple_graph.descendants_from(["MONDO:0000003"])
        assert result["MONDO:0000003"] == []

    def test_ancestors_from_leaf(self, simple_graph):
        result = simple_graph.ancestors_from(["MONDO:0000003"])
        assert set(result["MONDO:0000003"]) == {"MONDO:0000001", "MONDO:0000002"}

    def test_ancestors_from_root_is_empty(self, simple_graph):
        result = simple_graph.ancestors_from(["MONDO:0000001"])
        assert result["MONDO:0000001"] == []

    def test_descendants_from_absent_node_excluded(self, simple_graph):
        result = simple_graph.descendants_from(["MONDO:9999999"])
        assert result == {}

    def test_ancestors_from_absent_node_excluded(self, simple_graph):
        result = simple_graph.ancestors_from(["MONDO:9999999"])
        assert result == {}

    def test_deepest_node_of_three(self, simple_graph):
        result = simple_graph.deepest_node(
            ["MONDO:0000001", "MONDO:0000002", "MONDO:0000003"]
        )
        assert result == "MONDO:0000003"

    def test_deepest_node_single(self, simple_graph):
        result = simple_graph.deepest_node(["MONDO:0000001"])
        assert result == "MONDO:0000001"

    def test_ancestors_single_term(self, simple_graph):
        result = simple_graph.ancestors("MONDO:0000003")
        assert set(result) == {"MONDO:0000001", "MONDO:0000002"}

    def test_descendants_single_term(self, simple_graph):
        result = simple_graph.descendants("MONDO:0000001")
        assert set(result) == {"MONDO:0000002", "MONDO:0000003"}

    def test_relations_matrix_shape(self, simple_graph):
        rm = simple_graph.relations_matrix()
        assert rm.matrix.shape == (3, 3)
        assert len(rm.terms) == 3

    def test_relations_matrix_dtype(self, simple_graph):
        rm = simple_graph.relations_matrix()
        assert rm.matrix.dtype == np.int8

    def test_relations_matrix_diagonal_is_ones(self, simple_graph):
        rm = simple_graph.relations_matrix()
        np.testing.assert_array_equal(np.diag(rm.matrix), np.ones(3, dtype=np.int8))

    def test_relations_matrix_root_is_ancestor_of_leaf(self, simple_graph):
        rm = simple_graph.relations_matrix()
        terms = list(rm.terms)
        i_root = terms.index("MONDO:0000001")
        i_leaf = terms.index("MONDO:0000003")
        assert rm.matrix[i_root, i_leaf] == 1

    def test_relations_matrix_leaf_not_ancestor_of_root(self, simple_graph):
        rm = simple_graph.relations_matrix()
        terms = list(rm.terms)
        i_leaf = terms.index("MONDO:0000003")
        i_root = terms.index("MONDO:0000001")
        assert rm.matrix[i_leaf, i_root] == 0

    def test_relations_matrix_root_is_ancestor_of_intermediate(self, simple_graph):
        rm = simple_graph.relations_matrix()
        terms = list(rm.terms)
        i_root = terms.index("MONDO:0000001")
        i_mid = terms.index("MONDO:0000002")
        assert rm.matrix[i_root, i_mid] == 1


# ---------------------------------------------------------------------------
# RelationsMatrix
# ---------------------------------------------------------------------------


class TestRelationsMatrix:

    def test_save_creates_file(self, tmp_path):
        rm = RelationsMatrix(matrix=SIMPLE_MATRIX, terms=SIMPLE_TERMS)
        out = tmp_path / "relations.parquet"
        rm.save(out)
        assert out.exists()

    def test_save_creates_nested_parent_dirs(self, tmp_path):
        rm = RelationsMatrix(matrix=SIMPLE_MATRIX, terms=SIMPLE_TERMS)
        out = tmp_path / "nested" / "dir" / "relations.parquet"
        rm.save(out)
        assert out.exists()

    def test_saved_parquet_columns_are_terms(self, tmp_path):
        rm = RelationsMatrix(matrix=SIMPLE_MATRIX, terms=SIMPLE_TERMS)
        out = tmp_path / "relations.parquet"
        rm.save(out)
        df = pl.read_parquet(out)
        assert set(df.columns) == set(SIMPLE_TERMS)

    def test_saved_parquet_values_match_matrix(self, tmp_path):
        rm = RelationsMatrix(matrix=SIMPLE_MATRIX, terms=SIMPLE_TERMS)
        out = tmp_path / "relations.parquet"
        rm.save(out)
        df = pl.read_parquet(out)
        expected = pl.DataFrame(SIMPLE_MATRIX, schema=list(SIMPLE_TERMS), orient="row")
        assert df.equals(expected)


# ---------------------------------------------------------------------------
# RelationsLazyFrame
# ---------------------------------------------------------------------------


class TestRelationsLazyFrame:

    def test_from_parquet_returns_instance(self, simple_relations_parquet):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        assert isinstance(lf, RelationsLazyFrame)
        assert isinstance(lf.relations, pl.LazyFrame)

    def test_from_parquet_has_row_id_column(self, simple_relations_parquet):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        assert "row_id" in lf.relations.collect_schema().names()

    def test_get_descendants_root_has_two_descendants(self, simple_relations_lf):
        result = simple_relations_lf.get_descendants()
        assert set(result["MONDO:0000001"]) == {"MONDO:0000002", "MONDO:0000003"}

    def test_get_descendants_intermediate_has_one_descendant(self, simple_relations_lf):
        result = simple_relations_lf.get_descendants()
        assert set(result["MONDO:0000002"]) == {"MONDO:0000003"}

    def test_get_descendants_leaf_has_no_descendants(self, simple_relations_lf):
        result = simple_relations_lf.get_descendants()
        assert result["MONDO:0000003"] == list()

    def test_get_descendants_subset_limits_output_keys(self, simple_relations_parquet):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        result = lf.get_descendants(subset=["MONDO:0000001"])
        assert set(result.keys()) == {"MONDO:0000001"}
        assert set(result["MONDO:0000001"]) == {"MONDO:0000002", "MONDO:0000003"}

    def test_get_descendants_rm_self_false_includes_self(
        self, simple_relations_parquet
    ):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        result = lf.get_descendants(subset=["MONDO:0000001"], rm_self=False)
        assert "MONDO:0000001" in result["MONDO:0000001"]

    def test_get_ancestors_leaf_has_two_ancestors(self, simple_relations_parquet):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        result = lf.get_ancestors(rm_self=True)
        assert set(result["MONDO:0000003"]) == {"MONDO:0000001", "MONDO:0000002"}

    def test_get_ancestors_intermediate_has_one_ancestor(
        self, simple_relations_parquet
    ):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        result = lf.get_ancestors(rm_self=True)
        assert set(result["MONDO:0000002"]) == {"MONDO:0000001"}

    def test_get_ancestors_root_has_no_ancestors(self, simple_relations_parquet):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        result = lf.get_ancestors(rm_self=True)
        assert result["MONDO:0000001"] == list()

    def test_get_ancestors_subset_limits_output_keys(self, simple_relations_parquet):
        lf = RelationsLazyFrame.from_parquet(simple_relations_parquet)
        result = lf.get_ancestors(subset=["MONDO:0000003"], rm_self=True)
        assert "MONDO:0000003" in result
        assert set(result["MONDO:0000003"]) == {"MONDO:0000001", "MONDO:0000002"}
