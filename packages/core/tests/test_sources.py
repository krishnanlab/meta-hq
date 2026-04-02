"""
Unit tests for license-related functionality in sources.py.

Author: Parker Hicks
Date: 2026-04-02

Last updated: 2026-04-02 by Parker Hicks
"""

import pytest

from metahq_core.sources import (
    SOURCE_LICENSE_CATEGORY,
    Ale,
    Bgee,
    CellO,
    Creeds,
    DiSignAtlas,
    Gemma,
    Golightly,
    Gu,
    Johnson,
    KrishnanLab,
    REFERENCE_MAP,
    Sirota,
    Ursa,
    UrsaHD,
    _license_categories,
    get_allowed_sources,
)


# =======================================================
# ==== license_category attribute tests
# =======================================================


class TestLicenseCategories:
    """Test license_category class attributes on each Reference subclass."""

    @pytest.mark.parametrize(
        "cls",
        [Ale, Bgee, CellO, Creeds, Golightly, Gu, Johnson, KrishnanLab],
    )
    def test_permissive_sources(self, cls):
        """CC0 and CC BY sources should be categorized as permissive."""
        assert cls.license_category == "permissive"

    @pytest.mark.parametrize("cls", [Gemma, Sirota, Ursa, UrsaHD, DiSignAtlas])
    def test_nc_sources(self, cls):
        """Non-commercial sources should be categorized as nc."""
        assert cls.license_category == "nc"

    def test_all_reference_map_entries_have_license_category(self):
        """Every class in REFERENCE_MAP must have a license_category attribute."""
        for name, cls in REFERENCE_MAP.items():
            assert hasattr(cls, "license_category"), (
                f"{name} is missing license_category"
            )

    def test_all_license_categories_are_valid(self):
        """Every license_category value must be a recognized category."""
        valid = set(_license_categories()) - {"any"}
        for name, cls in REFERENCE_MAP.items():
            assert cls.license_category in valid, (
                f"{name}.license_category={cls.license_category!r} is not valid"
            )


# =======================================================
# ==== SOURCE_LICENSE_CATEGORY mapping tests
# =======================================================


class TestSourceLicenseCategoryMapping:
    """Test SOURCE_LICENSE_CATEGORY dict."""

    def test_all_reference_map_keys_present(self):
        """Every key in REFERENCE_MAP must appear in SOURCE_LICENSE_CATEGORY."""
        for name in REFERENCE_MAP:
            assert name in SOURCE_LICENSE_CATEGORY, (
                f"{name} missing from SOURCE_LICENSE_CATEGORY"
            )

    def test_no_extra_keys(self):
        """SOURCE_LICENSE_CATEGORY must not have keys absent from REFERENCE_MAP."""
        for name in SOURCE_LICENSE_CATEGORY:
            assert name in REFERENCE_MAP, (
                f"{name} in SOURCE_LICENSE_CATEGORY but not in REFERENCE_MAP"
            )

    def test_category_matches_class_attribute(self):
        """SOURCE_LICENSE_CATEGORY values must match each class's license_category."""
        for name, cls in REFERENCE_MAP.items():
            assert SOURCE_LICENSE_CATEGORY[name] == cls.license_category, (
                f"Mismatch for {name}: map={SOURCE_LICENSE_CATEGORY[name]!r}, "
                f"attr={cls.license_category!r}"
            )

    def test_disignatlas_is_nc(self):
        assert SOURCE_LICENSE_CATEGORY["DiSignAtlas"] == "nc"

    def test_gemma_is_nc(self):
        assert SOURCE_LICENSE_CATEGORY["Gemma"] == "nc"

    def test_ursa_is_nc(self):
        assert SOURCE_LICENSE_CATEGORY["URSA"] == "nc"

    def test_ursa_hd_is_nc(self):
        assert SOURCE_LICENSE_CATEGORY["URSA_HD"] == "nc"

    def test_krishnanlab_is_permissive(self):
        assert SOURCE_LICENSE_CATEGORY["KrishnanLab"] == "permissive"

    def test_ale_is_permissive(self):
        assert SOURCE_LICENSE_CATEGORY["ALE"] == "permissive"


# =======================================================
# ==== _license_categories tests
# =======================================================


class TestLicenseCategoriesFunction:
    """Test _license_categories()."""

    def test_returns_list(self):
        assert isinstance(_license_categories(), list)

    def test_contains_required_categories(self):
        cats = _license_categories()
        assert "permissive" in cats
        assert "nc" in cats
        assert "any" in cats

    def test_no_extra_categories(self):
        """Ensure only the documented categories are present."""
        assert set(_license_categories()) == {"permissive", "nc", "any"}


# =======================================================
# ==== get_allowed_sources tests
# =======================================================


class TestGetAllowedSources:
    """Test get_allowed_sources()."""

    def test_any_returns_none(self):
        """'any' should return None, meaning no filtering."""
        assert get_allowed_sources("any") is None

    def test_permissive_returns_set(self):
        result = get_allowed_sources("permissive")
        assert isinstance(result, set)

    def test_nc_returns_set(self):
        result = get_allowed_sources("nc")
        assert isinstance(result, set)

    def test_permissive_excludes_nc_sources(self):
        result = get_allowed_sources("permissive")
        nc_sources = {name for name, cat in SOURCE_LICENSE_CATEGORY.items() if cat == "nc"}
        for source in nc_sources:
            assert source not in result, f"NC source {source} found in permissive result"

    def test_permissive_includes_all_permissive_sources(self):
        result = get_allowed_sources("permissive")
        permissive_sources = {
            name for name, cat in SOURCE_LICENSE_CATEGORY.items() if cat == "permissive"
        }
        assert permissive_sources == result

    def test_nc_includes_permissive_sources(self):
        result = get_allowed_sources("nc")
        permissive_sources = {
            name for name, cat in SOURCE_LICENSE_CATEGORY.items() if cat == "permissive"
        }
        for source in permissive_sources:
            assert source in result

    def test_nc_includes_nc_sources(self):
        result = get_allowed_sources("nc")
        nc_sources = {name for name, cat in SOURCE_LICENSE_CATEGORY.items() if cat == "nc"}
        for source in nc_sources:
            assert source in result

    def test_nc_is_superset_of_permissive(self):
        permissive = get_allowed_sources("permissive")
        nc = get_allowed_sources("nc")
        assert permissive.issubset(nc)

    def test_nc_excludes_nothing_in_current_schema(self):
        """nc should cover all current sources since there are no 'restricted' sources."""
        nc = get_allowed_sources("nc")
        all_sources = set(SOURCE_LICENSE_CATEGORY.keys())
        assert nc == all_sources

    def test_invalid_license_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid license query"):
            get_allowed_sources("commercial")

    def test_invalid_license_message_contains_valid_options(self):
        with pytest.raises(ValueError) as exc_info:
            get_allowed_sources("open")
        assert "permissive" in str(exc_info.value)
        assert "nc" in str(exc_info.value)
        assert "any" in str(exc_info.value)

    def test_disignatlas_excluded_from_permissive(self):
        result = get_allowed_sources("permissive")
        assert "DiSignAtlas" not in result

    def test_disignatlas_included_in_nc(self):
        result = get_allowed_sources("nc")
        assert "DiSignAtlas" in result
