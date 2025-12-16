"""
Test the quality of MetaHQ annotations.

Author: Parker Hicks
Date: 2025-09-23

Last updated: 2025-09-24 by Parker Hicks
"""

import pytest

from metahq_core.util.io import load_bson
from metahq_core.util.supported import get_annotations

# TODO: Make sure each sample annotation has a sample, series, and platform accession ID

SAMPLE_IDS = ["sample", "series", "platform"]
SERIES_IDS = ["samples", "series", "platforms"]


@pytest.fixture
def sample_annotations():
    """Load sample annotations."""
    return load_bson(get_annotations("sample"))


@pytest.fixture
def series_annotations():
    """Load sample annotations."""
    return load_bson(get_annotations("series"))


class TestSampleAnnotations:
    def test_sample_accessions(self, sample_annotations):
        missing_ids = set()
        for entry, annos in sample_annotations.items():
            if entry.startswith("GSE"):
                continue
            for field in SAMPLE_IDS:
                if field in annos["accession_ids"]:
                    continue
                missing_ids.add(entry)

        assert len(missing_ids) == 0, f"{sorted(missing_ids)} are missing ids."


# class TestSeriesAnnotations:
#    def test_series_accessions(self, series_annotations):
#        missing_ids = []
#        for entry, annos in series_annotations.items():
#            for field in SERIES_IDS:
#                if field in annos["accession_ids"]:
#                    continue
#                missing_ids.append(entry)
#
#        assert len(missing_ids) == 0, f"{missing_ids} are missing ids."
