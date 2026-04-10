"""
Annotation combination utilities.

Combines processed annotations from multiple sources into a single BSON
file for use with the MetaHQ database.

Three combiners are provided:

- ``GeoCombiner`` — GEO-based sources (GSM/GSE IDs, no ID mapping needed).
- ``SraCombiner`` — SRA-based sources (SRR/SRX IDs mapped to GSM via OmicIDX).
- ``SampleCombiner`` — merges the GEO and SRA combined BSONs into a single
  sample-level database keyed by GSM.
"""

from metahq_setup.combiners.geo import GeoCombiner, GEO_COMBINED_BSON, GEO_SOURCES
from metahq_setup.combiners.sra import SraCombiner, SRA_COMBINED_BSON, SRA_SOURCES
from metahq_setup.combiners.sample import SampleCombiner, SAMPLE_COMBINED_BSON

__all__ = [
    "GeoCombiner",
    "GEO_COMBINED_BSON",
    "GEO_SOURCES",
    "SraCombiner",
    "SRA_COMBINED_BSON",
    "SRA_SOURCES",
    "SampleCombiner",
    "SAMPLE_COMBINED_BSON",
]
