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

from metahq_build.combiners.geo import GEO_SOURCES, GeoCombiner
from metahq_build.combiners.sample import SampleCombiner
from metahq_build.combiners.sra import SRA_SOURCES, SraCombiner

__all__ = [
    "GeoCombiner",
    "GEO_SOURCES",
    "SraCombiner",
    "SRA_SOURCES",
    "SampleCombiner",
]
