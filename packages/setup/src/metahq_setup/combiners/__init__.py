"""
Annotation combination utilities.

Combines processed annotations from multiple sources into a single BSON
file for use with the MetaHQ database.

Two combiners are provided:

- ``GeoCombiner`` — GEO-based sources (GSM/GSE IDs, no ID mapping needed).
- ``SraCombiner`` — SRA-based sources (SRR/SRX IDs mapped to GSM via OmicIDX).
"""

from metahq_setup.combiners.geo import GeoCombiner, GEO_COMBINED_BSON, GEO_SOURCES
from metahq_setup.combiners.sra import SraCombiner, SRA_COMBINED_BSON, SRA_SOURCES

__all__ = [
    "GeoCombiner",
    "GEO_COMBINED_BSON",
    "GEO_SOURCES",
    "SraCombiner",
    "SRA_COMBINED_BSON",
    "SRA_SOURCES",
]
