"""
This script is used to create semi-processed series-level annotations from the
outputs of the SraCombiner and the GeoCombiner modules. This is only used for
analysis comparing the attribute annotation coverage of pre- and post-harmonization.

Author: Parker Hicks
Date: 2026-05-27
"""

from pathlib import Path

from metahq_build.combiners.study import StudyCombiner
from metahq_build.config import GEO_COMBINED_BSON, SRA_COMBINED_BSON


def main():
    """Main entry point."""
    combiner = StudyCombiner()
    combiner.combine_from_unprocessed(
        unprocessed_combined_geo_bson=GEO_COMBINED_BSON,
        unprocessed_combined_sra_bson=SRA_COMBINED_BSON,
    )
    combiner.save(Path("data/analysis/semi_processed__combined__level-series.bson"))


if __name__ == "__main__":
    main()
