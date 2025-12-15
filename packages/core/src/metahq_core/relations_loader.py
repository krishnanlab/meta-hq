"""
Loader for the terms x terms ontology relations DataFrames in the
MetaHQ data package.

Relations DataFrame structure
-----------------------------
The values of these DataFrames answer the following questions with a
1 for 'yes' and 0 for 'no':
    Is a particular row an ancestor of a particular column?
    Is a particular column a descendant of a particular row?

Author: Parker Hicks
Date: 2025-11-17

Last updated: 2025-11-21 by Parker Hicks
"""

from pathlib import Path

import polars as pl

from metahq_core.logger import setup_logger

# these are used a lot so defining them as constants to
# make them easy to change later.
ROW_ID: str = "row_id"
COL_ID: str = "col_id"


class RelationsLoader:
    """Loader for the MetaHQ data package ontology relations DataFrames.

    Performs fast lookup of precomputed ancestor/descendant relationships of nodes
    within the ontology graph.

    """

    def __init__(self, file, logger=None, loglevel=20, logdir=Path(".")):
        self.relations: pl.LazyFrame = self.setup(file)

        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.logger = logger

    def setup(self, file) -> pl.LazyFrame:
        """Load and format the relations dataframe.

        This loads in a base pl.LazyFrame to extract ancestor relationstips,
        descendant relationships, or both within using a single RelationLoader instance.

        Returns
        -------
        pl.DataFrame with an additional temporary 'index' column indicating
        the term ID for each row.

        Raises
        ------
        pl.exceptions.PolarsError if file reading fails.

        """
        lf = pl.scan_parquet(file)

        try:
            return lf.with_columns(pl.Series(ROW_ID, lf.collect_schema().names()))

        except pl.exceptions.PolarsError as e:
            self.logger.error(e)
            raise e

    def get_ancestors(self, subset: list[str] | None = None) -> dict[str, list[str]]:
        """Extract relationships of terms to their ancestors.

        Note that terms queried for their ancestors are included
        in the output mapping.

        Parameters
        ----------
        subset: list[str] | None
            Can be a list of Term IDs in the columns of the relations .parquet file.
            Default is None. Extracts all relationships if None.

        Returns
        -------
        A dictionary of term: [ancestors, ...] relationships.

        """
        lf = self.relations

        if isinstance(subset, list) and (len(subset) > 0):
            lf = lf.select(subset + [ROW_ID])

        relations = self._collect_relations(lf, group_by=COL_ID, agg=ROW_ID)

        return dict(zip(relations[COL_ID], relations[ROW_ID]))

    def get_descendants(self, subset: list[str] | None = None) -> dict[str, list[str]]:
        """Extract relationships of terms to their descendants.

        Note that terms queried for their ancestors are included
        in the output mapping.

        Parameters
        ----------
        subset: list[str] | None
            Can be a list of Term IDs in the columns of the relations .parquet file.
            Default is None. Extracts all relationships if None.

        Returns
        -------
        A dictionary of term: [descendants, ...] relationships.

        """
        lf = self.relations

        if isinstance(subset, list) and (len(subset) > 0):
            lf = lf.filter(pl.col(ROW_ID).is_in(subset))

        relations = self._collect_relations(lf, group_by=ROW_ID, agg=COL_ID)

        return dict(zip(relations[ROW_ID], relations[COL_ID]))

    def _collect_relations(
        self, lf: pl.LazyFrame, group_by: str, agg: str
    ) -> dict[str, list]:
        """Collect the relations LazyFrame.

        Parameters
        ----------
        lf: pl.LazyFrame
            A LazyFrame with term ID columns, an additional column indicating which
            term ID a row represents, and values are 0 or 1.

        group_by: str
            The column to become the keys of the output dictionary.
            Either ROW_ID or COL_ID.

        agg: str
            The column to become the values of the output dictionary.
            Either ROW_ID or COL_ID.

        Returns
        -------
        A dictionary of the following structure:
            {<group_by>: 'Term_x', <agg>: ['Term_a', 'Term_b', 'Term_c']}
        """
        return (
            lf.unpivot(index=ROW_ID, variable_name=COL_ID, value_name="value")
            .filter(pl.col("value") != 0)
            .group_by(group_by)
            .agg(pl.col(agg))
            .collect()
            .to_dict(as_series=False)
        )
