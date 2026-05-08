import sys
from pathlib import Path
from typing import Literal

import polars as pl
from numpy.typing import NDArray

from metahq_build.util.logging import setup_logger

# temporary names used for polars group-by operations
ROW_ID: str = "row_id"
COL_ID: str = "col_id"


class RelationsMatrix:
    """Class to store and save ontology relations matrices.

    Attributes:
        matrix (NDArray):
            A terms x terms matrix storing binary relationships between term pairs.
                You may interpret it as the following: For any row, column pair, if the
                value is 1, then the term representing that particular row is an ancestor
                of the term representing that particular column. If the value is 0, then
                there is no relationship between the terms.

        terms (NDArray):
            An array representing the columns and rows of the matrix.
    """

    def __init__(self, matrix: NDArray, terms: NDArray):
        self.matrix = matrix
        self.terms = terms

    def save(self, outfile: Path):
        outdir = outfile.resolve().parents[0]
        if not outdir.exists():
            outdir.mkdir(exist_ok=True, parents=True)

        pl.LazyFrame(self.matrix, schema=list(self.terms), orient="row").sink_parquet(
            outfile, engine="streaming"
        )


class RelationsLazyFrame:
    """Loader for the MetaHQ setup package ontology relations DataFrames.

    Performs fast lookup of precomputed ancestor/descendant relationships of nodes
    within the ontology graph.

    Relations DataFrame structure
    -----------------------------
    The values of a collected frame answers the following questions with a
    1 for 'yes' and 0 for 'no':
        Is a particular row an ancestor of a particular column?
        Is a particular column a descendant of a particular row?
    """

    def __init__(self, lf):
        self.relations: pl.LazyFrame = lf
        self.logger = setup_logger("metahq_build.ontology.relations.RelationsLazyFrame")

    def get_ancestors(
        self, subset: list[str] | None = None, rm_self: bool = False
    ) -> dict[str, set[str]]:
        """Extract relationships of terms to their ancestors.

        Note that terms queried for their ancestors are included
        in the output mapping.

        Arguments:
            subset (list[str] | None):
                Can be a list of Term IDs in the columns of the relations .parquet file.
                    Default is None. Extracts all relationships if None.
            rm_self (bool):
                If True, will remove the term ID representing a particular key from the
                    values of that same key.

        Returns:
            (dict): A dictionary of term: [ancestors, ...] relationships.

        """
        lf = self.relations

        if isinstance(subset, list) and (len(subset) > 0):
            lf = lf.select(subset + [ROW_ID])

        relations = self._collect_relations(lf, group_by=COL_ID, agg=ROW_ID)
        relations = dict(zip(relations[COL_ID], set(relations[ROW_ID])))

        if rm_self:
            self.logger.info("Removing self terms from ancestors query...")
            relations = self._rm_self_relations(relations)

        return relations

    def get_descendants(
        self, subset: list[str] | None = None, rm_self: bool = True
    ) -> dict[str, set[str]]:
        """Extract relationships of terms to their descendants.

        Note that terms queried for their ancestors are included
        in the output mapping.

        Arguments:
            subset (list[str] | None):
                Can be a list of Term IDs in the columns of the relations .parquet file.
                    Default is None. Extracts all relationships if None.
            rm_self (bool):
                If True, will remove the term ID representing a particular key from the
                    values of that same key.

        Returns:
            (dict): A dictionary of term: [descendants, ...] relationships.
        """
        lf = self.relations

        if isinstance(subset, list) and (len(subset) > 0):
            lf = lf.filter(pl.col(ROW_ID).is_in(subset))

        relations = self._collect_relations(lf, group_by=ROW_ID, agg=COL_ID)
        relations = dict(zip(relations[ROW_ID], relations[COL_ID]))

        if rm_self:
            relations = self._rm_self_relations(relations)

        return relations

    @classmethod
    def from_parquet(cls, file: Path):
        """Load and format the relations dataframe.

        This loads in a base pl.LazyFrame to extract ancestor relationstips,
        descendant relationships, or both within using a single RelationLoader instance.

        Returns:
            (pl.DataFrame): pl.DataFrame with an additional temporary 'index' column indicating
                the term ID for each row.

        Raises: pl.exceptions.PolarsError if file reading fails.

        """
        try:
            lf = pl.scan_parquet(file)
            instance = cls(
                lf=lf.with_columns(pl.Series(ROW_ID, lf.collect_schema().names()))
            )
            return instance

        except pl.exceptions.PolarsError as e:
            print(e)
            sys.exit(1)

    def _collect_relations(
        self,
        lf: pl.LazyFrame,
        group_by: str,
        agg: str,
        engine: Literal["auto", "streaming", "gpu"] = "streaming",
    ) -> dict[str, list]:
        """Collect the relations LazyFrame.

        Arguments:
            lf (pl.LazyFrame):
                A LazyFrame with term ID columns, an additional column indicating which
                    term ID a row represents, and values are 0 or 1.

            group_by (str):
                The column to become the keys of the output dictionary.
                    Either ROW_ID or COL_ID.

            agg (str):
                The column to become the values of the output dictionary.
                    Either ROW_ID or COL_ID.

        Returns:
            (dict): A dictionary of the following structure:
                {<group_by>: 'Term_x', <agg>: ['Term_a', 'Term_b', 'Term_c']}
        """
        return (
            lf.unpivot(index=ROW_ID, variable_name=COL_ID, value_name="value")
            .filter(pl.col("value") != 0)
            .group_by(group_by)
            .agg(pl.col(agg))
            .collect(engine=engine)
            .to_dict(as_series=False)
        )

    @staticmethod
    def _rm_self_relations(relations: dict[str, list[str]]):
        """Remove the term ID representing a particular key from the
        values of that same key.
        """
        relations_self_rm = {}
        for term, ds in relations.items():
            ds = set(ds)
            ds.discard(term)
            relations_self_rm[term] = ds

        return relations_self_rm
