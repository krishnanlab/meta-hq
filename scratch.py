import polars as pl
from metahq_core.util.supported import get_onto_families

file = get_onto_families("mondo")["relations"]

df = pl.read_parquet(file)

# Add row index as identifier
ancestor_matrix = df.with_columns(pl.Series("row_id", df.columns))

node_descendants = (
    ancestor_matrix.unpivot(
        index="row_id", variable_name="ancestor", value_name="value"
    )
    .filter(pl.col("value") != 0)
    .group_by("ancestor")
    .agg(pl.col("row_id"))
    .to_dict(as_series=False)
)

node_descendants = dict(zip(node_descendants["ancestor"], node_descendants["row_id"]))

print(list(node_descendants.keys())[0])
print(node_descendants[list(node_descendants.keys())[0]])
