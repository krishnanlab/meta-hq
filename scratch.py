import polars as pl
from metahq_core.curations.annotations import Annotations

df = pl.DataFrame(
    {
        "index": ["i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8"],
        "group": ["g1", "g1", "g2", "g2", "g3", "g3", "g4", "g4"],
        "MONDO:0005217": [1, 1, 1, 0, 0, 0, 0, 0],
        "MONDO:0004995": [0, 0, 0, 0, 0, 0, 1, 1],
        "MONDO:0008903": [0, 0, 0, 0, 1, 1, 0, 0],
        "MONDO:0005211": [0, 0, 0, 0, 0, 0, 0, 0],
        #        "MONDO:0000001": [0, 0, 0, 1, 1, 1, 1, 0],
    }
)
anno = Annotations.from_df(
    df, index_col="index", group_cols=["group"], verbose=True, loglevel=10
)

to = ["MONDO:0004994", "MONDO:0004992", "MONDO:0005211"]

# print(anno)
print(anno.propagate(to_terms=to, ontology="mondo", mode=1))
