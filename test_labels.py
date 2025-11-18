import sys
from pathlib import Path

import numpy as np
import polars as pl
from metahq_core.util.io import load_bson
from metahq_core.util.supported import get_annotations

ROOT = Path(__file__).resolve().parents[0]

true = pl.scan_csv(ROOT / "cpc_tissue_sample_label.csv.gz").rename({"": "sample"})

metahq = pl.scan_parquet(sys.argv[1])

terms_x = true.collect_schema().names()
terms_y = metahq.collect_schema().names()

common_terms = list(set(terms_x) & set(terms_y))


true = true.select(common_terms).collect()
metahq = metahq.select(common_terms).collect()


true = true.filter(pl.col("sample").is_in(metahq["sample"].to_list())).sort(by="sample")
metahq = metahq.filter(pl.col("sample").is_in(true["sample"].to_list())).sort(
    by="sample"
)


bad = {}
index = true["sample"].to_numpy()
for col in true.columns:
    mask = true[col].to_numpy() == metahq[col].to_numpy()

    if not np.all(mask):
        bad[col] = list(index[~mask])

print("Number bad columns: ", len(bad))


example = list(bad.keys())[0]

example_true = (
    true.select([example, "sample"]).filter(pl.col(example) == 1)[example].to_numpy()
)

example_metahq = (
    metahq.select([example, "sample"]).filter(pl.col(example) == 1)[example].to_numpy()
)

print(example)
if example_true.shape != example_metahq.shape:
    print(true.select([example, "sample"]).filter(pl.col("sample").is_in(bad[example])))
    print(
        metahq.select([example, "sample"]).filter(pl.col("sample").is_in(bad[example]))
    )

    anno = load_bson(get_annotations("sample"))
    if len(bad[example]) < 10:
        for entry in bad[example]:
            print(anno[entry])

    else:
        for entry in bad[example][0:5]:
            print(anno[entry])
        for entry in bad[example][-5:-1]:
            print(anno[entry])

else:
    print(true.select([example, "sample"]).filter(pl.col("sample").is_in(bad[example])))
    print(
        metahq.select([example, "sample"]).filter(pl.col("sample").is_in(bad[example]))
    )
    print(true.select([example, "sample"]).equals(metahq.select([example, "sample"])))
