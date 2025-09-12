import polars as pl
from metahq_core.curations.annotation_converter import AnnotationsConverter
from metahq_core.curations.annotations import Annotations


def main():

    test_df = pl.DataFrame(
        {
            "index": ["s1", "s2", "s3", "s4", "s5"],
            "group": ["a", "a", "a", "b", "b"],
            "MONDO:0004994": [1, 0, 0, 0, 0],
            "MONDO:0005267": [0, 1, 0, 0, 0],
            "MONDO:0005217": [1, 0, 1, 0, 0],
            "MONDO:0005575": [0, 0, 0, 1, 0],
        }
    )
    anno = Annotations.from_df(test_df, index_col="index", group_cols=["group"])

    cardiovascular_disease = "MONDO:0004995"
    cancer = "MONDO:0004992"
    familial_cardiomyopathy = "MONDO:0005217"
    gsd = "MONDO:0009290"
    to_terms = [cardiovascular_disease, cancer, familial_cardiomyopathy, gsd]

    converter = AnnotationsConverter(anno, mode="annotations", ontology="mondo")
    converter.propagate_up(to_terms)


if __name__ == "__main__":
    main()
