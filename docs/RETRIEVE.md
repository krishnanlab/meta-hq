# metahq retrieve

With MetaHQ retrieval functions, users can query and manipulate tissue, disease, sex, and age annotations for >250k publicly available microarray and RNA-Seq samples and their corresponding series.

Most retrieve commands share the same set of arguments: `terms`, `level`, `filters`, `metadata`, `fmt`, and `output`.

### Terms

The `terms` argument specifies the entities to retrieve annotations for. Queries can be passed as a comma-delimited string of term IDs (e.g., `"UBERON:0000948,UBERON:0000955"`, `"M,F"`)
or a text file where each line contains a single term ID. Users may also pass `--terms "all"` to retrieve annotations for all tissues.

### Level

MetaHQ allows for sample- and series-level annotations specified by passing `"sample"` or `"series"` to the `--level` argument. Default is `sample`.

### Mode

Tissue and disease retrieval include an additional `mode` argument allowing users to retrieve direct annotations to the queried terms, propagated annotations, or labels.

- **direct**: The direct annotations submitted by annotation set curators.
- **propagate**: For a partiular query, any samples and series directly annotated to that term will be returned **as well** as any samples/series annotated to any descendants of the query term.
  For example, if the query is `heart` (UBERON:0000948), then any samples or series annotated to `myocardium` (UBERON:0002349) and any other descendants of heart will also be included.
- **label**: For a given query, any entries directly annotated to the term or one of it's descendants are assigned a +1, any entries directly annotated to an ancestor of that term are assigned
  a 0 label (or unknown), and all other entries are assigned a -1 label. Additionally, disease labels include samples annotated as healthy controls. These samples are assigned a label of 2 to diseases
  that any of the other samples from the same series as the control sample are annotated to.

Default is `direct`.

### Filters

Users can filter for various attributes of the annotations. Filters should be passed as a single string with entries separated by commas (e.g. `"species=human,ecode=expert,technology=rnaseq"`).

#### species

A vast majority of annotations are derived from human samples and series, but all of the following species are queriable:

- _human_ (homo sapiens)
- _mouse_ (mus musculus)
- _rat_ (rattus norvegicus)
- _fish_ (danio rerio)
- _fly_ (drosolphila melanogaster)
- _worm_ (caenorhabditis elegans)

#### ecode

- _expert_: Annotations derived from expert curators.
- _semi_: Annotations that were predicted using some automated system where a subset of those predictions were checked by an expert.
- _crowd_: Annotations derived from non-experts and crowd-sourced projects.

#### technology

Currently MetaHQ only contains annotations for _microarray_ and _rnaseq_

- _microarray_: Annotations derived from expert curators.
- _rnaseq_: Annotations that were predicted using some automated system where a subset of those predictions were checked by an expert.

### Metadata

Users can include verious metadata associated with returned samples and series.

At the **sample level**, users can include the following:

- _sample_
- _series_
- _platform_
- _description_
- _srx_
- _srs_
- _srp_

At the **series level**, users can include the following:

- _series_
- _platform_
- _description_
- _srp_

The defualt argument is the annotation level.

### Formats

The following file formats are supported:

- _csv_
- _tsv_
- _parquet_
- _json_

For large queries (e.g., `--terms "all"`), we recommend using the `parquet` format. Default is `parquet`.

## tissues

Tissue queries require ontology term ID inputs. To find the appropriate term ID for your context of interest, see [metahq search](SEARCH.md)
or the EMBL-EBI ontology lookup service [here](https://www.ebi.ac.uk/ols4). Only ontology term IDs can be input to the `term` argument.

#### Example:

`metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" --level sample --mode label --filters "species=human,ecode=expert,technology=rnaseq" --metadata "sample,series,platform" --fmt tsv --output annotations.tsv`

## diseases

Diseases are queriable in a similar manner as tissues. Only ontology term IDs can be input to the `term` argument.

#### Example:

`metahq retrieve diseases --terms "MONDO:0004994,MONDO:0018177" --level sample --mode label --filters "species=human,ecode=expert,technology=rnaseq" --metadata "sample,series,platform" --fmt tsv --output annotations.tsv`

## sex

Direct annotations are queriable for biological sex. There is no `mode` argument since only direct annotations can be returned. All other arguments are the same.

#### Example:

`metahq retrieve sex --terms "M,F" --level sample --filters "species=human,ecode=expert,technology=rnaseq" --metadata "sample,series,platform" --fmt tsv --output annotations.tsv`

## age

Users can query age group labels. Age groups are defined by hormone levels throughout the human life cycle described by Ober, Liosel, and Gilad (2008).
Supported age groups are `fetus`, `infant`, `child`, `adolescent`, `adult`, `older_adult`, and `elderly_adult`.

Simliar to sex retrieval, there is no `mode` argument since only direct annotations can be returned.

#### Example:

`metahq retrieve age --terms "infant,older_adult" --level sample --filters "species=human,ecode=expert,technology=rnaseq" --metadata "sample,series,platform" --fmt tsv --output annotations.tsv`

## References

Ober, C., Loisel, D. A., & Gilad, Y. (2008). Sex-specific genetic architecture of human disease. Nature Reviews Genetics, 9(12), 911-922.
