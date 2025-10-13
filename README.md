# meta-hq

A platform for harmonizing and distributing community-curated high-quality metadata of public omics samples and datasets.

## Setup

### 1. Install metahq

For dev install, see [dev install instructions](docs/INSTALL.md). Otherwise run the following:

```bash
pip install metahq
```

### 2. Setup metahq dir in home directory (dev only)

MetaHQ will require users to download a data instance from Zenodo. The data used here are not yet uploaded to Zenodo
and are instead included in `metahq.tar.gz`. Unzip this file and place the folder into your home directory.

From `/path/to/metahq` run:

```bash
tar -xvf metahq.tar.gz -C ~/metahq
```

### 3. Setup the metahq config

This creates a config file in `~/metahq` that outlines where to pull data from for MetaHQ functionalities.
**NOTE**: there is no Zenodo DOI yet. Just use something random.

```bash
metahq setup --zenodo_doi xxx --data_dir ~/metahq/data
```

## Functionalities

### Term search

Input a free text query (e.g. "heart attack") and return the most similar ontology term IDs (e.g., MONDO:0005068)

```bash
metahq search [--query "heart attack"] [--type celltype,tissue,disease] \
    [--ontology CL,UBERON,MONDO] [--max-results 20]
```

#### Examples

1. Heart attack

```bash
metahq search --query "heart attack" --type disease --ontology MONDO --max-results 3
```

Should return something like:

```
1. MONDO:0005068
    name: myocardial infarction
    synonyms: heart attack | infarction (MI), myocardial | MI | myocardial infarct

2. MONDO:0005267
    name: heart disorder
    synonyms: cardiac disease | heart disease | disease of heart | disorder of heart

3. MONDO:0005264
    name: transient ischemic attack
    synonyms: attack, transient ischaemic | transient ischemic attacks | TIA - transient ischemic attack
```

2. Hepatocyte (cells in liver)

```bash
metahq search --query "hepatocyte" --type celltype --ontology CL --max-results 3
```

Should return something like:

```
1. CL:0000182
    name: hepatocyte

2. CL:0019028
    name: midzonal region hepatocyte
    synonyms: midzonal hepatocyte

3. CL:0019026
    name: periportal region hepatocyte
    synonyms: periportal hepatocyte

```

### Curation retrieval

```bash
metahq retrieve diseases \
    [--terms "MONDO:0005267,MONDO:0004992"] \
    [--mode direct,propagate,label] [--fmt parquet,tsv,csv,json] [--output filename.etx] \
    [--filters "species=human,db=geo,ecode=expert-curated"] \
    [--metadata "index,group,platform,description"]
```
