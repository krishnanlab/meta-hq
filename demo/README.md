# MetaHQ demo

MetaHQ is a Python package and CLI for querying tissue, disease, sex, and age annotations of public microarray and RNA-Seq samples and series stored in the Gene Expression Omnibus (GEO).

## Setup

MetaHQ is not yet on PyPI, so we will install the dev version locally. This requires a couple extra steps, but needs to be tested anyways.

1. Clone the repo

For SSH (recommended):

```bash
git clone git@github.com:krishnanlab/meta-hq.git
cd meta-hq
```

For HTTPS:

```bash
git clone https://github.com/krishnanlab/meta-hq.git
cd meta-hq
```

2. Install `uv tools`

`uv tools` is a modern, fast Python dependency and environment manager that is well-suited for deploying Python packages stored in monorepos (e.g., repos that have a core package and a CLI package like MetaHQ).

To install on macOS and Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
source .venv/bin/activate
```

To install on windows:

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
.venv\Scripts\activate
```

3. Install MetaHQ (dev)

Run the following:

```bash
uv pip install -e . ".[dev]"
```

4. Extract MetaHQ database

Currently this is stored in `tar.gz` file in the repo. This will be posted on Zenodo.

```bash
tar -xvf metahq.tar.gz -C ~/metahq
```

5. Configure MetaHQ

```bash
metahq setup --zenodo_doi xxx --data_dir ~/metahq/data
```

## Search

Great, we should be ready to go now. Let's retrieve some annotations. We want to search for heart and brain annotations, but don't know what the ontology term IDs are for these anatomical systems.
So, we can run `metahq search` to figure this out.

Heart:

```bash
metahq search --query heart --type tissue --ontology UBERON -k 3
```

Brain:

```bash
metahq search --query brain --type tissue --ontology UBERON -k 3
```

## Supported

Now that we have our terms, we can retrieve annotations from the MetaHQ database. However, there are lots of filters to include in the query and we know none of them.
So, let's run `metahq supported` to see what's offered by MetaHQ.

```bash
metahq supported
```

## Retrieve tissues

Alright now we're ready to go. Let's retrieve our tissue annotations.

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" --level sample --mode direct --filters "species=human,ecode=expert,technology=microarray" --metadata "sample,series,platform" --fmt tsv --output annotations.tsv
```

Alright that doesn't satisfy my needs though. I also need to know which samples are not heart and brain. Let's change the mode to `--mode label`

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" --level sample --mode label --filters "species=human,ecode=expert,technology=microarray" --metadata "sample,series,platform" --fmt tsv --output annotations.tsv
```

Microarray is so last decade though, so I want some fancy RNA-Seq data. We can change the `technolgy` filter to `rnaseq` and even include metadata from the Sequence Read Archive (SRA).

```bash
metahq retrieve tissues --terms "UBERON:0000948,UBERON:0000955" --level sample --mode label --filters "species=human,ecode=expert,technology=rnaseq" --metadata "sample,series,platform,srx,srp" --fmt tsv --output annotations.tsv
```

## Retrieve sex

Yeah yeah ok we have the tissue labels, but my reviewer yelled at me for not controlling for sex in my analysis. I need to retrieve sex annotations for these samples so I can add the proper controls.

```bash
metahq retrieve sex --terms "M,F" --level sample --filters "species=human,ecode=expert,technology=rnaseq" --metadata "sample" --fmt tsv --output annotations.tsv
```

## Retrieve age

(3 months later...) Turns out they want age information for these samples now too. You know what to do.

```bash
metahq retrieve age --terms "all" --level sample --filters "species=human,ecode=expert,technology=rnaseq" --metadata "sample" --fmt tsv --output annotations.tsv
```
