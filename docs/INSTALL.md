# MetaHQ installation

## Normal install

Run the following command:

```bash
pip install metahq
```

## Development install

### uv tools

If installing an editable version of `metahq` locally, use `uv tools` (docs [here](https://docs.astral.sh/uv/concepts/tools/)).

### metahq install

To install `metahq`, run the following within the root of the `metahq` repository:

```bash
uv pip install -e . ".[dev]"
```
