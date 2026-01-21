# exp-runner

Minimal framework for parameterized experiments that collect per-run metadata
and export results as tabular files.

## Requirements

- Python 3.13+ (uses PEP 695 type parameters).
- `polars` and `tqdm` are required runtime dependencies.

## Install

```bash
python -m pip install -e .
```

## Core Concepts

- `Variable(value, metadata)` wraps each input with descriptive metadata.
- `VarProduct.generate_from(...)` builds a cartesian product of inputs.
- `runner(...)` decorates an experiment function that returns a list of row
  metadata dicts (one dict per row).
- Result rows merge input metadata with output metadata; output keys override
  input keys on conflict.
- Outputs are saved to `output/` by default with a timestamped filename.

Supported output formats: `csv`, `json`, `ndjson`, `parquet`, `ipc`,
`ipc_stream`, `clipboard`, `avro`.

## Quickstart

```python
from dataclasses import dataclass

from exp_runner import MetaData, Variable, VarProduct, runner
from exp_runner.runner import combine_variables


@dataclass
class Params(VarProduct):
    lr: float
    batch: int


lrs = [Variable(0.01, {"lr": 0.01}), Variable(0.1, {"lr": 0.1})]
batches = [Variable(16, {"batch": 16}), Variable(32, {"batch": 32})]

inputs = Params.generate_from((lrs, batches))


@runner(output_dir="output", format="csv", verbose=True)
def experiment(p: Params) -> list[MetaData]:
    # Replace with real training/eval logic
    accuracy = 1.0 - p.lr
    return [{"accuracy": accuracy}]


experiment(inputs)
```

## Combining Variables

Use `combine_variables` to derive a new variable from multiple inputs while
preserving metadata.

```python
from exp_runner import Variable
from exp_runner.runner import combine_variables

inputs = [
    Variable(2, {"a": 2}),
    Variable(3, {"b": 3}),
]

summed = combine_variables(inputs, lambda vals: sum(vals))
```

## Runner Options

- `output_dir`: target directory (default: `output`).
- `format`: output format (default: `csv`).
- `head`: limit number of runs (default: `None`).
- `verbose`: show progress via `tqdm` (default: `True`).
