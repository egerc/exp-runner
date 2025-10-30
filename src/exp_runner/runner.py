# runner.py
import argparse
import importlib
import tomllib
import itertools
from typing import Any, Callable, Dict, List, Optional, Union
import polars as pl
from pathlib import Path
from dataclasses import dataclass
import inspect

from tqdm import tqdm

@dataclass
class Variable[A]:
    value: A
    name: Union[str, int]
    metadata: Optional[Dict[Union[str, int], Union[str, int, float, bool]]] = None

    @property
    def has_metadata(self) -> bool:
        return bool(self.metadata)


def variable_range(start: int, stop: int, step: Optional[int] = None):
    """Generate Variables for a range of values."""
    for i in range(start, stop, step or 1):
        yield Variable(i, i)


def load_config(path: str) -> Dict[str, Any]:
    """Load TOML config file."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def build_iterator(param_name: str, spec: Any, fn: Callable) -> List[Variable]:
    """Build a list of Variable objects for a parameter."""
    if isinstance(spec, dict):
        if "range" in spec:
            r = spec["range"]
            return [Variable(i, i) for i in range(r["start"], r["stop"], r.get("step", 1))]
        elif "custom" in spec:
            module_name = Path(fn.__code__.co_filename).stem
            module = importlib.import_module(module_name)
            gen_fn = getattr(module, spec["custom"])
            return list(gen_fn(**spec.get("args", {})))
        else:
            raise ValueError(f"Unknown dict spec for {param_name}: {spec}")
    elif isinstance(spec, list):
        return [Variable(v, v) for v in spec]
    else:
        raise ValueError(f"Invalid spec type for {param_name}: {spec}")


def combo_to_records(combo: List[Variable], fn: Callable, result: Dict) -> Dict:
    """
    Convert a single experiment result into a single row (wide format).
    Columns: function arguments in order + one column per metric.
    """
    fn_args = list(inspect.signature(fn).parameters.keys())
    row: Dict[str, Any] = {}
    combo_dict = {var_name: var for var, var_name in zip(combo, fn_args)}
    for arg in fn_args:
        row[arg] = combo_dict[arg].name
    row.update(result)  # add metrics as separate columns
    return row


def run(fn: Callable[[Any], Dict[Union[str, int], Union[str, int, float, bool]]]):
    """Decorator to run experiment over parameter combinations from a TOML config."""
    def wrapper(config_path: Optional[str] = None):
        if config_path is None:
            parser = argparse.ArgumentParser(description="Run experiment.")
            parser.add_argument("--config", type=str, required=True, help="Path to TOML config file.")
            args = parser.parse_args()
            config_path_final = args.config
        else:
            config_path_final = config_path

        cfg = load_config(config_path_final)
        param_specs = cfg.get("params", {})
        output_dir = Path(cfg.get("output", {}).get("dir", "results"))
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build iterators for all parameters
        iterators = {k: build_iterator(k, spec, fn) for k, spec in param_specs.items()}

        # Save metadata CSVs for parameters that have metadata (wide format)
        for param, vars_ in iterators.items():
            metadata_rows = []
            for var in vars_:
                if var.metadata:
                    row = {param: var.name}  # variable name column
                    row.update(var.metadata)  # properties become columns
                    metadata_rows.append(row)
            if metadata_rows:
                meta_df = pl.DataFrame(metadata_rows)
                meta_file = output_dir / f"{param}_metadata.csv"
                meta_df.write_csv(meta_file)
                print(f"{param} metadata\n", meta_df)

        # Run all combinations and save main results CSV (wide format)
        all_records: List[Dict] = []
        fn_args = list(inspect.signature(fn).parameters.keys())
        for combo in tqdm(itertools.product(*(iterators[k] for k in fn_args))):
            fn_kwargs = {k: v.value for k, v in zip(fn_args, combo)}
            result = fn(**fn_kwargs)
            all_records.append(combo_to_records(list(combo), fn, result))

        df = pl.DataFrame(all_records)
        main_csv = output_dir / "results.csv"
        df.write_csv(main_csv)
        print(df)

    return wrapper