"""
Utility tools for parameterized experiments and result management.

This module provides:

- `Variable`: A container pairing a value with associated metadata.
- `VarProduct`: A base class for generating all combinations of `Variable` instances,
  automatically constructing subclass instances with merged metadata.
- `combine_variables`: Helper to combine multiple `Variable` instances into one
  by applying a function to their values.
- `runner`: Decorator factory to execute experiment functions over collections
  of `Variable` instances and export results as CSV.
- Utility functions:
  - `get_timestamp`: Generate a high-resolution timestamp string.
  - `save_df`: Save a Polars DataFrame as a CSV with timestamped filename.

Typical workflow:

1. Wrap inputs in `Variable` objects with metadata describing each variable.
2. Use `VarProduct.generate_from` to create all combinations of inputs (if needed).
3. Define an experiment function that accepts a dataclass or object and returns
   a list of metadata dictionaries.
4. Decorate the function with `@runner(...)` to automatically run it over variables
   and save results to CSV.
"""

from dataclasses import dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Self,
    Sequence,
    Tuple,
    Union,
)
import polars as pl


type MetaData = Dict[str, Union[str, int, float, bool, None]]
type df_disk_formats = Literal[
    "avro",
    "clipboard",
    "csv",
    "database",
    "delta",
    "excel",
    "iceberg",
    "ipc",
    "ipc_stream",
    "json",
    "ndjson",
    "parquet",
    "csv",
]


@dataclass
class Variable[A]:
    value: A
    metadata: MetaData


def combine_variables[A, B](
    inputs: Sequence[Variable[A]], func: Callable[[Sequence[A]], B]
) -> Variable[B]:
    value = func([var.value for var in inputs])
    metadata: MetaData = {}
    for var in inputs:
        metadata.update(var.metadata)
    return Variable(value=value, metadata=metadata)


def get_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")[:-3]


def save_df(
    dataframe: pl.DataFrame,
    output_dir: Optional[str] = None,
    name: Optional[str] = None,
    format: df_disk_formats = "csv",
) -> None:
    """Save a DataFrame as a CSV file.

    Args:
        dataframe: The DataFrame to save.
        output_dir: Directory to write into. Defaults to "output".
        name: Optional suffix added to the filename.
    """
    output_dir = output_dir or "output"
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    parts = [get_timestamp()]
    if name:
        parts.append(name)

    writer_map = {
        "csv": dataframe.write_csv,
        "json": dataframe.write_json,
        "ndjson": dataframe.write_ndjson,
        "parquet": dataframe.write_parquet,
        "ipc": dataframe.write_ipc,
        "ipc_stream": dataframe.write_ipc_stream,
        "excel": dataframe.write_excel,
        "clipboard": dataframe.write_clipboard,
        "avro": dataframe.write_avro,
        "delta": dataframe.write_delta,
        "iceberg": dataframe.write_iceberg,
        "database": dataframe.write_database,
    }
    writer = writer_map.get(format)
    filename = output_path / f"{'_'.join(parts)}"
    writer(filename)


class VarProduct:
    @classmethod
    def generate_from(
        cls: type[Self],
        inputs: Tuple[Iterable[Variable[Any]], ...],
    ) -> Generator[Variable[Self], None, None]:
        for combo in product(*inputs):
            values = [v.value for v in combo]
            metadata = {}
            for v in combo:
                metadata.update(v.metadata)  # type: ignore
            yield Variable(cls(*values), metadata)  # type: ignore


def runner[A](
    output_dir: Optional[str] = None,
    format: df_disk_formats = "csv",
    head: Optional[int] = None,
) -> Callable[
    [Callable[[A], List[MetaData]]],
    Callable[[Iterable[Variable[A]]], None],
]:
    def decorator(
        func: Callable[[A], List[MetaData]],
    ) -> Callable[[Iterable[Variable[A]]], None]:
        def wrapped(inputs: Iterable[Variable[A]]) -> None:
            rows: List[MetaData] = []

            for i, var in enumerate(inputs):
                if head is not None and i >= head:
                    break

                for res in func(var.value):
                    rows.append({**var.metadata, **res})

            df = pl.DataFrame(rows)
            save_df(df, output_dir, func.__name__, format=format)

        return wrapped

    return decorator
