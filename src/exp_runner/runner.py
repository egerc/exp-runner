"""Utility tools for running parameterized experiments and exporting results.

This module provides:
- A `Variable` container type for pairing values with metadata.
- A `runner` decorator factory for executing experiment functions over
  collections of `Variable` instances.
- Helpers for timestamping and saving results as CSV files using Polars.

The typical workflow is:
1. Wrap inputs in `Variable` objects with associated metadata.
2. Decorate an experiment function with `@runner(...)`.
3. Call the wrapped function on a collection of variables.
4. Results are merged with metadata and saved automatically.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import (
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Union,
)
import polars as pl


type MetaData = Dict[str, Union[str, int, float, bool, None]]


@dataclass
class Variable[A]:
    value: A
    metadata: MetaData

def combine_variables[A, B](inputs: Sequence[Variable[A]], func: Callable[[Sequence[A]], B]) -> Variable[B]:
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

    filename = output_path / f"{'_'.join(parts)}.csv"
    dataframe.write_csv(filename)


def runner[A](
    output_dir: Optional[str] = None,
) -> Callable[
    [Callable[[A], List[MetaData]]],
    Callable[[Collection[Variable[A]]], None],
]:
    """Decorator factory for running experiments and saving results.

    Args:
        output_dir: Directory for CSV output. Defaults to "output".

    Returns:
        A decorator that wraps a function returning a list of metadata dicts.
    """

    def decorator(
        func: Callable[[A], List[MetaData]],
    ) -> Callable[[Iterable[Variable[A]]], None]:
        def wrapped(inputs: Iterable[Variable[A]]) -> None:
            rows: List[MetaData] = [
                {**var.metadata, **res} for var in inputs for res in func(var.value)
            ]
            save_df(pl.DataFrame(rows), output_dir, func.__name__)

        return wrapped

    return decorator
