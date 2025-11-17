from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import (
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Union,
)
import polars as pl


type MetaData = Dict[str, Union[str, int, float, bool, None]]


@dataclass
class Variable[A]:
    value: A
    metadata: MetaData


def parse_dir(dir: Optional[str], default: str) -> Path:
    dir = dir or default
    path = Path(dir)
    return path


def runner[A](
    output_dir: Optional[str] = None,
) -> Callable[
    [Callable[[A], List[MetaData]]], Callable[[Collection[Variable[A]]], None]
]:
    output_path = parse_dir(output_dir, "output")
    output_path.mkdir(parents=True, exist_ok=True)

    def decorator(
        func: Callable[[A], List[MetaData]],
    ) -> Callable[[Collection[Variable[A]]], None]:
        def wrapped(inputs: Collection[Variable[A]]) -> None:
            rows: list[MetaData] = []
            for var in inputs:
                result_rows = func(var.value)
                for result in result_rows:
                    rows.append({**var.metadata, **result})
            df = pl.DataFrame(rows)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")[:-3]
            filename = output_path / f"{func.__name__}_{timestamp}.csv"
            df.write_csv(filename)

        return wrapped

    return decorator
