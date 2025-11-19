from dataclasses import dataclass
from itertools import product
from typing import List

from exp_runner import runner, MetaData, Variable


@dataclass
class Input:
    a: int
    b: int


@runner()
def experiment(x: Input) -> List[MetaData]:
    return [
        {"sum": x.a + x.b, "product": x.a + x.b},
    ]


def main() -> None:
    inputs = [
        Variable(Input(a, b), {"a": a, "b": b}) for a, b in product([1, 2], [2, 3])
    ]
    experiment(inputs)


if __name__ == "__main__":
    main()
