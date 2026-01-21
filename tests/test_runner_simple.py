from dataclasses import dataclass

import polars as pl

from exp_runner import MetaData, Variable, VarProduct, runner


@dataclass
class Params(VarProduct):
    a: int
    b: int


def test_runner_writes_csv_and_merges_metadata(tmp_path):
    a_values = [Variable(1, {"a": 1}), Variable(2, {"a": 2})]
    b_values = [Variable(3, {"b": 3}), Variable(4, {"b": 4})]
    inputs = Params.generate_from((a_values, b_values))

    @runner(output_dir=str(tmp_path), format="csv", verbose=True)
    def experiment(params: Params) -> list[MetaData]:
        return [{"sum": params.a + params.b}]

    experiment(inputs)

    csv_files = list(tmp_path.glob("*.csv"))
    assert len(csv_files) == 1

    df = pl.read_csv(csv_files[0])
    assert set(df.columns) == {"a", "b", "sum"}
    assert df.height == 4
    assert set(df.select(["a", "b", "sum"]).rows()) == {
        (1, 3, 4),
        (1, 4, 5),
        (2, 3, 5),
        (2, 4, 6),
    }
