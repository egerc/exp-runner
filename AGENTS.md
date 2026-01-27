# Repository Guidelines

## Project Structure & Module Organization

- `src/exp_runner/`: Core library code (e.g., `runner.py` with `Variable`, `VarProduct`, and `runner`).
- `tests/`: Test suite (currently `test_runner_simple.py`).
- `output/`: Default location for generated experiment results; safe to clean between runs.
- `pyproject.toml`: Packaging metadata, dependencies, and pytest configuration.

## Build, Test, and Development Commands

- `python -m pip install -e .`: Install in editable mode for local development.
- `python -m pytest`: Run the test suite (uses settings in `pyproject.toml`).
- `python -m pytest -q`: Quieter test output for quick checks.

## Coding Style & Naming Conventions

- Indentation: 4 spaces; follow standard PEP 8 formatting.
- Typing: Uses Python 3.13+ and PEP 695 type parameters (e.g., `def runner[A](...)`).
- Naming: `snake_case` for functions/variables, `PascalCase` for classes, `SCREAMING_SNAKE_CASE` for constants.
- Modules: `snake_case` filenames under `src/exp_runner/`.

## Testing Guidelines

- Framework: `pytest` (configured in `pyproject.toml`).
- Location: Add new tests under `tests/` with filenames like `test_*.py`.
- Focus: Validate metadata merging, output formats, and decorator behavior.
- Coverage: No explicit coverage target; add tests when fixing bugs or changing core behavior.

## Commit & Pull Request Guidelines

- Commit messages: The current history favors short, plain-English summaries (no strict prefixing).
- Suggested style: concise imperative or descriptive phrases, e.g., "add parquet output support".
- PRs: Include a brief description, what changed, and how to verify (commands + expected results).
- If changes affect outputs, include example files or note output format changes.

## Security & Configuration Tips

- Generated files land in `output/` by default; avoid committing large result files.
- When running experiments, ensure inputs/metadata do not leak secrets into exported tables.
