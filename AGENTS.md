# AGENTS.md

Guidance for coding agents working on the QuasarDB Python API.

## Critical rules

- Build and run tests ONLY via `bash scripts/cicd/20.test.sh [pytest args...]`.
  Never run bare pytest, manual cmake, or ad-hoc pip installs.
- Assume qdbd is already running (insecure `qdb://127.0.0.1:2836`, secure
  `qdb://127.0.0.1:2838`). If it is not, start it only via
  `bash scripts/tests/setup/start-services.sh` with `QDB_ENCRYPT_TRAFFIC=1`.
- Run black on every Python file you modify.
- Write Python compatible with 3.7: no `match`, no `X | Y` unions, no
  builtin generics in annotations.
- Never modify `thirdparty/` (vendored) or `qdb/` (extracted C API).
- pytest runs with `-x`: the suite stops at the first failure, so a passing
  prefix does not mean the whole suite is green.

## What this repo is

Python client API for QuasarDB, a distributed timeseries database. The
package wraps the QuasarDB C API through a C++ pybind11 extension, with
numpy and pandas adapters layered on top:

    user code
      -> quasardb.pandas   (DataFrame I/O)
      -> quasardb.numpy    (masked-array I/O, dtype conversion)
      -> quasardb.quasardb (C++ pybind11 extension module)
      -> QuasarDB C API    (libqdb_api, extracted into qdb/)

## Layout

| Path                      | Purpose                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------ |
| `quasardb/*.cpp,*.hpp`    | C++ pybind11 bindings; entry point `module.cpp`                                                        |
| `quasardb/convert/`       | C++ Python<->C value/array conversion                                                                  |
| `quasardb/detail/`        | C++ internals: RAII (`qdb_resource.hpp`), retry, writer                                                |
| `quasardb/CMakeLists.txt` | C++ build (C++20 required)                                                                             |
| `quasardb/__init__.py`    | Imports `from quasardb.quasardb import *`, applies extensions                                          |
| `quasardb/quasardb/*.pyi` | Type stubs for the extension module (no .py implementations)                                           |
| `quasardb/numpy/`         | Numpy adapter: `read_arrays`, `write_arrays`, `query`, `stream_query`                                  |
| `quasardb/pandas/`        | Pandas adapter: `read_dataframe`, `write_dataframe(s)`, `query`, `stream_query`, `stream_dataframe(s)` |
| `quasardb/extensions/`    | Runtime method additions to C++ classes (e.g. legacy writer API)                                       |
| `quasardb/pool.py`        | Connection pooling                                                                                     |
| `quasardb/firehose.py`    | Transaction-log changelog streaming                                                                    |
| `tests/`                  | pytest suite; all fixtures in `tests/conftest.py`                                                      |
| `scripts/cicd/`           | Canonical build/test scripts (see below)                                                               |
| `scripts/tests/setup/`    | qdbd service management (qdb-test-setup submodule)                                                     |
| `qdb/`                    | Extracted QuasarDB C API + server tarballs (not in git)                                                |
| `thirdparty/`             | Vendored C++ libraries (pybind11, ...); never touch or lint                                            |
| `examples/tutorials/`     | Example code, tested by the test suite, injected into docs                                             |

The C++ extension is registered via `register_*()` functions called from
`module.cpp` (cluster, table, writer, reader, entries, errors). The
Python-visible module name is `quasardb.quasardb`.

## Building and testing: use scripts/cicd/20.test.sh

Always build and run tests through the canonical interface:

    bash scripts/cicd/20.test.sh [pytest args...]

All arguments are forwarded verbatim to pytest (invoked from `tests/`):

    bash scripts/cicd/20.test.sh -s test_stats.py            # single file
    bash scripts/cicd/20.test.sh -v -k test_query_find       # by keyword

What the script does, in order:

- reuses (or creates) the `.env/` virtualenv at repo root
- installs `dev-requirements.txt`
- builds the wheel with `QDB_TESTS_ENABLED=ON`, reusing `build/` for
  incremental compilation
- force-reinstalls the wheel into the venv
- execs pytest from `tests/`

This is the same interface CI uses, and it guarantees the extension you
test is the extension you built:

    # WRONG - tests whatever wheel was last installed, not your changes:
    cd tests && pytest test_writer.py

    # RIGHT - rebuilds, reinstalls, then runs pytest with your args:
    bash scripts/cicd/20.test.sh -s test_writer.py

Note: README.md refers to `scripts/teamcity/20.test.sh`; the canonical
path is `scripts/cicd/20.test.sh`.

Useful build environment variables (read by `setup.py`):

    export CMAKE_BUILD_TYPE=Debug     # recommended during development
    export CMAKE_GENERATOR=Ninja      # parallel builds
    export QDB_TESTS_ENABLED=ON       # 20.test.sh sets this itself

### Services (assumed running)

Tests require two local qdbd clusters, assumed to be already running:

- insecure: `qdb://127.0.0.1:2836`
- secure: `qdb://127.0.0.1:2838` (keys: `user_private.key`,
  `cluster_public.key` at repo root; `QDB_ENCRYPT_TRAFFIC=1`)

If they are not running: `bash scripts/tests/setup/start-services.sh`
(with `QDB_ENCRYPT_TRAFFIC=1` exported). Do not manage qdbd any other way.

### Linting

Black only, pinned major version 24, line length 88 (`pyproject.toml`):

    .env/bin/python -m black quasardb/ tests/

Run it on any Python file you modify. `thirdparty/` is excluded.

## Python version constraints

Code must support old Python versions: classifiers say 3.7+, black targets
py37-py313, mypy is pinned to `python_version = 3.7`. Practically:

- no structural pattern matching (`match`), no `X | Y` type unions,
  no `list[int]`-style builtin generics in annotations
  (use `typing.Optional`, `typing.List`, ...)
- be conservative with numpy/pandas APIs; dev-requirements pin different
  numpy/black versions per Python version

## Test suite conventions

The suite is generative and data-driven: fixtures in `tests/conftest.py`
parametrize tests over column types, dtypes, sparsity, row counts, push
modes, and shard sizes, so one test function fans out into dozens of
cases. pytest runs with `-s -x` (stop at first failure) and
`xfail_strict`.

Key fixtures:

- `qdbd_connection` / `qdbd_secure_connection` (module scope, auto-purged)
- `table`: fresh 6-column table (`the_double`, `the_blob`, `the_string`,
  `the_int64`, `the_ts`, `the_symbol`), random name per test
- `gen_cdtype` / `gen_array` / `gen_df`: (ColumnType, np.dtype, data)
  generators driving the data-driven tests; sparse data uses
  `numpy.ma` masked arrays
- `push_mode` (Fast/Transactional/Async), `row_count`, `sparsify`,
  `shard_size`, `reader_batch_size` -- all parametrized

When adding a test:

1. Add `tests/test_<feature>.py`; take fixtures as arguments, never do
   manual connect/cleanup (names are randomized to avoid collisions).
2. Control combinatorial explosion with the conftest decorators:
   `@conftest.override_cdtypes("native")`,
   `@conftest.override_sparsify("none")`,
   `@conftest.override_row_count(100)`.
3. Model on existing files: `test_writer.py` (writer API),
   `test_table_reader.py` (bulk reader), `test_numpy.py` (dtype matrix),
   `test_query_find.py` (queries/tags).
4. Run it through `bash scripts/cicd/20.test.sh -s test_<feature>.py`.

## Domain concepts

- **Cluster**: connection entry point, context manager
  (`quasardb.Cluster("qdb://127.0.0.1:2836")`).
- **Table**: timeseries table; `$timestamp` is the mandatory index column.
  Column types: Double, Int64, Timestamp, String, Blob, Symbol. Columns
  cannot be dropped after creation. Tables have shard size and optional TTL.
- **Writer**: batch writer; push modes Transactional (blocks until
  acknowledged), Fast, Async, Truncate. Wrong-type writes raise
  `IncompatibleTypeError`.
- **Reader**: bulk reader, iterates batches of dicts of numpy arrays;
  batch size and column selection configurable.
- **Query**: SQL-like queries via `Cluster.query()`; pandas/numpy `query()`
  wrap it. `Cluster.stream_query()` (wrapped by `qdbnp.stream_query` /
  `qdbpd.stream_query`) streams results in batches for results that do not
  fit in memory; schema is probed once at open, so dtypes are stable across
  batches. String columns come back as numpy `U` arrays.
- **Entries**: scalar types Blob, String, Integer, Double, Timestamp; tags
  for lookup (`find()`).
- **Errors**: all inherit `quasardb.Error`; specific subclasses like
  `AliasNotFoundError`, `InvalidArgumentError`.

## Gotchas

- Null handling uses `numpy.ma` masked arrays; NaN/NaT are values, not
  nulls. Use `quasardb.numpy.ensure_ma` when constructing inputs.
- tz-naive timestamps are treated as UTC.
- `read_dataframe()` loads everything; use `stream_dataframe()` for large
  tables. Likewise `query()` materializes everything; use `stream_query()`
  for large results.
- Continuous queries exist but are flaky; their tests are skipped due to segfaults.
- `pytest` runs with `-x`: a single failure aborts the run, so a "short"
  test run may hide later failures.
- The `.pyi` stubs under `quasardb/quasardb/` must be kept in sync when
  changing the C++ binding surface.
- After changing C++ code you must rebuild -- which is exactly what
  `20.test.sh` does; there is no separate incremental dev loop (see
  internal ticket QDB-16522).

## When in doubt

If this document disagrees with the repository, the repository wins.
Authoritative sources:

| Question                       | Read                      |
| ------------------------------ | ------------------------- |
| What does the test flow do?    | `scripts/cicd/20.test.sh` |
| Which fixtures exist, and how? | `tests/conftest.py`       |
| Lint / pytest / mypy config?   | `pyproject.toml`          |
| Dependency and tool pins?      | `dev-requirements.txt`    |
