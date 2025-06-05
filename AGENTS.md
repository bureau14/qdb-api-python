````markdown
# Project Agents.md Guide for OpenAI Codex

This Agents.md file provides comprehensive guidance for OpenAI Codex and other AI agents working with this codebase.

## Project Structure for OpenAI Codex Navigation

- `/doc`: Utility script for building the pydoc documentation.
- `/quasardb`: QuasarDB Python API source code. Contains both C++ and Python source code.
- `/qdb`: QuasarDB dependencies, including header files, libraries, and utilities. Do not modify.
  - `include/qdb/`: C API headers defining types and functions used by CGO.
- `/scripts`: Utility scripts.
  - `/tests/setup/`: Git submodule with scripts for managing the QuasarDB daemon lifecycle. Do **not** modify these scripts; updating the submodule is permitted.
  - `/teamcity/`: TeamCity CI/CD automation scripts.
    - `10.build.sh`: Builds the module without running tests, ensuring build validity.
    - `20.test.sh`: Executes the test suite, assuming a running QuasarDB daemon.
    - `30.doc.sh`: Generates pydoc documentation; optional for Codex.
  - `/codex`: Scripts for environment preparation specifically used by OpenAI Codex.
- `/tests`: Integration tests using pytest; includes additional C++ tests activated by `QDB_TESTS_ENABLED=1`.
- `/thirdparty`: Vendored third-party C++ dependencies.

## Testing Requirements for OpenAI Codex

### Test Setup

Launch the test clusters from the project root:

```bash
bash scripts/tests/setup/start-services.sh
```

### Running Tests

The project's test suite is executed via the TeamCity script:

```bash
bash scripts/teamcity/20.test.sh
```

Tests leverage `pytest` internally, passing through additional arguments as needed.

Because the full test suite can take approximately 10 minutes, it is strongly recommended that OpenAI Codex first runs individual test modules related specifically to recent changes, providing quicker feedback.

To run a single test module, for example `tests/test_stats.py`, execute:

```bash
bash scripts/teamcity/20.test.sh test_stats.py
```

For early termination upon first test failure, append the `-x` option:

```bash
bash scripts/teamcity/20.test.sh -x test_stats.py
```

This approach allows OpenAI Codex to rapidly iterate and confirm correctness without waiting for the full test suite.

### Test Teardown

Stop the test clusters from the project root:

```bash
bash scripts/tests/setup/stop-services.sh
```
````

## Function and Inline Comment Guidelines

When documenting functions or writing inline comments in the C++ and Python codebases in this repository, adhere strictly to these standards to ensure clarity, maintainability, and readability, optimized especially for LLM consumption.

### General Guidelines (C++ and Python)

- Write concise, informative comments emphasizing **why** and **how** rather than repeating **what** the code already expresses.
- Clearly document design decisions, key assumptions, and performance implications rather than trivial or self-explanatory information.
- Optimize comments for maximum clarity and readability by both humans and LLMs.
- Never use emojis or informal expressions.

### C++ Comment Guidelines

#### Function Comments

Use Doxygen-style block comments placed immediately above function definitions:

```cpp
/**
 * Brief summary clearly stating the purpose of the function.
 *
 * Additional context (if necessary), describing key behavior, details, and intended use.
 *
 * Decision rationale:
 * - Briefly explain important design or implementation choices.
 *
 * Key assumptions:
 * - Specify preconditions or invariants the function depends upon.
 *
 * Performance trade-offs:
 * - Clearly outline performance costs versus benefits.
 *
 * Example usage:
 * // auto batch_size = config.get_batch_size();
 */
constexpr inline std::size_t get_batch_size() const noexcept
{
    return batch_size_;
}
```

#### Inline Comments

Within code blocks, use concise inline comments to clarify subtle or critical behavior:

```cpp
if (err == qdb_e_iterator_end) [[unlikely]]
{
    // Iterator reached end; reset internal state to resemble an "end" iterator.
    handle_      = nullptr;
    reader_      = nullptr;
    batch_size_  = 0;
    table_count_ = 0;
    ptr_         = nullptr;
    n_           = 0;
}
```

### Python Comment Guidelines

#### Function Comments

Follow NumPy-style docstrings clearly documenting parameters, return values, and behavior:

```python
def query(cluster: quasardb.Cluster, query, index=None, blobs=False, numpy=True):
    """
    Execute a query and return results as a dictionary of DataFrames keyed by table names.

    Parameters
    ----------
    cluster : quasardb.Cluster
        Active connection to the QuasarDB cluster.

    query : str
        The query string to execute.

    blobs : bool or list, optional
        Determines how QuasarDB blob columns are returned:
        - If True, all blob columns are returned as bytearrays.
        - If a list, specifies specific columns to return as bytearrays.
        - Defaults to False, meaning blobs are returned as UTF-8 strings.

    numpy : bool, optional
        Indicates whether results leverage NumPy arrays internally. Defaults to True.

    Decision rationale:
    - Returning data as DataFrames for user-friendly consumption while internally leveraging NumPy for efficiency.

    Key assumptions:
    - Provided cluster connection is active and valid.

    Performance trade-offs:
    - Converting between NumPy arrays and DataFrames introduces slight overhead but provides significant convenience.

    Example usage:
    # results = query(cluster, "SELECT * FROM timeseries")
    # df = results["timeseries"]
    """
```

#### Inline Comments

Document key steps, critical behaviors, or notable conditions clearly within code blocks:

```python
if not df.index.is_monotonic_increasing:
    logger.warn(
        "Dataframe index is unsorted; sorting and reindexing dataframe."
    )
    df = df.sort_index().reindex()

# Delegate further processing to qdbnp.write_arrays using numpy arrays directly.
# Pandas DataFrames may implicitly cast sparse integer arrays to floats, causing issues.
data = _extract_columns(df, cinfos)
data["$timestamp"] = df.index.to_numpy(copy=False, dtype="datetime64[ns]")
```

### Summary of Commenting Principles

- **Clarity and context**: Comments must add value beyond the code itself.
- **Structured reasoning**: Clearly document rationale, assumptions, and trade-offs.
- **Inline examples**: Provide concise usage examples prefixed by comment markers to enhance comprehension.

By adhering strictly to these guidelines, comments across the `qdb-api-python` codebase will consistently deliver clarity and insight, optimized for readability by both human and LLM readers.
