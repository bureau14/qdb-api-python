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
