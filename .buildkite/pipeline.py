#!/usr/bin/env python3
"""Buildkite dynamic pipeline generator for qdb-api-python.

Step templates in steps/*.yml define nearly-complete Buildkite steps with
{placeholder} variables.  This script loads them, substitutes variables, and
overlays environment variables and the Docker plugin per platform.

Usage:
    python3 pipeline.py           # emit pipeline YAML to stdout
    python3 pipeline.py check     # validate without emitting
"""
from __future__ import annotations

import dataclasses
import sys
from pathlib import Path
import os

from buildkite_sdk import CommandStep, Pipeline

sys.path.insert(0, str(Path(__file__).parent / "tools"))
from qdb_pipeline import (
    Platform,
    apply_docker,
    load_template,
    merge_env,
    select_platforms,
    validate_pipeline,
)  # noqa: E402

STEPS_DIR = Path(__file__).parent / "steps"

# Quasardb-specific toolchain overlays on top of shared infrastructure platforms.
_LINUX = dict(
)
_WIN = dict(
)
_MACOS = dict(
)

_OS_OVERLAY = {"linux": _LINUX, "windows": _WIN, "macos": _MACOS}
PLATFORMS: list[Platform] = [
    dataclasses.replace(p, **_OS_OVERLAY.get(p.os, {}))
    for p in select_platforms(
        "linux-amd64-core2",
        "linux-aarch64",
        "windows-amd64-core2",
        "macos-aarch64",
    )
]

BUILD_TYPES = ["Release", "Debug"]

PYTHON_VERSIONS = [
    # "3.9",
    # "3.10",
    # "3.11",
    # "3.12",
    # "3.13",
    "3.14",
]

# Environment variable layering: global → step → os → os+step → platform compilers.
GLOBAL_ENV: dict[str, str] = {
    "AWS_DEFAULT_REGION": "eu-west-1",
    "PYTHON_EXECUTABLE": "/usr/bin/python3",
    "PYTHON_CMD": "python3",
}

STEP_ENV: dict[str, dict[str, str]] = {
        "test": {"QDB_ENCRYPT_TRAFFIC": "1",},
}

OS_ENV: dict[str, dict[str, str]] = {
    "linux": {},
    "freebsd": {},
    "macos": {},
    "windows": {},
}

OS_STEP_ENV: dict[str, dict[str, str]] = {

}

CPU_ENV: dict[str, dict[str, str]] = {
    "aarch64": {"ARCH": "aarch64"},
}


def _env(p: Platform, step_name: str, build_type: str) -> dict[str, str]:
    """Compose the full environment dict for one step."""
    return merge_env(
        GLOBAL_ENV,
        STEP_ENV.get(step_name, {}),
        OS_ENV.get(p.os, {}),
        OS_STEP_ENV.get(f"{p.os}/{step_name}", {}),
        CPU_ENV.get(p.cpu, {}),
        {"CMAKE_BUILD_TYPE": build_type},
        platform=p,
    )

def _get_git_ref() -> str:
    branch = os.environ.get("BUILDKITE_BRANCH")
    tag = os.environ.get("BUILDKITE_TAG")
    if not branch and not tag:
        raise ValueError(
            "BUILDKITE_BRANCH and BUILDKITE_TAG are both empty — are we running inside a Buildkite job?"
        )

    ref = f"refs/tags/{tag}" if tag else f"refs/heads/{branch}"

    return ref


def _set_artifact_plugin_defaults(step: dict, vars_per_step: dict[str, dict[str, str]]) -> None:
    """
    Goes through list of plugins and fills in defaults for qdb-artifacts plugin if present.
    Doesn't overwrite existing keys, only fills in missing ones from provided dict.
    """
    plugins = step.get("plugins", {})

    for plugin_dict in plugins:
        plugin_name = list(plugin_dict.keys())[0]
        if plugin_name.startswith("bureau14/qdb-artifacts#"):
            plugin_config = plugin_dict[plugin_name]

            for plugin_step, config in plugin_config.items():
                if plugin_step in vars_per_step.keys():
                    for key in vars_per_step[plugin_step]:
                        if key not in config:
                            config[key] = vars_per_step[plugin_step][key]

def _get_agent_python_env(step: dict, platform: Platform, python_version: str) -> dict[str, str]:
    """
    Returns environment variables to set for Python executable on the agent, based on platform and python version.
    Applies to Windows and macOS where we have multiple Python versions installed in different locations.
    """
    # XXX (igor)
    # we can rely on referencing env variables instead of hardcoded paths but we need to update agents first to support this
    if platform.os == "windows":
        return {
            "PYTHON_EXECUTABLE": f"C:\\Python{python_version}-64\\python.exe",
            "PYTHON_CMD": f"C:\\Python{python_version}-64\\python.exe",
        }
    elif platform.os == "macos":
        return {
            "PYTHON_EXECUTABLE": f"/opt/local/bin/python{python_version}",
            "PYTHON_CMD": f"/opt/local/bin/python{python_version}"
        }
    return {}

def _apply_docker_compose(
    step: dict,
    config: dict[str, str] = {},
) -> None:
    DOCKER_COMPOSE_PLUGIN_VERSION="v5.12.1"


    docker_plugin = {
        f"docker-compose#{DOCKER_COMPOSE_PLUGIN_VERSION}": {
            "propagate-uid-gid": True,
            **config,
        },
    }
    existing = step.get("plugins", [])
    step["plugins"] = [docker_plugin] + existing



def generate_pipeline() -> Pipeline:
    """Load templates, expand across platforms × build_types, overlay env and docker."""
    pipeline = Pipeline()
    git_ref = _get_git_ref()

    for p in PLATFORMS:
        for bt in BUILD_TYPES:
            for py in PYTHON_VERSIONS:
                # TODO update slug logic in the submodule
                slug = p.slug(bt.lower()) + f"-py{py.replace('.', '')}"

                # We want to use Release QuasarDB binaries for building debug Python API
                dependency_slug = p.slug("release")

                # TODO: this is just for testing
                git_ref_dep = "refs/heads/sc-18547/buildkite"
                #
                tvars = {"slug": slug, "queue": f"{p.queue_os}-{p.arch}", "build_type": bt, "python_version": py}

                artifact_vars_per_step = {
                    "upload": {"variant": slug, "git-ref": git_ref},
                    "promote": {"variant": slug, "git-ref": git_ref},
                    "download": {"variant": dependency_slug, "git-ref": git_ref_dep},
                    }

                compose_config = {
                    "run": "pypa",
                    "config": "docker/docker-compose.yml",
                    "propagate-environment": True,
                }

                step = load_template(STEPS_DIR / "_build.yml", **tvars)
                env = _env(p, "test", bt)
                env.update(step.get("env") or {})
                env.update({"PYTHON_VERSION": py})
                env.update(_get_agent_python_env(step, p, py))
                step["env"] = env
                if p.os == "linux":
                    _apply_docker_compose(step, compose_config)
                _set_artifact_plugin_defaults(step, artifact_vars_per_step)
                pipeline.add_step(CommandStep.from_dict(step))

    return pipeline


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"

    try:
        pipeline = generate_pipeline()
    except Exception as e:
        print(f"[FAIL] Pipeline generation failed: {e}", file=sys.stderr)
        sys.exit(1)

    if command == "generate":
        print(pipeline.to_yaml())
    elif command == "check":
        errors = validate_pipeline(pipeline)
        if errors:
            for e in errors:
                print(f"[FAIL] {e}", file=sys.stderr)
            sys.exit(1)
        print(f"[OK] Pipeline valid: {len(pipeline.steps)} steps")
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Usage: pipeline.py [generate|check]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
