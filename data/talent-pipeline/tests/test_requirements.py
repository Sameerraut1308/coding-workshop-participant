"""requirements.txt is what the EKS job installs: only runtime packages, every one pinned.

The job runs ``pip install -r requirements.txt`` on top of its container image. Loose versions or extra
tools there let pip pick combinations that break in that image (pyarrow 26 with numpy 1.26 did).
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = {"numpy", "pandas", "pyarrow", "boto3", "sqlalchemy", "psycopg2-binary"}
# import name -> package that provides it in requirements.txt
PROVIDED_BY = {"numpy": "numpy", "pandas": "pandas", "pyarrow": "pyarrow", "boto3": "boto3",
               "botocore": "boto3", "sqlalchemy": "sqlalchemy", "psycopg2": "psycopg2-binary"}


def _requirements(name: str) -> dict[str, str]:
    """Package name (lower case) -> version spec, ignoring comments."""
    reqs = {}
    for line in (ROOT / name).read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            pkg, spec = re.match(r"([A-Za-z0-9_.\-\[\]]+)\s*(.*)", line).groups()  # type: ignore[union-attr]
            reqs[pkg.lower()] = spec
    return reqs


def test_job_requirements_are_runtime_only_and_pinned() -> None:
    reqs = _requirements("requirements.txt")
    assert set(reqs) == RUNTIME
    assert all(re.fullmatch(r"==\d+(\.\d+)*", spec) for spec in reqs.values()), reqs
    assert reqs["numpy"] == "==1.24.4"  # the job image's own numpy; its compiled packages need numpy < 2


def test_job_requirements_cover_every_third_party_import() -> None:
    """Every non-stdlib module the job imports (job.py + pipeline/) is installed by requirements.txt."""
    sources = [ROOT / "job.py", *sorted((ROOT / "pipeline").glob("*.py"))]
    imported = set()
    for path in sources:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                imported |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported.add(node.module.split(".")[0])
    third_party = imported - set(sys.stdlib_module_names) - {"pipeline", "__future__"}
    assert third_party <= set(PROVIDED_BY), third_party - set(PROVIDED_BY)
    assert {PROVIDED_BY[m] for m in third_party} <= set(_requirements("requirements.txt"))


def test_dev_requirements_keep_local_tools() -> None:
    dev = _requirements("requirements-dev.txt")
    assert RUNTIME - {"numpy"} <= set(dev)  # numpy comes with pandas
    assert {"pytest", "pytest-cov", "jupyter", "matplotlib", "moto[server]"} <= set(dev)
