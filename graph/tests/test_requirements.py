import ast
import re
import sys
from importlib.metadata import packages_distributions
from pathlib import Path

GRAPH_DIR = Path(__file__).resolve().parent.parent
SOURCES = [p for pattern in ("*.py", "lib/*.py", "dashboard/*.py", "tests/*.py")
           for p in GRAPH_DIR.glob(pattern)]
LOCAL_MODULES = {"lib"}


def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def third_party_imports():
    """Top-level modules imported anywhere in graph/, minus stdlib and lib/."""
    modules = set()
    for path in SOURCES:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                modules.add(node.module.split(".")[0])
    return modules - set(sys.stdlib_module_names) - LOCAL_MODULES


def required_distributions():
    names = set()
    for line in (GRAPH_DIR / "requirements.txt").read_text(encoding="utf-8").splitlines():
        match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", line.split("#")[0])
        if match:
            names.add(normalize(match.group(1)))
    return names


def test_every_third_party_import_is_in_requirements():
    """requirements.txt is the only record of how to rebuild the venv.

    graphframes was installed by hand in Task 1 and never written down, so a
    fresh `pip install -r requirements.txt` could not import it in stage 0,
    stage 3, the Spark benchmark worker or the test suite.
    """
    installed = packages_distributions()
    required = required_distributions()
    missing = sorted(
        module for module in third_party_imports()
        if not {normalize(d) for d in installed.get(module, [module])} & required
    )
    assert missing == []
