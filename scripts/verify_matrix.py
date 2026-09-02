#!/usr/bin/env python3
"""Check that the integration matrix still describes this repository.

``contracts/integration-matrix.yaml`` is the contract between the slide, the
code, the tests, the readiness report and the demo rubric. Its failure mode is
silent: a span gets renamed, a journey module moves, a test id is dropped — and
the matrix keeps asserting a system that no longer exists. Nothing fails, the
readiness report still prints ten green points, and the drift is only found on
stage.

So the cross-references are checked in both directions:

* every id, module, span, metric and readiness pillar the matrix names must be
  real, and
* every ``pytest.mark.matrix(...)`` claim in the suites must be declared.

The check is deliberately static — no imports, no network, no running stack —
so it can gate a pull request. Run it directly, or let the fast suite run it for
you: ``tests/test_integration_matrix.py`` calls ``verify()`` on every run.

    python scripts/verify_matrix.py [--matrix PATH] [--quiet]

Exit code 0 means the contract and the repository agree; 1 means they do not.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
MATRIX_PATH = REPO_ROOT / "contracts" / "integration-matrix.yaml"
TELEMETRY_PATH = REPO_ROOT / "src" / "lab28_platform" / "telemetry.py"
METRICS_PATH = REPO_ROOT / "src" / "lab28_platform" / "metrics.py"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"
SUITE_DIRS = (REPO_ROOT / "tests", REPO_ROOT / "integration-tests")

#: The ten integration points on the slide, in slide order.
EXPECTED_POINT_IDS = tuple(f"IP{n:02d}" for n in range(1, 11))
#: Every point must answer all of these; a point missing one is undemonstrable.
REQUIRED_POINT_FIELDS = (
    "id",
    "slide_name",
    "layer",
    "owner",
    "input_contract",
    "output_contract",
    "health_signal",
    "metrics",
    "tests",
    "readiness_check",
    "demo_evidence",
)
#: The readiness pillars the slide scores. ``<pillar>.<check>`` is the format
#: ``readiness._pillar_scores`` splits on, so an unknown prefix silently invents
#: a pillar that no rubric row reads.
READINESS_PILLARS = frozenset(
    {"reliability", "observability", "security", "performance", "operations"}
)
TOP_LEVEL_KEYS = (
    "schema_version",
    "slide",
    "slide_section",
    "roles",
    "points",
    "critical_journeys",
    "required_spans",
    "rubric",
)


@dataclass
class Report:
    """Collected problems, grouped by the question each check answers."""

    failures: list[str] = field(default_factory=list)
    checks: int = 0

    def check(self, condition: bool, message: str) -> bool:
        self.checks += 1
        if not condition:
            self.failures.append(message)
        return condition

    def fail(self, message: str) -> None:
        self.checks += 1
        self.failures.append(message)


def repo_path(path: Path) -> str:
    """Path as written in the matrix, so a message can be pasted into an editor."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


# -- what the code actually declares ---------------------------------------


def declared_spans(path: Path = TELEMETRY_PATH) -> set[str]:
    """String values of the ``SPAN_*`` constants, read without importing."""
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    spans: set[str] = set()
    for node in module.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        if not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id.startswith("SPAN_"):
                spans.add(node.value.value)
    return spans


def declared_metrics(path: Path = METRICS_PATH) -> set[str]:
    """Metric names passed as the first argument of a prometheus_client factory.

    A regex would also find them, but parsing means a name mentioned in a
    docstring or a comment cannot be mistaken for a registered series.
    """
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(module):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        factory = node.func.id if isinstance(node.func, ast.Name) else None
        if factory not in {"Counter", "Gauge", "Histogram", "Summary", "Info"}:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            names.add(first.value)
    return names


def suite_modules() -> Iterator[Path]:
    for directory in SUITE_DIRS:
        if directory.is_dir():
            yield from sorted(directory.glob("test_*.py"))


def claimed_test_ids() -> dict[str, set[str]]:
    """Test ids claimed by ``pytest.mark.matrix("...")``, keyed by id.

    The marker is how a module says *I am the proof of this matrix row*. Reading
    it back is what makes the matrix's ``tests:`` list verifiable instead of
    aspirational.
    """
    claims: dict[str, set[str]] = {}
    for module_path in suite_modules():
        source = module_path.read_text(encoding="utf-8")
        for node in ast.walk(ast.parse(source, filename=str(module_path))):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            if not isinstance(node.func, ast.Attribute) or node.func.attr != "matrix":
                continue
            argument = node.args[0]
            if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                claims.setdefault(argument.value, set()).add(repo_path(module_path))
    return claims


def registered_markers(path: Path = PYPROJECT_PATH) -> set[str]:
    """Marker names from ``[tool.pytest.ini_options] markers``.

    Read with a regex rather than a TOML parser because the only thing needed is
    the leading name of each entry, and ``--strict-markers`` already fails the
    suite on anything unregistered.
    """
    text = path.read_text(encoding="utf-8")
    block = re.search(r"^markers\s*=\s*\[(.*?)^\]", text, re.MULTILINE | re.DOTALL)
    if not block:
        return set()
    return set(re.findall(r'"([a-z_]+)', block.group(1)))


# -- the checks ------------------------------------------------------------


def check_structure(matrix: dict[str, Any], report: Report) -> None:
    for key in TOP_LEVEL_KEYS:
        report.check(key in matrix, f"matrix is missing the top-level key {key!r}")

    report.check(
        str(matrix.get("schema_version", "")) == "1",
        f"unknown schema_version {matrix.get('schema_version')!r}; this checker reads version 1",
    )

    ids = [str(point.get("id")) for point in matrix.get("points", [])]
    report.check(
        tuple(ids) == EXPECTED_POINT_IDS,
        f"the slide has ten integration points in order; the matrix has {ids}",
    )


def check_points(matrix: dict[str, Any], report: Report) -> None:
    roles = set(matrix.get("roles", {}))
    metrics = declared_metrics()
    markers = registered_markers()

    for point in matrix.get("points", []):
        point_id = point.get("id", "<unnamed point>")

        for field_name in REQUIRED_POINT_FIELDS:
            report.check(
                bool(point.get(field_name)),
                f"{point_id}: {field_name} is missing or empty",
            )

        owner = point.get("owner")
        report.check(
            owner in roles,
            f"{point_id}: owner {owner!r} is not one of the declared roles {sorted(roles)}",
        )

        check = str(point.get("readiness_check", ""))
        pillar = check.split(".", 1)[0]
        report.check(
            "." in check and pillar in READINESS_PILLARS,
            f"{point_id}: readiness_check {check!r} must be <pillar>.<check> with pillar in "
            f"{sorted(READINESS_PILLARS)}",
        )

        evidence = str(point.get("demo_evidence", ""))
        report.check(
            evidence.startswith("evidence/"),
            f"{point_id}: demo_evidence {evidence!r} must name a file under evidence/",
        )

        for metric in point.get("metrics", []):
            if not str(metric).startswith("lab28_"):
                continue  # envoy_*, otelcol_* and up come from other exporters.
            report.check(
                metric in metrics,
                f"{point_id}: metric {metric!r} is not registered in {repo_path(METRICS_PATH)}",
            )

        gate = point.get("gate")
        if gate is not None:
            report.check(
                gate in markers,
                f"{point_id}: gate {gate!r} is not a registered pytest marker",
            )
            report.check(
                bool(point.get("gate_note")),
                f"{point_id}: a gate must say why it exists; gate_note is empty",
            )


def check_journeys(matrix: dict[str, Any], report: Report) -> None:
    point_ids = {str(point.get("id")) for point in matrix.get("points", [])}
    journeys = matrix.get("critical_journeys", [])

    report.check(
        len(journeys) == 5,
        f"the slide requires exactly five critical journeys; the matrix declares {len(journeys)}",
    )

    for journey in journeys:
        journey_id = journey.get("id", "<unnamed journey>")
        module = REPO_ROOT / str(journey.get("module", ""))
        report.check(
            module.is_file(),
            f"{journey_id}: module {journey.get('module')!r} does not exist",
        )
        covers = [str(point) for point in journey.get("covers", [])]
        report.check(bool(covers), f"{journey_id}: covers no integration point")
        unknown = sorted(set(covers) - point_ids)
        report.check(not unknown, f"{journey_id}: covers unknown points {unknown}")

    covered = {point for journey in journeys for point in journey.get("covers", [])}
    uncovered = sorted(point_ids - covered)
    report.check(
        not uncovered,
        f"no critical journey covers {uncovered}; every point needs live proof",
    )


def check_test_cross_references(matrix: dict[str, Any], report: Report) -> None:
    """Both directions: the matrix names real tests, the tests claim real rows."""
    declared_journeys = {str(journey.get("id")) for journey in matrix.get("critical_journeys", [])}
    referenced = {test for point in matrix.get("points", []) for test in point.get("tests", [])}
    claims = claimed_test_ids()

    for point in matrix.get("points", []):
        for test_id in point.get("tests", []):
            report.check(
                test_id in claims,
                f"{point['id']}: no module claims pytest.mark.matrix({test_id!r})",
            )

    for journey_id in sorted(declared_journeys):
        report.check(
            journey_id in referenced,
            f"{journey_id} is declared as a critical journey but no point lists it",
        )

    for claimed, modules in sorted(claims.items()):
        report.check(
            claimed in referenced,
            f"{', '.join(sorted(modules))} claims {claimed!r}, which no point declares",
        )


def check_spans(matrix: dict[str, Any], report: Report) -> None:
    required = [str(span) for span in matrix.get("required_spans", [])]
    spans = declared_spans()

    duplicates = sorted({span for span in required if required.count(span) > 1})
    report.check(not duplicates, f"required_spans contains duplicates: {duplicates}")
    for span in required:
        report.check(
            span in spans,
            f"required span {span!r} is not a SPAN_* constant in {repo_path(TELEMETRY_PATH)}",
        )


def check_rubric(matrix: dict[str, Any], report: Report) -> None:
    rubric = matrix.get("rubric", [])
    weights = [entry.get("weight", 0) for entry in rubric]
    report.check(
        sum(weights) == 100,
        f"the rubric weights sum to {sum(weights)}, not the slide's 100",
    )
    for entry in rubric:
        report.check(
            bool(entry.get("criterion")) and bool(entry.get("description")),
            f"rubric row {entry!r} is missing a criterion or a description",
        )


CHECKS = (
    ("structure", check_structure),
    ("integration points", check_points),
    ("critical journeys", check_journeys),
    ("test cross-references", check_test_cross_references),
    ("required spans", check_spans),
    ("rubric", check_rubric),
)


def verify(matrix_path: Path = MATRIX_PATH) -> tuple[int, list[str]]:
    """Run every check and return ``(checks_run, failures)``."""
    matrix = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))
    report = Report()
    for _, check in CHECKS:
        check(matrix, report)
    return report.checks, report.failures


def _print(lines: Iterable[str]) -> None:
    for line in lines:
        print(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--matrix", type=Path, default=MATRIX_PATH, help="matrix file to verify")
    parser.add_argument("--quiet", action="store_true", help="print only failures")
    args = parser.parse_args(argv)

    if not args.matrix.is_file():
        print(f"integration matrix not found at {args.matrix}", file=sys.stderr)
        return 1

    checks, failures = verify(args.matrix)

    if failures:
        _print([f"FAIL  {failure}" for failure in failures])
        print(f"\n{len(failures)} of {checks} checks failed against {repo_path(args.matrix)}")
        return 1

    if not args.quiet:
        print(f"OK    {checks} checks passed: {repo_path(args.matrix)} matches the repository")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
