"""stdlib-only argparse CLI for datasource-kit.

Verbs:
  validate <source>                  — validate a source profile
  run <source> [--out report.json]   — run ingest, optionally save report
  coverage report <report.json>      — print completeness counts
  explain <report.json>              — print explainable trace
  examples run <name> [--out ...]    — run a shipped demo profile
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .errors import DatasourceKitError, ProfileError
from .profile import load_profile, validate_source
from .providers import builtin_registry
from .report import IngestReport
from .runtime import run_ingest

__all__ = ["main"]

# Shipped demo profiles live at repo root / examples / sources / <name>
EXAMPLE_ROOT = Path(__file__).resolve().parents[2] / "examples" / "sources"


def _cmd_validate(args: argparse.Namespace) -> int:
    profile = load_profile(args.source)
    validate_source(profile, builtin_registry())
    print("OK")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    report = run_ingest(args.source)
    if args.out:
        report.save_json(args.out)
        print(f"report written to {args.out}")
    else:
        print(json.dumps(report.as_dict(), indent=2, ensure_ascii=False))
    return 0


def _cmd_coverage_report(args: argparse.Namespace) -> int:
    report = IngestReport.load_json(args.report)
    c = report.completeness
    print(f"source: {report.source_name}")
    print(f"windows_processed: {report.windows_processed}")
    print(f"records_fetched:   {report.records_fetched}")
    print(f"diff:              {json.dumps(report.diff)}")
    print(f"present:           {c.present}")
    print(f"truth:             {c.truth}")
    ratio = c.ratio
    if ratio is not None:
        print(f"ratio:             {ratio:.2%}")
    for layer, count in c.layers.items():
        print(f"  layer[{layer}]: {count}")
    return 0


def _cmd_explain(args: argparse.Namespace) -> int:
    report = IngestReport.load_json(args.report)
    print(f"source_name:      {report.source_name}")
    print(f"kit_version:      {report.kit_version}")
    print(f"source_digest:    {report.source_digest}")
    print(f"retries_used:     {report.retries_used}")
    print(f"rate_limit_waits: {report.rate_limit_waits}")
    if report.warnings:
        print("warnings:")
        for w in report.warnings:
            print(f"  - {w}")
    return 0


def _cmd_examples_run(args: argparse.Namespace) -> int:
    source_path = EXAMPLE_ROOT / args.name
    if not source_path.exists():
        print(
            f"error: example '{args.name}' not found under {EXAMPLE_ROOT}",
            file=sys.stderr,
        )
        return 1
    report = run_ingest(source_path)
    if args.out:
        report.save_json(args.out)
        print(f"report written to {args.out}")
    else:
        print(json.dumps(report.as_dict(), indent=2, ensure_ascii=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datasource-kit")
    sub = parser.add_subparsers(dest="command", required=True)

    p_validate = sub.add_parser("validate", help="validate a source profile folder")
    p_validate.add_argument("source", help="path to source folder or source.json")

    p_run = sub.add_parser("run", help="run ingest for a source profile folder")
    p_run.add_argument("source", help="path to source folder or source.json")
    p_run.add_argument("--out", metavar="FILE", help="write JSON report to FILE")

    p_cov = sub.add_parser("coverage", help="coverage subcommands")
    cov_sub = p_cov.add_subparsers(dest="coverage_command", required=True)
    p_cov_report = cov_sub.add_parser("report", help="print completeness counts")
    p_cov_report.add_argument("report", help="path to saved report JSON")

    p_explain = sub.add_parser("explain", help="print explainable trace from a report")
    p_explain.add_argument("report", help="path to saved report JSON")

    p_examples = sub.add_parser("examples", help="shipped demo profile commands")
    ex_sub = p_examples.add_subparsers(dest="examples_command", required=True)
    p_ex_run = ex_sub.add_parser("run", help="run a shipped demo profile")
    p_ex_run.add_argument("name", help="demo name (e.g. demo-scraper, demo-batch)")
    p_ex_run.add_argument("--out", metavar="FILE", help="write JSON report to FILE")

    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            return _cmd_validate(args)
        if args.command == "run":
            return _cmd_run(args)
        if args.command == "coverage":
            return _cmd_coverage_report(args)
        if args.command == "explain":
            return _cmd_explain(args)
        if args.command == "examples":
            return _cmd_examples_run(args)
    except ProfileError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except DatasourceKitError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0
