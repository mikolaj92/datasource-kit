"""CLI entry point: ``python -m datasource_kit`` or ``datasource-kit``.

Usage:
    datasource-kit examples run demo-scraper
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

__all__: list[str] = []


def _run_demo(profile_name: str) -> None:
    from .ingest import run_ingest
    from .providers import builtin_registry
    from .storage import InMemoryArtifactStore

    # Fake records — no network, no extras, no consumer code.
    fake_records = [{"id": str(i), "value": f"item-{i}"} for i in range(5)]
    store: InMemoryArtifactStore = InMemoryArtifactStore()
    registry = builtin_registry()

    report = run_ingest(
        enumerator=lambda: fake_records,
        fetcher=lambda item: item,
        store=store,
        registry=registry,
        diff_provider="by_id",
        assess_provider="passthrough",
        max_retries=1,
    )

    print(f"Profile: {profile_name}")
    print(f"Report:  {report.summary()}")
    print(f"Store:   {len(store)} artifact(s) persisted")
    for label, count in report.totals.items():
        print(f"  {label}: {count}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="datasource-kit")
    sub = parser.add_subparsers(dest="command")

    ex = sub.add_parser("examples", help="Run bundled example profiles")
    ex_sub = ex.add_subparsers(dest="action")
    run_p = ex_sub.add_parser("run", help="Run a named example profile")
    run_p.add_argument("profile", help="Profile name (e.g. demo-scraper)")

    args = parser.parse_args(argv)

    if args.command == "examples" and args.action == "run":
        _run_demo(args.profile)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
