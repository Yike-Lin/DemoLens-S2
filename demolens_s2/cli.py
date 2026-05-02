from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .backends.csda_backend import CsDemoAnalyzerBackend
from .exporters import export_result
from .pipeline import DemoLensExtractor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="demolens-s2")
    parser.add_argument("demo", help="Path to a CS2 .dem file")
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Directory for demo_meta.json, players.json, ticks.jsonl/parquet, kills.json, diagnostics.json",
    )
    parser.add_argument(
        "--ticks-format",
        choices=["jsonl", "parquet"],
        default="jsonl",
        help="Output format for per-tick player state",
    )
    parser.add_argument(
        "--source",
        choices=["auto", *CsDemoAnalyzerBackend.supported_sources],
        default="auto",
        help="Force cs-demo-analyzer demo source when auto detection is unreliable",
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    demo_path = Path(args.demo)
    output_dir = args.output_dir or str(demo_path.parent / (demo_path.stem + "_out"))
    backend = CsDemoAnalyzerBackend(
        source=None if args.source == "auto" else args.source
    )
    extractor = DemoLensExtractor(backend=backend)
    result = extractor.extract(str(demo_path))
    paths = export_result(result, output_dir, ticks_format=args.ticks_format)
    print(json.dumps(paths, ensure_ascii=False, indent=2))
    if result.diagnostics.status != "success":
        print(
            json.dumps(
                {
                    "failure_stage": result.diagnostics.failure_stage,
                    "failure_reason": result.diagnostics.failure_reason,
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
