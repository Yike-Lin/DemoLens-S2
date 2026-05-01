from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .models import ExtractionResult
from .normalize import dataframe_to_records, to_native


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(to_native(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def dump_jsonl(path: Path, frame: Optional[pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = dataframe_to_records(frame)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")


def dump_parquet(path: Path, frame: Optional[pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if frame is None:
        pd.DataFrame().to_parquet(path, index=False)
    else:
        frame.to_parquet(path, index=False)


def export_result(result: ExtractionResult, output_dir: str, ticks_format: str = "jsonl") -> Dict[str, str]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths: Dict[str, str] = {}

    dump_json(out / "demo_meta.json", _build_meta_payload(result))
    paths["demo_meta"] = str(out / "demo_meta.json")

    if result.players is not None:
        dump_json(out / "players.json", dataframe_to_records(result.players))
        paths["players"] = str(out / "players.json")

    if result.ticks is not None:
        ticks_path = out / ("ticks.parquet" if ticks_format == "parquet" else "ticks.jsonl")
        if ticks_format == "parquet":
            dump_parquet(ticks_path, result.ticks)
        else:
            dump_jsonl(ticks_path, result.ticks)
        paths["ticks"] = str(ticks_path)

    if result.kills is not None:
        dump_json(out / "kills.json", dataframe_to_records(result.kills))
        paths["kills"] = str(out / "kills.json")

    dump_json(out / "diagnostics.json", _build_diagnostics_payload(result))
    paths["diagnostics"] = str(out / "diagnostics.json")
    return paths


def _build_meta_payload(result: ExtractionResult) -> Dict[str, Any]:
    return {
        "schema_version": result.diagnostics.schema_version,
        "status": result.diagnostics.status,
        "failure_stage": result.diagnostics.failure_stage,
        "failure_reason": result.diagnostics.failure_reason,
        "backend": result.diagnostics.backend,
        "backend_version": result.diagnostics.backend_version,
        "demo_path": result.diagnostics.demo_path,
        "header": result.header,
        "stage_summary": [to_native(stage.__dict__) for stage in result.diagnostics.stages],
    }


def _build_diagnostics_payload(result: ExtractionResult) -> Dict[str, Any]:
    return {
        "schema_version": result.diagnostics.schema_version,
        "status": result.diagnostics.status,
        "failure_stage": result.diagnostics.failure_stage,
        "failure_reason": result.diagnostics.failure_reason,
        "backend": result.diagnostics.backend,
        "backend_version": result.diagnostics.backend_version,
        "demo_path": result.diagnostics.demo_path,
        "stages": [to_native(stage.__dict__) for stage in result.diagnostics.stages],
        "warnings": result.diagnostics.warnings,
        "notes": result.diagnostics.notes,
    }
