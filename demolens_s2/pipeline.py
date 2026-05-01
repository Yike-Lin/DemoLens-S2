from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

import pandas as pd

from .backends.csda_backend import CsDemoAnalyzerBackend
from .diagnostics import classify_exception
from .errors import DemoLensError, EntityNotFoundError, ParseFailedError, UnsupportedPatchError
from .models import ExtractionDiagnostics, ExtractionResult, StageStat
from .normalize import normalize_header, normalize_kills, normalize_players, normalize_ticks


SCHEMA_VERSION = "1.0"


class DemoLensExtractor(object):
    def __init__(self, backend: Optional[Any] = None) -> None:
        self.backend = backend or CsDemoAnalyzerBackend()

    def extract(
        self,
        demo_path: str,
        ticks_players: Optional[Sequence[int]] = None,
        ticks_filter: Optional[Sequence[int]] = None,
    ) -> ExtractionResult:
        demo_path = str(Path(demo_path))
        stages = []
        warnings = []
        first_failure = None

        header: Dict[str, Any] = {}
        players: Optional[pd.DataFrame] = None
        ticks: Optional[pd.DataFrame] = None
        kills: Optional[pd.DataFrame] = None

        header, header_stage, failure = self._execute_stage(
            "header",
            lambda: self.backend.parse_header(demo_path),
            normalize_header,
            demo_path,
            required_nonempty=True,
        )
        stages.append(header_stage)
        if failure is not None:
            return self._finalize(
                demo_path=demo_path,
                header=header,
                players=players,
                ticks=ticks,
                kills=kills,
                stages=stages,
                warnings=warnings,
                failure=failure,
            )

        players, player_stage, failure = self._execute_stage(
            "player_list",
            lambda: self.backend.parse_players(demo_path),
            normalize_players,
            demo_path,
            required_nonempty=True,
        )
        stages.append(player_stage)
        if failure is not None:
            return self._finalize(
                demo_path=demo_path,
                header=header,
                players=players,
                ticks=ticks,
                kills=kills,
                stages=stages,
                warnings=warnings,
                failure=failure,
            )

        ticks, tick_stage, failure = self._execute_stage(
            "ticks",
            lambda: self.backend.parse_ticks(
                demo_path, players=ticks_players, ticks=ticks_filter
            ),
            normalize_ticks,
            demo_path,
            required_nonempty=True,
            context={"patch_version": header.get("patch_version")},
        )
        stages.append(tick_stage)
        if failure is not None:
            first_failure = failure

        kills, kill_stage, failure = self._execute_stage(
            "kills",
            lambda: self.backend.parse_kills(demo_path),
            lambda frame: normalize_kills(frame, players, "kills"),
            demo_path,
            required_nonempty=False,
            context={"patch_version": header.get("patch_version")},
        )
        stages.append(kill_stage)
        if first_failure is None and failure is not None:
            first_failure = failure

        if first_failure is not None:
            return self._finalize(
                demo_path=demo_path,
                header=header,
                players=players,
                ticks=ticks,
                kills=kills,
                stages=stages,
                warnings=warnings,
                failure=first_failure,
            )

        diagnostics = ExtractionDiagnostics(
            schema_version=SCHEMA_VERSION,
            status="success",
            failure_stage=None,
            failure_reason=None,
            backend=getattr(self.backend, "name", self.backend.__class__.__name__),
            backend_version=getattr(self.backend, "version", None),
            demo_path=demo_path,
            stages=stages,
            warnings=warnings,
        )
        return ExtractionResult(
            header=header,
            players=players,
            ticks=ticks,
            kills=kills,
            diagnostics=diagnostics,
        )

    def _finalize(
        self,
        demo_path: str,
        header: Dict[str, Any],
        players: Optional[pd.DataFrame],
        ticks: Optional[pd.DataFrame],
        kills: Optional[pd.DataFrame],
        stages,
        warnings,
        failure: DemoLensError,
    ) -> ExtractionResult:
        diagnostics = ExtractionDiagnostics(
            schema_version=SCHEMA_VERSION,
            status="failed",
            failure_stage=failure.failure_stage,
            failure_reason=failure.failure_reason,
            backend=getattr(self.backend, "name", self.backend.__class__.__name__),
            backend_version=getattr(self.backend, "version", None),
            demo_path=demo_path,
            stages=stages,
            warnings=warnings,
            notes={"failure_category": failure.category, "failure_details": failure.details},
        )
        return ExtractionResult(
            header=header,
            players=players,
            ticks=ticks,
            kills=kills,
            diagnostics=diagnostics,
        )

    def _execute_stage(
        self,
        stage: str,
        load_fn,
        normalize_fn,
        demo_path: str,
        required_nonempty: bool,
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, StageStat, Optional[DemoLensError]]:
        start = time.time()
        raw = None
        try:
            raw = load_fn()
            normalized = normalize_fn(raw)
            row_count = self._row_count(normalized)
            if required_nonempty and row_count == 0:
                raise ParseFailedError(
                    stage,
                    "%s stage produced no rows" % stage,
                    {"demo_path": demo_path, "patch_version": context.get("patch_version") if context else None},
                )
            stat = StageStat(
                stage=stage,
                status="success",
                row_count=row_count if stage != "header" else 1,
                duration_ms=(time.time() - start) * 1000.0,
                columns=self._columns(normalized),
                details={"demo_path": demo_path},
            )
            return normalized, stat, None
        except Exception as exc:
            category, reason, details = classify_exception(stage, exc, context)
            stat = StageStat(
                stage=stage,
                status="failed",
                row_count=self._row_count(raw),
                duration_ms=(time.time() - start) * 1000.0,
                columns=self._columns(raw),
                details=details,
                failure_reason=reason,
            )
            if category == "entity_not_found":
                failure = EntityNotFoundError(stage, reason, details)
            elif category == "unsupported_patch":
                failure = UnsupportedPatchError(stage, reason, details)
            else:
                failure = ParseFailedError(stage, reason, details)
            return None, stat, failure

    @staticmethod
    def _row_count(payload: Any) -> int:
        if isinstance(payload, pd.DataFrame):
            return int(len(payload))
        if isinstance(payload, dict):
            return 1 if payload else 0
        if payload is None:
            return 0
        try:
            return int(len(payload))
        except Exception:
            return 0

    @staticmethod
    def _columns(payload: Any):
        if isinstance(payload, pd.DataFrame):
            return list(payload.columns)
        if isinstance(payload, dict):
            return list(payload.keys())
        return []
