from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class StageStat:
    stage: str
    status: str
    row_count: int = 0
    duration_ms: float = 0.0
    columns: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    failure_reason: Optional[str] = None


@dataclass
class ExtractionDiagnostics:
    schema_version: str
    status: str
    failure_stage: Optional[str]
    failure_reason: Optional[str]
    backend: str
    backend_version: Optional[str]
    demo_path: str
    stages: List[StageStat] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    notes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractionResult:
    header: Dict[str, Any]
    players: Optional[pd.DataFrame]
    ticks: Optional[pd.DataFrame]
    kills: Optional[pd.DataFrame]
    diagnostics: ExtractionDiagnostics

