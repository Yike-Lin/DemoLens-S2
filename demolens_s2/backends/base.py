from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, Sequence

import pandas as pd


class DemoParserBackend(Protocol):
    name: str

    def parse_header(self, demo_path: str) -> Dict[str, Any]:
        raise NotImplementedError

    def parse_players(self, demo_path: str) -> pd.DataFrame:
        raise NotImplementedError

    def parse_ticks(
        self,
        demo_path: str,
        players: Optional[Sequence[int]] = None,
        ticks: Optional[Sequence[int]] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError

    def parse_kills(self, demo_path: str) -> pd.DataFrame:
        raise NotImplementedError

