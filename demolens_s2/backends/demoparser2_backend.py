from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from demoparser2 import DemoParser

from ..diagnostics import classify_exception
from ..errors import EntityNotFoundError, ParseFailedError


class Demoparser2Backend(object):
    name = "demoparser2"
    tick_prop_candidates = [
        ["X", "Y", "Z", "pitch", "yaw", "health", "is_alive", "active_weapon_name"],
        ["X", "Y", "Z", "pitch", "yaw", "health", "is_alive", "active_weapon"],
        ["X", "Y", "Z", "health", "is_alive", "active_weapon"],
        ["X", "Y", "Z"],
        ["m_vecOrigin", "m_angEyeAngles[0]", "m_angEyeAngles[1]", "m_iHealth", "m_bPawnIsAlive", "m_hActiveWeapon"],
    ]

    kill_event_candidates = [
        ("parse_event", {"event_name": "player_death", "player": None, "other": ["game_time", "round_start_time", "total_rounds_played"]}),
        ("parse_event", {"event_name": "player_death", "other": ["game_time", "round_start_time", "total_rounds_played"]}),
        ("parse_events", {"event_name": ["player_death"], "other": ["game_time", "round_start_time", "total_rounds_played"]}),
        ("parse_event", {"event_name": "player_death"}),
    ]

    def __init__(self) -> None:
        self.version = self._read_version()

    @staticmethod
    def _read_version() -> Optional[str]:
        try:
            from importlib.metadata import version

            return version("demoparser2")
        except Exception:
            return None

    def _new_parser(self, demo_path: str) -> DemoParser:
        return DemoParser(str(demo_path))

    def parse_header(self, demo_path: str) -> Dict[str, Any]:
        parser = self._new_parser(demo_path)
        return parser.parse_header()

    def parse_players(self, demo_path: str) -> pd.DataFrame:
        parser = self._new_parser(demo_path)
        return parser.parse_player_info()

    def probe_tick_fields(self, demo_path: str) -> Dict[str, Any]:
        parser = self._new_parser(demo_path)
        try:
            fields = parser.list_updated_fields()
            return {"ok": True, "fields": fields}
        except Exception as exc:
            category, reason, details = classify_exception("ticks", exc)
            return {"ok": False, "category": category, "reason": reason, "details": details}

    def probe_game_events(self, demo_path: str) -> Dict[str, Any]:
        parser = self._new_parser(demo_path)
        try:
            events = parser.list_game_events()
            return {"ok": True, "events": events}
        except Exception as exc:
            category, reason, details = classify_exception("kills", exc)
            return {"ok": False, "category": category, "reason": reason, "details": details}

    def parse_ticks(
        self,
        demo_path: str,
        players: Optional[Sequence[int]] = None,
        ticks: Optional[Sequence[int]] = None,
    ) -> pd.DataFrame:
        parser = self._new_parser(demo_path)
        last_exc = None
        for props in self.tick_prop_candidates:
            try:
                kwargs: Dict[str, Any] = {}
                if players is not None:
                    kwargs["players"] = players
                if ticks is not None:
                    kwargs["ticks"] = ticks
                return parser.parse_ticks(props, **kwargs)
            except Exception as exc:
                last_exc = exc
        category, reason, details = classify_exception(
            "ticks",
            last_exc if last_exc is not None else ParseFailedError("ticks", "parse_ticks failed"),
            {"candidate_sets": self.tick_prop_candidates},
        )
        if category == "entity_not_found":
            raise EntityNotFoundError("ticks", reason, details)
        if category == "unsupported_patch":
            raise ParseFailedError("ticks", reason, details)
        raise ParseFailedError("ticks", reason, details)

    def parse_kills(self, demo_path: str) -> pd.DataFrame:
        parser = self._new_parser(demo_path)
        probe = self.probe_game_events(demo_path)
        if probe.get("ok") and "player_death" not in probe.get("events", []):
            raise EntityNotFoundError(
                "kills",
                "player_death event not present in demo",
                {"probe": probe},
            )

        last_exc = None
        for method_name, kwargs in self.kill_event_candidates:
            try:
                if method_name == "parse_event":
                    return parser.parse_event(kwargs["event_name"], player=kwargs.get("player"), other=kwargs.get("other"))
                return parser.parse_events(kwargs["event_name"], player=kwargs.get("player"), other=kwargs.get("other"))[0][1]
            except Exception as exc:
                last_exc = exc
        category, reason, details = classify_exception(
            "kills",
            last_exc if last_exc is not None else ParseFailedError("kills", "parse_event failed"),
            {"candidate_sets": self.kill_event_candidates, "probe": probe},
        )
        if category == "entity_not_found":
            raise EntityNotFoundError("kills", reason, details)
        if category == "unsupported_patch":
            raise ParseFailedError("kills", reason, details)
        raise ParseFailedError("kills", reason, details)
