from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd

from .errors import MissingFieldError, ParseFailedError


HEADER_REQUIRED_FIELDS = [
    "map_name",
    "demo_version_name",
    "demo_version_guid",
    "patch_version",
    "fullpackets_version",
    "server_name",
]

PLAYER_REQUIRED_FIELDS = ["steamid", "name", "team_number"]
TICK_REQUIRED_FIELDS = [
    "tick",
    "steamid",
    "X",
    "Y",
    "Z",
    "pitch",
    "yaw",
    "is_alive",
    "active_weapon",
    "health",
]
KILL_REQUIRED_FIELDS = [
    "tick",
    "attacker_steamid",
    "victim_steamid",
    "attacker_name",
    "victim_name",
    "weapon",
    "headshot",
    "wallbang",
    "through_smoke",
    "round_num",
    "time_sec",
    "round_time_sec",
]


def to_native(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): to_native(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_native(item) for item in value]
    if isinstance(value, tuple):
        return [to_native(item) for item in value]
    if isinstance(value, set):
        return [to_native(item) for item in value]
    if hasattr(value, "item") and callable(value.item):
        try:
            return to_native(value.item())
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, pd.Timedelta):
        return value.total_seconds()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def dataframe_to_records(frame: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    records = frame.to_dict(orient="records")
    return [to_native(record) for record in records]


def _first_existing_column(frame: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _series_or_none(frame: pd.DataFrame, candidates: Sequence[str]) -> Optional[pd.Series]:
    column = _first_existing_column(frame, candidates)
    if column is None:
        return None
    return frame[column]


def _ensure_frame(value: Any) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(value)


def _coerce_bool_series(series: pd.Series) -> pd.Series:
    def _coerce(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not pd.isna(value):
            return bool(value)
        if isinstance(value, str):
            lower = value.strip().lower()
            if lower in ("true", "1", "yes"):
                return True
            if lower in ("false", "0", "no"):
                return False
        return value

    return series.map(_coerce)


def _coerce_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _cast_if_present(frame: pd.DataFrame, column: str, caster) -> None:
    if column in frame.columns:
        frame[column] = caster(frame[column])


def _validate_required_columns(
    frame: pd.DataFrame, required: Sequence[str], stage: str
) -> None:
    missing = []
    for column in required:
        if column not in frame.columns:
            missing.append(column)
            continue
        if len(frame[column]) > 0 and frame[column].isna().all():
            missing.append(column)
    if missing:
        raise MissingFieldError(
            stage,
            "missing required fields: %s" % ", ".join(missing),
            details={"missing_fields": missing},
        )


def normalize_header(raw: Mapping[str, Any]) -> Dict[str, Any]:
    header = {str(key): to_native(value) for key, value in dict(raw).items()}
    aliases = {
        "map_name": "mapName",
        "demo_version_name": "demoVersionName",
        "demo_version_guid": "demoVersionGuid",
        "patch_version": "networkProtocol",
        "fullpackets_version": "fullpacketsVersion",
        "server_name": "serverName",
    }
    for canonical, alias in aliases.items():
        if canonical not in header and alias in header:
            header[canonical] = header[alias]
    if "patch_version" in header:
        header["patch_version"] = _maybe_int(header["patch_version"])
    if "fullpackets_version" in header:
        header["fullpackets_version"] = _maybe_int(header["fullpackets_version"])
    if "network_protocol" in header:
        header["network_protocol"] = _maybe_int(header["network_protocol"])
    for key in ("allow_clientside_entities", "allow_clientside_particles"):
        if key in header:
            header[key] = _maybe_bool(header[key])
    _validate_required_mapping(header, HEADER_REQUIRED_FIELDS, "header")
    return header


def _validate_required_mapping(
    mapping: Mapping[str, Any], required: Sequence[str], stage: str
) -> None:
    missing = []
    for column in required:
        if column not in mapping or mapping[column] is None:
            missing.append(column)
    if missing:
        raise MissingFieldError(
            stage,
            "missing required fields: %s" % ", ".join(missing),
            details={"missing_fields": missing},
        )


def _maybe_int(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("-"):
            digits = text[1:]
            sign = -1
        else:
            digits = text
            sign = 1
        if digits.isdigit():
            try:
                return sign * int(digits)
            except Exception:
                return value
    return value


def _maybe_bool(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lower = value.strip().lower()
        if lower in ("true", "1", "yes"):
            return True
        if lower in ("false", "0", "no"):
            return False
    return value


def normalize_players(raw: Any) -> pd.DataFrame:
    frame = _ensure_frame(raw)
    if frame.empty:
        raise ParseFailedError(
            "player_list",
            "player list is empty",
            details={"row_count": 0},
        )
    result = pd.DataFrame(index=frame.index)
    result["steamid"] = _series_or_none(frame, ["steamid", "player_steamid"])
    result["name"] = _series_or_none(frame, ["name", "player_name"])
    result["team_number"] = _series_or_none(frame, ["team_number", "team_num"])
    _validate_required_columns(result, PLAYER_REQUIRED_FIELDS, "player_list")
    return result


def normalize_ticks(raw: Any, stage: str = "ticks") -> pd.DataFrame:
    frame = _ensure_frame(raw)
    if frame.empty:
        raise ParseFailedError(stage, "tick stream is empty", details={"row_count": 0})
    result = pd.DataFrame(index=frame.index)
    result["tick"] = _series_or_none(frame, ["tick", "game_tick"])
    result["steamid"] = _series_or_none(frame, ["steamid", "player_steamid"])
    result["X"] = _series_or_none(frame, ["X", "m_vecOrigin[0]", "origin_x", "pos_x"])
    result["Y"] = _series_or_none(frame, ["Y", "m_vecOrigin[1]", "origin_y", "pos_y"])
    result["Z"] = _series_or_none(frame, ["Z", "m_vecOrigin[2]", "origin_z", "pos_z"])
    result["pitch"] = _series_or_none(frame, ["pitch", "m_angEyeAngles[0]"])
    result["yaw"] = _series_or_none(frame, ["yaw", "m_angEyeAngles[1]"])
    result["is_alive"] = _series_or_none(
        frame, ["is_alive", "m_bPawnIsAlive", "alive"]
    )
    result["active_weapon"] = _series_or_none(
        frame,
        ["active_weapon", "active_weapon_name", "weapon", "weapon_name", "m_hActiveWeapon"],
    )
    result["health"] = _series_or_none(frame, ["health", "m_iHealth"])
    _validate_required_columns(result, TICK_REQUIRED_FIELDS, stage)
    result["is_alive"] = _coerce_bool_series(result["is_alive"])
    for column in ("tick", "steamid", "X", "Y", "Z", "pitch", "yaw", "health"):
        result[column] = _coerce_numeric_series(result[column])
    return result


def normalize_kills(
    raw: Any,
    players: Optional[pd.DataFrame] = None,
    stage: str = "kills",
) -> pd.DataFrame:
    frame = _ensure_frame(raw)
    result = pd.DataFrame(index=frame.index)
    result["tick"] = _series_or_none(frame, ["tick", "game_tick"])
    result["attacker_steamid"] = _series_or_none(
        frame, ["attacker_steamid", "attacker_user_steamid", "killer_steamid"]
    )
    result["victim_steamid"] = _series_or_none(
        frame, ["victim_steamid", "user_steamid", "player_steamid", "userid_steamid"]
    )
    result["attacker_name"] = _series_or_none(
        frame, ["attacker_name", "attacker_user_name", "killer_name"]
    )
    result["victim_name"] = _series_or_none(frame, ["victim_name", "user_name", "player_name"])
    result["weapon"] = _series_or_none(frame, ["weapon", "weapon_name", "weapon_class"])
    result["headshot"] = _series_or_none(frame, ["headshot", "is_headshot"])
    wallbang_source = _series_or_none(frame, ["wallbang", "through_wall", "penetrated"])
    result["wallbang"] = wallbang_source
    result["through_smoke"] = _series_or_none(frame, ["through_smoke", "thrusmoke"])
    round_source = _first_existing_column(
        frame, ["round_num", "round", "roundNumber", "total_rounds_played"]
    )
    result["round_num"] = _series_or_none(
        frame, ["round_num", "round", "roundNumber", "total_rounds_played"]
    )
    if round_source == "total_rounds_played" and result["round_num"] is not None:
        result["round_num"] = _coerce_numeric_series(result["round_num"]) + 1
    result["time_sec"] = _series_or_none(frame, ["time_sec", "game_time", "event_time"])
    result["round_time_sec"] = _series_or_none(
        frame, ["round_time_sec", "player_died_time", "round_time"]
    )
    round_start_time = _series_or_none(frame, ["round_start_time"])
    if result["round_time_sec"].isna().all() and result["time_sec"] is not None and round_start_time is not None:
        result["round_time_sec"] = _coerce_numeric_series(result["time_sec"]) - _coerce_numeric_series(
            round_start_time
        )
    if players is not None and not players.empty:
        lookup = _build_player_lookup(players)
        if result["attacker_name"].isna().any():
            mapped = result["attacker_steamid"].map(lookup["steamid_to_name"])
            result["attacker_name"] = result["attacker_name"].fillna(mapped)
        if result["victim_name"].isna().any():
            mapped = result["victim_steamid"].map(lookup["steamid_to_name"])
            result["victim_name"] = result["victim_name"].fillna(mapped)
        if result["attacker_steamid"].isna().any():
            mapped = result["attacker_name"].map(lookup["name_to_steamid"])
            result["attacker_steamid"] = result["attacker_steamid"].fillna(mapped)
        if result["victim_steamid"].isna().any():
            mapped = result["victim_name"].map(lookup["name_to_steamid"])
            result["victim_steamid"] = result["victim_steamid"].fillna(mapped)
    result["headshot"] = _coerce_bool_series(result["headshot"])
    result["through_smoke"] = _coerce_bool_series(result["through_smoke"])
    result["wallbang"] = _coerce_wallbang_series(result["wallbang"])
    for column in ("tick", "attacker_steamid", "victim_steamid", "round_num"):
        result[column] = _coerce_numeric_series(result[column])
    for column in ("time_sec", "round_time_sec"):
        result[column] = _coerce_numeric_series(result[column])
    _validate_required_columns(result, KILL_REQUIRED_FIELDS, stage)
    return result


def _coerce_wallbang_series(series: pd.Series) -> pd.Series:
    def _coerce(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not pd.isna(value):
            return bool(value)
        if isinstance(value, str):
            lower = value.strip().lower()
            if lower in ("true", "1", "yes"):
                return True
            if lower in ("false", "0", "no"):
                return False
        return value

    return series.map(_coerce)


def _build_player_lookup(players: pd.DataFrame) -> Dict[str, Dict[Any, Any]]:
    steamid_to_name: Dict[Any, Any] = {}
    name_to_steamid: Dict[Any, Any] = {}
    if "steamid" in players.columns and "name" in players.columns:
        for steamid, name in zip(players["steamid"], players["name"]):
            if steamid is not None and not pd.isna(steamid) and name is not None and not pd.isna(name):
                steamid_to_name[steamid] = name
                name_to_steamid[name] = steamid
    return {"steamid_to_name": steamid_to_name, "name_to_steamid": name_to_steamid}
