from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import pandas as pd

from ..errors import EntityNotFoundError, ParseFailedError, UnsupportedPatchError
from .demoparser2_backend import Demoparser2Backend


@dataclass
class _CsdaCache:
    demo_info: Dict[str, Any]
    positions: pd.DataFrame
    shots: pd.DataFrame
    kills: pd.DataFrame
    rounds: pd.DataFrame


class CsDemoAnalyzerBackend(object):
    name = "cs-demo-analyzer"
    version = "1.9.5"

    def __init__(self, executable_path: Optional[str] = None) -> None:
        self._legacy = Demoparser2Backend()
        self._workspace_root = Path(__file__).resolve().parents[2]
        self._scratch_root = self._workspace_root / ".demolens_csda"
        self._scratch_root.mkdir(parents=True, exist_ok=True)
        self._executable_path = self._resolve_executable(executable_path)
        self._cache: Dict[str, _CsdaCache] = {}

    def parse_header(self, demo_path: str) -> Dict[str, Any]:
        return self._legacy.parse_header(demo_path)

    def parse_players(self, demo_path: str) -> pd.DataFrame:
        legacy = self._try_legacy_parse(lambda: self._legacy.parse_players(demo_path))
        if legacy is not None and not legacy.empty:
            return legacy

        cache = self._ensure_csda_cache(demo_path, stage="player_list")
        if cache.positions.empty:
            raise ParseFailedError(
                "player_list",
                "cs-demo-analyzer player list is empty",
                {"demo_path": demo_path},
            )

        frame = cache.positions[["steamid", "name", "team_number"]].copy()
        frame = frame.dropna(subset=["steamid", "name"])
        frame = frame.drop_duplicates(subset=["steamid"], keep="first")
        return frame.reset_index(drop=True)

    def parse_ticks(
        self,
        demo_path: str,
        players: Optional[Sequence[int]] = None,
        ticks: Optional[Sequence[int]] = None,
    ) -> pd.DataFrame:
        legacy = self._try_legacy_parse(
            lambda: self._legacy.parse_ticks(demo_path, players=players, ticks=ticks)
        )
        if legacy is not None and not legacy.empty:
            return legacy

        cache = self._ensure_csda_cache(demo_path, stage="ticks")
        frame = cache.positions.copy()
        if players is not None:
            player_ids = self._normalize_int_set(players)
            if player_ids:
                frame = frame[frame["steamid"].isin(player_ids)]
        if ticks is not None:
            tick_ids = self._normalize_int_set(ticks)
            if tick_ids:
                frame = frame[frame["tick"].isin(tick_ids)]

        if frame.empty:
            raise ParseFailedError(
                "ticks",
                "tick stream is empty after cs-demo-analyzer filtering",
                {"demo_path": demo_path},
            )

        frame = self._attach_pitch(frame, cache.shots)
        result = pd.DataFrame(
            {
                "tick": frame["tick"],
                "steamid": frame["steamid"],
                "X": frame["X"],
                "Y": frame["Y"],
                "Z": frame["Z"],
                "pitch": frame["pitch"],
                "yaw": frame["yaw"],
                "is_alive": frame["is_alive"],
                "active_weapon": frame["active_weapon"],
                "health": frame["health"],
            }
        )
        return result.reset_index(drop=True)

    def parse_kills(self, demo_path: str) -> pd.DataFrame:
        legacy = self._try_legacy_parse(lambda: self._legacy.parse_kills(demo_path))
        if legacy is not None:
            return legacy

        cache = self._ensure_csda_cache(demo_path, stage="kills")
        if cache.kills.empty:
            return cache.kills.copy()

        frame = cache.kills.copy()
        round_start = self._round_start_lookup(cache.rounds)
        tickrate = float(cache.demo_info.get("tickrate") or 64.0)
        frame["round_num"] = pd.to_numeric(frame["round_num"], errors="coerce")
        frame["tick"] = pd.to_numeric(frame["tick"], errors="coerce")
        frame["time_sec"] = (frame["tick"] / tickrate).round(6)
        frame["round_time_sec"] = frame.apply(
            lambda row: round(
                max(
                    0.0,
                    (float(row["tick"]) - float(round_start.get(int(row["round_num"]), 0)))
                    / tickrate,
                ),
                6,
            )
            if pd.notna(row["tick"]) and pd.notna(row["round_num"])
            else None,
            axis=1,
        )
        result = frame[
            [
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
        ].copy()
        return result.reset_index(drop=True)

    def _try_legacy_parse(self, fn):
        try:
            frame = fn()
        except Exception:
            return None
        if frame is None:
            return None
        if isinstance(frame, pd.DataFrame) and frame.empty:
            return frame
        return frame

    def _ensure_csda_cache(self, demo_path: str, stage: str) -> _CsdaCache:
        cache_key = str(Path(demo_path).resolve())
        if cache_key in self._cache:
            return self._cache[cache_key]
        if self._executable_path is None:
            raise ParseFailedError(
                stage,
                "cs-demo-analyzer executable not found",
                {"demo_path": demo_path, "workspace_root": str(self._workspace_root)},
            )

        output_dir = Path(
            tempfile.mkdtemp(prefix="demolens_csda_", dir=str(self._scratch_root))
        )
        cmd = [
            str(self._executable_path),
            f"-demo-path={demo_path}",
            f"-output={str(output_dir)}",
            "-format=csdm",
            "-positions",
        ]
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.returncode != 0:
            shutil.rmtree(output_dir, ignore_errors=True)
            self._raise_csda_failure(stage, demo_path, cmd, completed)

        try:
            stem = Path(demo_path).stem
            demo_info = self._load_demo_info(self._require_file(output_dir, f"{stem}_demo.csv", stage))
            positions = self._load_positions(self._require_file(output_dir, f"{stem}_positions.csv", stage))
            shots = self._load_shots(self._require_file(output_dir, f"{stem}_shots.csv", stage))
            kills = self._load_kills(self._require_file(output_dir, f"{stem}_kills.csv", stage))
            rounds = self._load_rounds(self._require_file(output_dir, f"{stem}_rounds.csv", stage))
        except Exception:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise

        shutil.rmtree(output_dir, ignore_errors=True)
        cache = _CsdaCache(
            demo_info=demo_info,
            positions=positions,
            shots=shots,
            kills=kills,
            rounds=rounds,
        )
        self._cache[cache_key] = cache
        return cache

    def _raise_csda_failure(self, stage: str, demo_path: str, cmd: Sequence[str], completed) -> None:
        message = "\n".join(
            [part for part in (completed.stdout.strip(), completed.stderr.strip()) if part]
        ).strip()
        lowered = message.lower()
        details = {
            "demo_path": demo_path,
            "command": list(cmd),
            "exit_code": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        if "unsupported patch" in lowered:
            raise UnsupportedPatchError(
                stage,
                message or "cs-demo-analyzer reported unsupported patch",
                details,
            )
        if "entitynotfound" in lowered or "entity not found" in lowered:
            raise EntityNotFoundError(
                stage,
                message or "cs-demo-analyzer reported entity not found",
                details,
            )
        raise ParseFailedError(
            stage,
            message or "cs-demo-analyzer failed",
            details,
        )

    def _resolve_executable(self, explicit: Optional[str]) -> Optional[Path]:
        candidates = []
        if explicit:
            candidates.append(Path(explicit))
        env_path = os.environ.get("DEMOLENS_CSDA_PATH")
        if env_path:
            candidates.append(Path(env_path))
        candidates.extend(
            [
                self._workspace_root / "temp_csda_pkg" / "package" / "dist" / "bin" / "windows-x64" / "csda.exe",
                self._workspace_root / "node_modules" / ".bin" / "csda.cmd",
                self._workspace_root / "node_modules" / "@akiver" / "cs-demo-analyzer" / "dist" / "bin" / "windows-x64" / "csda.exe",
            ]
        )
        for candidate in candidates:
            if candidate and candidate.exists():
                return candidate
        return None

    def _require_file(self, output_dir: Path, filename: str, stage: str) -> Path:
        path = output_dir / filename
        if not path.exists():
            raise ParseFailedError(
                stage,
                f"missing cs-demo-analyzer output file: {filename}",
                {"output_dir": str(output_dir)},
            )
        return path

    def _load_demo_info(self, path: Path) -> Dict[str, Any]:
        names = [
            "checksum",
            "game",
            "demo_file_name",
            "date",
            "source",
            "type",
            "share_code",
            "map_name",
            "server_name",
            "client_name",
            "tick_count",
            "tickrate",
            "framerate",
            "duration",
            "network_protocol",
            "build_number",
        ]
        row = pd.read_csv(path, header=None, names=names, encoding="utf-8").iloc[0].to_dict()
        return {
            "checksum": row.get("checksum"),
            "tickrate": float(row.get("tickrate") or 64.0),
            "map_name": row.get("map_name"),
            "server_name": row.get("server_name"),
            "network_protocol": row.get("network_protocol"),
        }

    def _load_positions(self, path: Path) -> pd.DataFrame:
        # cs-demo-analyzer emits headerless CSVs in a fixed column order.
        # We read only the fields needed for downstream normalization.
        usecols = [1, 2, 3, 4, 5, 6, 10, 21, 28, 29, 30]
        names = [
            "tick",
            "is_alive",
            "X",
            "Y",
            "Z",
            "yaw",
            "health",
            "active_weapon",
            "steamid",
            "name",
            "team_number",
        ]
        frame = pd.read_csv(path, header=None, usecols=usecols, names=names, encoding="utf-8")
        for column in ("tick", "steamid", "team_number", "health", "X", "Y", "Z", "yaw"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["is_alive"] = pd.to_numeric(frame["is_alive"], errors="coerce").fillna(0) > 0
        return frame

    def _load_shots(self, path: Path) -> pd.DataFrame:
        usecols = [1, 10, 18]
        names = ["tick", "steamid", "pitch"]
        frame = pd.read_csv(path, header=None, usecols=usecols, names=names, encoding="utf-8")
        for column in ("tick", "steamid", "pitch"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        return frame

    def _load_kills(self, path: Path) -> pd.DataFrame:
        # The export is headerless. These indices match the current cs-demo-analyzer
        # schema for the fields we need.
        usecols = [1, 2, 3, 4, 7, 8, 15, 17, 18, 39]
        names = [
            "tick",
            "round_num",
            "attacker_name",
            "attacker_steamid",
            "victim_name",
            "victim_steamid",
            "weapon",
            "headshot",
            "penetrated_objects",
            "through_smoke",
        ]
        frame = pd.read_csv(path, header=None, usecols=usecols, names=names, encoding="utf-8")
        frame["wallbang"] = pd.to_numeric(frame["penetrated_objects"], errors="coerce").fillna(0) > 0
        frame["headshot"] = pd.to_numeric(frame["headshot"], errors="coerce").fillna(0) > 0
        frame["through_smoke"] = pd.to_numeric(frame["through_smoke"], errors="coerce").fillna(0) > 0
        for column in ("tick", "round_num", "attacker_steamid", "victim_steamid"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        return frame

    def _load_rounds(self, path: Path) -> pd.DataFrame:
        frame = pd.read_csv(
            path,
            header=None,
            usecols=[0, 1],
            names=["number", "start_tick"],
            encoding="utf-8",
        )
        frame["number"] = pd.to_numeric(frame["number"], errors="coerce")
        frame["start_tick"] = pd.to_numeric(frame["start_tick"], errors="coerce")
        return frame

    @staticmethod
    def _attach_pitch(positions: pd.DataFrame, shots: pd.DataFrame) -> pd.DataFrame:
        if shots.empty:
            frame = positions.copy()
            frame["pitch"] = 0.0
            return frame
        # merge_asof requires the join key to be globally sorted.
        left = positions.sort_values(["tick", "steamid"]).copy()
        right = shots.sort_values(["tick", "steamid"]).copy()
        right = right.drop_duplicates(subset=["steamid", "tick"], keep="last")
        merged = pd.merge_asof(
            left,
            right[["steamid", "tick", "pitch"]],
            on="tick",
            by="steamid",
            direction="backward",
            allow_exact_matches=True,
        )
        merged["pitch"] = merged.groupby("steamid")["pitch"].transform(lambda series: series.ffill().bfill())
        merged["pitch"] = merged["pitch"].fillna(0.0)
        return merged

    @staticmethod
    def _round_start_lookup(rounds: pd.DataFrame) -> Dict[int, int]:
        lookup: Dict[int, int] = {}
        if rounds.empty:
            return lookup
        for number, start_tick in zip(rounds["number"], rounds["start_tick"]):
            if pd.isna(number) or pd.isna(start_tick):
                continue
            lookup[int(number)] = int(start_tick)
        return lookup

    @staticmethod
    def _normalize_int_set(values: Sequence[int]) -> set:
        result = set()
        for value in values:
            try:
                number = int(value)
            except Exception:
                continue
            result.add(number)
        return result
