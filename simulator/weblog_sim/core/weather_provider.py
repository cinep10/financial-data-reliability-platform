from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pymysql

from .exogenous import ExogenousConfig, ExogenousState


class WeatherProvider(ABC):
    @abstractmethod
    def get_state(self, when: datetime) -> ExogenousState:
        raise NotImplementedError


class StaticWeatherProvider(WeatherProvider):
    def __init__(self, cfg: ExogenousConfig):
        self.cfg = cfg

    def get_state(self, when: datetime) -> ExogenousState:
        st = self.cfg.resolve_static()
        st.as_of = when
        return st


class FileWeatherProvider(WeatherProvider):
    def __init__(self, cfg: ExogenousConfig):
        self.cfg = cfg
        self.payload: Dict[str, Any] = {}
        if cfg.weather_file:
            p = Path(cfg.weather_file)
            if p.exists():
                self.payload = json.loads(p.read_text(encoding="utf-8"))

    def get_state(self, when: datetime) -> ExogenousState:
        dt_key = when.strftime("%Y-%m-%d")
        default = self.payload.get("default", {})
        by_date = self.payload.get("by_date", {})
        row = by_date.get(dt_key, {})
        return ExogenousState(
            enabled=self.cfg.enabled,
            weather_type=str(row.get("weather_type", default.get("weather_type", self.cfg.weather_type))),
            campaign_flag=str(row.get("campaign_flag", default.get("campaign_flag", self.cfg.campaign_flag))),
            system_flag=str(row.get("system_flag", default.get("system_flag", self.cfg.system_flag))),
            volume_multiplier=float(row.get("volume_multiplier", default.get("volume_multiplier", 1.0))),
            conversion_multiplier=float(row.get("conversion_multiplier", default.get("conversion_multiplier", 1.0))),
            timeout_multiplier=float(row.get("timeout_multiplier", default.get("timeout_multiplier", 1.0))),
            retry_multiplier=float(row.get("retry_multiplier", default.get("retry_multiplier", 1.0))),
            source="file",
            as_of=when,
        )


class ApiWeatherProvider(WeatherProvider):
    def __init__(self, cfg: ExogenousConfig):
        self.cfg = cfg

    def get_state(self, when: datetime) -> ExogenousState:
        return ExogenousState(
            enabled=self.cfg.enabled,
            weather_type=self.cfg.weather_type,
            campaign_flag=self.cfg.campaign_flag,
            system_flag=self.cfg.system_flag,
            volume_multiplier=1.0,
            conversion_multiplier=1.0,
            timeout_multiplier=1.0,
            retry_multiplier=1.0,
            source="api",
            as_of=when,
        )


class TimelineDbProvider(WeatherProvider):
    def __init__(self, cfg: ExogenousConfig):
        self.cfg = cfg
        self._conn = pymysql.connect(
            host=cfg.db_host,
            port=int(cfg.db_port),
            user=cfg.db_user,
            password=cfg.db_password,
            database=cfg.db_name,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def get_state(self, when: datetime) -> ExogenousState:
        dt = when.strftime("%Y-%m-%d")
        hh = int(when.hour)
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT weather_type, campaign_flag, system_flag,
                       volume_multiplier, conversion_multiplier,
                       timeout_multiplier, retry_multiplier
                FROM exogenous_state_timeline
                WHERE profile_id=%s AND dt=%s AND hh=%s
                """,
                (self.cfg.profile_id, dt, hh),
            )
            row = cur.fetchone()

        if not row:
            return ExogenousState(enabled=False, source="timeline_db", as_of=when)

        return ExogenousState(
            enabled=True,
            weather_type=str(row.get("weather_type") or "clear"),
            campaign_flag=str(row.get("campaign_flag") or "none"),
            system_flag=str(row.get("system_flag") or "normal"),
            volume_multiplier=float(row.get("volume_multiplier") or 1.0),
            conversion_multiplier=float(row.get("conversion_multiplier") or 1.0),
            timeout_multiplier=float(row.get("timeout_multiplier") or 1.0),
            retry_multiplier=float(row.get("retry_multiplier") or 1.0),
            source="timeline_db",
            as_of=when,
        )


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _cfg_from_env(cfg: ExogenousConfig) -> ExogenousConfig:
    exo = ExogenousConfig(**cfg.__dict__)
    if _bool_env("EXO_TIMELINE_ENABLED", False):
        exo.enabled = True
        exo.use_timeline_db = True
        exo.weather_source = "timeline_db"
        exo.db_host = os.getenv("EXO_DB_HOST", exo.db_host or "127.0.0.1")
        exo.db_port = int(os.getenv("EXO_DB_PORT", str(exo.db_port or 3306)))
        exo.db_user = os.getenv("EXO_DB_USER", exo.db_user or "")
        exo.db_password = os.getenv("EXO_DB_PASSWORD", exo.db_password or "")
        exo.db_name = os.getenv("EXO_DB_NAME", exo.db_name or "")
        exo.profile_id = os.getenv("EXO_PROFILE_ID", exo.profile_id or "")
    return exo


def build_weather_provider(cfg: Optional[ExogenousConfig]) -> WeatherProvider:
    if cfg is None:
        cfg = ExogenousConfig(enabled=False)

    cfg = _cfg_from_env(cfg)
    source = (cfg.weather_source or "static").lower()

    if source == "timeline_db" or getattr(cfg, "use_timeline_db", False):
        required = [cfg.db_host, cfg.db_user, cfg.db_name, cfg.profile_id]
        if all(required):
            return TimelineDbProvider(cfg)

    if source == "file":
        return FileWeatherProvider(cfg)
    if source == "api":
        return ApiWeatherProvider(cfg)
    return StaticWeatherProvider(cfg)
