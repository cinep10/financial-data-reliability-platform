from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

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
            as_of=when,
        )


def build_weather_provider(cfg: Optional[ExogenousConfig]) -> WeatherProvider:
    if cfg is None:
        return StaticWeatherProvider(ExogenousConfig(enabled=False))
    source = (cfg.weather_source or "static").lower()
    if source == "file":
        return FileWeatherProvider(cfg)
    if source == "api":
        return ApiWeatherProvider(cfg)
    return StaticWeatherProvider(cfg)
