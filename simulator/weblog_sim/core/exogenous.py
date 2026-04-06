from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ExogenousState:
    enabled: bool = False
    weather_type: str = "clear"
    campaign_flag: str = "none"
    system_flag: str = "normal"
    as_of: Optional[datetime] = None


@dataclass
class ExogenousConfig:
    enabled: bool = False
    weather_type: str = "clear"
    campaign_flag: str = "none"
    system_flag: str = "normal"
    weather_source: str = "static"
    weather_file: str = ""
    weather_api_base_url: str = ""
    weather_api_key: str = ""

    def resolve_static(self) -> ExogenousState:
        return ExogenousState(
            enabled=self.enabled,
            weather_type=self.weather_type,
            campaign_flag=self.campaign_flag,
            system_flag=self.system_flag,
            as_of=None,
        )
