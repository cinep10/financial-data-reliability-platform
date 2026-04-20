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
    volume_multiplier: float = 1.0
    conversion_multiplier: float = 1.0
    timeout_multiplier: float = 1.0
    retry_multiplier: float = 1.0
    source: str = "static"
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

    use_timeline_db: bool = False
    db_host: str = "127.0.0.1"
    db_port: int = 3306
    db_user: str = ""
    db_password: str = ""
    db_name: str = ""
    profile_id: str = ""

    def resolve_static(self) -> ExogenousState:
        return ExogenousState(
            enabled=self.enabled,
            weather_type=self.weather_type,
            campaign_flag=self.campaign_flag,
            system_flag=self.system_flag,
            volume_multiplier=1.0,
            conversion_multiplier=1.0,
            timeout_multiplier=1.0,
            retry_multiplier=1.0,
            source="static",
            as_of=None,
        )
