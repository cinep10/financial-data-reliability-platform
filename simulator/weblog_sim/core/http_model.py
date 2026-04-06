from __future__ import annotations

import math
import random

from .randomutil import clamp


def sample_referrer(host: str, weather: str, drift_enabled: bool) -> str:
    r = random.random()
    if r < 0.58:
        return "-"
    if r < 0.78:
        return f"https://{host}/"
    if r < 0.95:
        dom = random.choice(["google.co.kr", "google.com", "search.naver.com", "m.news.naver.com"])
        q = random.choice(["loan", "card", "deposit", "weather", weather or "bank"])
        return f"https://{dom}/search?q={q}"
    return f"https://{host}/event/landing.do"


def sample_latency_ms(event: str, page_type: str, drift_enabled: bool = False) -> int:
    base_mu = math.log(120)
    base_sigma = 0.55
    if page_type in ("radar", "forecast", "loan_apply", "card_apply", "otp", "transfer_confirm"):
        base_mu = math.log(180)
    if event in ("scroll", "swipe"):
        base_mu = math.log(90)
    if drift_enabled:
        base_mu *= 1.03
    x = random.lognormvariate(base_mu, base_sigma)
    return int(clamp(x, 10, 12000))


def sample_status(latency_ms: int) -> int:
    r = random.random()
    if latency_ms >= 6000:
        if r < 0.84:
            return 200
        if r < 0.92:
            return 429
        if r < 0.98:
            return 500
        return 504
    if latency_ms >= 3000:
        if r < 0.93:
            return 200
        if r < 0.97:
            return 429
        if r < 0.99:
            return 500
        return 404
    if r < 0.985:
        return 200
    if r < 0.993:
        return 304
    if r < 0.997:
        return 404
    if r < 0.999:
        return 500
    return 429


def sample_bytes(event: str, page_type: str) -> int:
    if event == "view":
        mu = math.log(28000 if page_type not in ("radar", "dashboard") else 42000)
        return int(clamp(random.lognormvariate(mu, 0.65), 800, 1500000))
    return int(clamp(random.lognormvariate(math.log(1200), 0.85), 120, 250000))


def event_to_path(base_path: str, event: str) -> str:
    if event == "view":
        return base_path
    joiner = "&" if "?" in base_path else "?"
    return f"{base_path}{joiner}evt={event}"
