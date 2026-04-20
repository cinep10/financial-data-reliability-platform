# Generator exogenous hook patch guide

## 목적
`source_generator_adapter.py`의 preview JSON 수준을 넘어서,
generator가 `exogenous_state_timeline`을 직접 읽어 시간대별 외생 상태를 실제 생성 로직에 반영하도록 붙인다.

## 권장 패치 위치
현재 generator/session/page chooser/outcome 계산 앞단에서
`dt`, `hh`, `profile_id`를 알고 있는 지점에 loader를 한 번 붙인다.

## 권장 흐름

1. CLI 또는 simulator entrypoint에서 DB 파라미터를 optional로 받는다.
2. `ExogenousTimelineLoader`를 초기화한다.
3. 이벤트 생성 루프에서 현재 시각의 `dt`, `hh`를 계산한다.
4. `hour_state = loader.get_hour_state(profile_id, dt, hh)` 호출
5. 아래 항목을 생성 확률에 반영한다:
   - `volume_multiplier`
   - `conversion_multiplier`
   - `timeout_multiplier`
   - `retry_multiplier`
   - `campaign_flag`
   - `weather_type`
   - `system_flag`

## 적용 예시 pseudo code

```python
from core.exogenous.exogenous_loader import ExogenousTimelineLoader

loader = None
if args.use_exogenous_timeline:
    loader = ExogenousTimelineLoader(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        db=args.db_name,
    )

for ts in generation_loop:
    dt = ts.strftime("%Y-%m-%d")
    hh = ts.hour

    hour_state = {}
    if loader is not None:
        hour_state = loader.get_hour_state(args.profile_id, dt, hh)

    volume_multiplier = float(hour_state.get("volume_multiplier") or 1.0)
    conversion_multiplier = float(hour_state.get("conversion_multiplier") or 1.0)
    timeout_multiplier = float(hour_state.get("timeout_multiplier") or 1.0)
    retry_multiplier = float(hour_state.get("retry_multiplier") or 1.0)
    campaign_flag = hour_state.get("campaign_flag") or "none"
    weather_type = hour_state.get("weather_type") or "clear"
    system_flag = hour_state.get("system_flag") or "normal"

    # traffic volume
    session_count = int(base_session_count * volume_multiplier)

    # conversion
    final_submit_prob = base_submit_prob * conversion_multiplier

    # timeout / retry
    timeout_prob = base_timeout_prob * timeout_multiplier
    retry_prob = base_retry_prob * retry_multiplier

    # kv enrichment
    kv["campaign_flag"] = campaign_flag
    kv["weather_type"] = weather_type
    kv["system_flag"] = system_flag
```

## 구현 원칙
- helper 함수 내부에서 DB를 반복 조회하지 않는다.
- 시간 슬롯 기준으로 조회하고 상위 루프에서 전달한다.
- `exogenous_state_timeline`이 없으면 기존 baseline 로직으로 fallback 한다.
- 기존 `weather_provider`/`exogenous`가 있다면 우선순위를 정한다:
  - 권장: `timeline > provider default`

## 최소 CLI 옵션 권장
- `--use-exogenous-timeline`
- `--db-host`
- `--db-port`
- `--db-user`
- `--db-password`
- `--db-name`
- `--profile-id`
