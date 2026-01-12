from __future__ import annotations

from datetime import datetime, timedelta, time
from typing import Optional

from config import DATE_FORMAT, TIME_FORMAT, TIME_STEP, PAUSE_MINUTES
import data


def _parse_hhmm(s: str) -> Optional[time]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.strptime(s.strip(), "%H:%M").time()
    except Exception:
        return None


def _get_master_schedule(master_id: int) -> Optional[dict]:
    ov = data.master_overrides.get(str(master_id), {})
    m = data.masters_custom.get(str(master_id))
    if not isinstance(m, dict):
        return None

    # enabled: приоритет overrides, затем masters_custom
    enabled = True
    if isinstance(ov, dict) and "enabled" in ov:
        enabled = bool(ov.get("enabled"))
    elif "enabled" in m:
        enabled = bool(m.get("enabled"))

    if not enabled:
        return None

    sch = m.get("schedule")
    if not isinstance(sch, dict):
        return None

    days = sch.get("days")
    st = _parse_hhmm(sch.get("start"))
    en = _parse_hhmm(sch.get("end"))

    if not isinstance(days, list) or st is None or en is None:
        return None

    try:
        daily_limit = int(sch.get("daily_limit") or 0)
    except Exception:
        daily_limit = 0

    return {"days": days, "start": st, "end": en, "daily_limit": daily_limit}


def ceil_to_step(dt: datetime, step_minutes: int) -> datetime:
    m = dt.minute
    r = m % step_minutes
    if r == 0:
        return dt.replace(second=0, microsecond=0)
    add = step_minutes - r
    return (dt + timedelta(minutes=add)).replace(second=0, microsecond=0)


def get_available_slots(master_id: int, date_str: str, service_id: int) -> list[str]:
    # 1) услуга (base + custom)
    base_services = data.masters_custom.get(str(master_id), {}).get("services", [])
    custom_services = data.services_custom.get(str(master_id), [])
    all_services = list(base_services) + list(custom_services)

    base_service = next((s for s in all_services if s.get("id") == service_id), None)
    if not base_service:
        return []

    # 2) overrides (длительность + включенность)
    ov = data.service_overrides.get(str(master_id), {}).get(str(service_id), {})
    if ov.get("enabled") is False:
        return []

    try:
        duration = int(ov.get("duration", base_service.get("duration", 0)) or 0)
    except Exception:
        duration = 0

    if duration <= 0:
        return []

    # 3) график мастера (из masters_custom.json)
    work = _get_master_schedule(master_id)
    if not work:
        return []

    # 3.1) день недели должен входить в days
    try:
        d = datetime.strptime(date_str, DATE_FORMAT).date()
    except Exception:
        return []
    weekday = d.weekday()  # 0=Пн ... 6=Вс
    if weekday not in work["days"]:
        return []

    # 3.2) daily_limit (если задан)
    daily_limit = int(work.get("daily_limit") or 0)
    if daily_limit > 0:
        day_count = sum(
            1 for b in data.bookings
            if b.get("master_id") == master_id
            and b.get("date") == date_str
            and b.get("status") in ("PENDING", "CONFIRMED")
            and b.get("time")
        )
        if day_count >= daily_limit:
            return []

    # 4) границы дня
    start_dt = datetime.strptime(date_str, DATE_FORMAT).replace(
        hour=work["start"].hour,
        minute=work["start"].minute,
        second=0,
        microsecond=0
    )
    end_dt = datetime.strptime(date_str, DATE_FORMAT).replace(
        hour=work["end"].hour,
        minute=work["end"].minute,
        second=0,
        microsecond=0
    )

    start_dt = ceil_to_step(start_dt, TIME_STEP)

    # 5) потенциальные слоты
    slots: list[str] = []
    cur = start_dt
    while cur + timedelta(minutes=duration) <= end_dt:
        slots.append(cur.strftime(TIME_FORMAT))
        cur += timedelta(minutes=TIME_STEP)

    # 6) занятые интервалы (PENDING + CONFIRMED)
    booked_slots = [
        (b["time"], int(b.get("service_duration", duration) or duration))
        for b in data.bookings
        if b.get("master_id") == master_id
        and b.get("date") == date_str
        and b.get("status") in ("PENDING", "CONFIRMED")
        and b.get("time")
    ]

    # 7) блокировки
    master_blocked = data.blocked_slots.get(str(master_id), [])
    if any(b.get("date") == date_str and b.get("time") is None for b in master_blocked):
        return []

    blocked_times = [
        b.get("time") for b in master_blocked
        if b.get("date") == date_str and b.get("time")
    ]

    # 8) фильтр по пересечениям
    available: list[str] = []
    now = datetime.now()

    for slot in slots:
        slot_start = datetime.strptime(f"{date_str} {slot}", f"{DATE_FORMAT} {TIME_FORMAT}")
        if slot_start <= now:
            continue

        slot_end = slot_start + timedelta(minutes=duration)
        if PAUSE_MINUTES and PAUSE_MINUTES > 0:
            slot_end = slot_end + timedelta(minutes=PAUSE_MINUTES)

        conflict = False

        # пересечение с бронями
        for b_time, b_dur in booked_slots:
            b_start = datetime.strptime(f"{date_str} {b_time}", f"{DATE_FORMAT} {TIME_FORMAT}")
            b_end = b_start + timedelta(minutes=b_dur)
            if PAUSE_MINUTES and PAUSE_MINUTES > 0:
                b_end = b_end + timedelta(minutes=PAUSE_MINUTES)

            if slot_start < b_end and slot_end > b_start:
                conflict = True
                break

        # точечная блокировка
        if slot in blocked_times:
            conflict = True

        if not conflict:
            available.append(slot)

    return available
