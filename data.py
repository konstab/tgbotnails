import json
import os
from typing import Any, Dict

BASE_DIR = os.path.dirname(__file__)

def _abs(name: str) -> str:
    return os.path.join(BASE_DIR, name)

BOOKINGS_FILE = _abs("bookings.json")
BLOCKED_FILE = _abs("blocked_slots.json")

MASTERS_CUSTOM_FILE = _abs("masters_custom.json")
MASTER_OVERRIDES_FILE = _abs("master_overrides.json")

SERVICE_OVERRIDES_FILE = _abs("service_overrides.json")
SERVICES_CUSTOM_FILE = _abs("services_custom.json")

SETTINGS_FILE = _abs("admin_settings.json")


def _load_json(path: str, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, data):
    # атомарная запись
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ----------------------------
# ДАННЫЕ
# ----------------------------

admin_settings = _load_json(SETTINGS_FILE, default={})

masters_custom: Dict[str, dict] = _load_json(MASTERS_CUSTOM_FILE, default={})
master_overrides: Dict[str, dict] = _load_json(MASTER_OVERRIDES_FILE, default={})

bookings = _load_json(BOOKINGS_FILE, default=[])
blocked_slots: Dict[str, list] = _load_json(BLOCKED_FILE, default={})

service_overrides: Dict[str, dict] = _load_json(SERVICE_OVERRIDES_FILE, default={})
services_custom: Dict[str, list] = _load_json(SERVICES_CUSTOM_FILE, default={})


# ----------------------------
# СХЕМА МАСТЕРА (чтобы всё было в одном месте)
# ----------------------------
def ensure_master_schema(master_id: int | str) -> dict:
    """
    Гарантирует, что masters_custom[str(mid)] имеет нужные ключи:
    name, enabled, about, contacts, schedule.
    Ничего не ломает, если файл старого формата.
    """
    mid = str(master_id)
    m = masters_custom.get(mid)
    if not isinstance(m, dict):
        m = {}

    # базовые поля
    if "name" not in m:
        m["name"] = mid
    if "enabled" not in m:
        m["enabled"] = True

    # описание/контакты
    if "about" not in m:
        m["about"] = ""
    if "contacts" not in m or not isinstance(m.get("contacts"), dict):
        m["contacts"] = {
            "phone": "",
            "instagram": "",
            "address": "",
            "telegram": ""
        }

    # график
    # schedule: {"days":[0..6], "start":"10:00", "end":"18:00", "daily_limit": 6}
    if "schedule" not in m or not isinstance(m.get("schedule"), dict):
        m["schedule"] = {}

    sch = m["schedule"]
    if "days" not in sch:
        sch["days"] = []  # пусто = мастер "без графика" (слотов нет)
    if "start" not in sch:
        sch["start"] = ""
    if "end" not in sch:
        sch["end"] = ""
    if "daily_limit" not in sch:
        sch["daily_limit"] = 0  # 0 = без лимита

    masters_custom[mid] = m
    return m


# ----------------------------
# SAVE
# ----------------------------
def save_admin_settings():
    _save_json(SETTINGS_FILE, admin_settings)

def save_masters_custom():
    _save_json(MASTERS_CUSTOM_FILE, masters_custom)

def save_master_overrides():
    _save_json(MASTER_OVERRIDES_FILE, master_overrides)

def save_bookings():
    _save_json(BOOKINGS_FILE, bookings)

def save_blocked():
    _save_json(BLOCKED_FILE, blocked_slots)

def save_service_overrides():
    _save_json(SERVICE_OVERRIDES_FILE, service_overrides)

def save_services_custom():
    _save_json(SERVICES_CUSTOM_FILE, services_custom)

