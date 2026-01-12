from logic.schedule import get_available_slots

fake_bookings = [
    {
        "master_id": 123456789,
        "date": "2026-01-10",
        "time": "10:00",
        "status": "CONFIRMED"
    }
]

slots = get_available_slots(
    master_id=123456789,
    date_str="2026-01-10",
    service_id=1,
    bookings=fake_bookings
)

print(slots)
