import json
from datetime import datetime

with open("user_events.json", "r") as f:
    data = eval(f.read())  # т.к. это невалидный JSON, используем eval

# Преобразуем datetime в строки
for doc in data:
    if isinstance(doc.get("event_time"), datetime):
        doc["event_time"] = doc["event_time"].isoformat()
    if "user_info" in doc and isinstance(doc["user_info"].get("registration_date"), datetime):
        doc["user_info"]["registration_date"] = doc["user_info"]["registration_date"].isoformat()

# Сохраняем исправленный файл
with open("user_events_fixed.json", "w") as f:
    json.dump(data, f, indent=4)
