from pymongo import MongoClient
from datetime import datetime, timedelta
from pprint import pprint
import json
import os

# Подключение и принт коллекции
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
events_col = db["user_events"]
archived_col = db["archived_users"]

# print("\n Все покупатели:")
# for doc in events_col.find():
#     pprint(doc)

#############################################################################

# задаем временнЫе рамки
now = datetime.now()
day_m30 = now - timedelta(days=30)
day_m14 = now - timedelta(days=14)

# групируем по user_id и добавляем уcловия что зареган <30 + посл. активность <14
pipeline = [
    {
        "$group": {
            "_id": "$user_id",
            "last_event": {"$max": "$event_time"},
            "signup_event": {"$first": "$$ROOT"}
        }
    },
    {
        "$match": {
            "last_event": {"$lt": day_m14},
            "signup_event.user_info.registration_date": {"$lt": day_m30}
        }
    }
]

inactive_users = list(events_col.aggregate(pipeline))

# парсим документы для отправки в архив
to_archive = [doc["signup_event"] for doc in inactive_users]

# отправляем юзеров в архив
if to_archive: # если такие есть, чтоб если нет код не падал
    archived_col.drop()  # немного идемпотентности
    archived_col.insert_many(to_archive)

    # отчёт
    report = {
        "date": now.strftime("%Y-%m-%d"), # текущая дата
        "archived_users_count": len(to_archive), # количество юзеров
        "archived_user_ids": [doc["user_id"] for doc in to_archive] # лист id-шников
    }

    # путь для сохранения
    output_dir = "reports"
    os.makedirs(output_dir, exist_ok=True)
    # динамический ежедневный нэйминг (например если скрипт будет в ДАГе крутиться)
    report_file = os.path.join(output_dir, f"archived_{now.strftime('%Y_%m_%d')}.json") 

    # открываем отчет в режиме write, перезаписываем если есть, делаем 4 пробела отступов для джона
    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    print(f"✅ Архивировано пользователей: {len(to_archive)}")
    print(f"📁 Отчёт сохранён в: {report_file}")
else:
    print("ℹ️ Нет пользователей для архивации сегодня.")
