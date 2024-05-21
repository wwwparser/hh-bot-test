import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command
from pymongo import MongoClient
import json
from datetime import datetime, timedelta

# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(level=logging.INFO)

API_TOKEN = '6218806811:AAEx4fG_0rvcfHcyNkyCoNUxGsqMx9fxnZ4'

bot = Bot(token=API_TOKEN)
# Диспетчер
dp = Dispatcher()

# Хэндлер на команду /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Hello! Send data in the format: dt_from, dt_to, group_type")

async def aggregate_data(dt_from, dt_to, group_type):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sampleDB"]
    collection = db["sample_collection"]

    dt_from = datetime.fromisoformat(dt_from)
    dt_to = datetime.fromisoformat(dt_to)

    match = {
        "$match": {
            "dt": {"$gte": dt_from, "$lte": dt_to}
        }
    }

    sort = {
        "$sort": {
            "dt": 1
        }
    }

    if group_type == "month":
        group = {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-01T00:00:00", "date": "$dt"}},
                "total": {"$sum": "$value"}
            }
        }
        sort = {"$sort": {"_id": 1}}
    elif group_type == "day":
        group = {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%dT00:00:00", "date": "$dt"}},
                "total": {"$sum": "$value"}
            }
        }
        sort = {"$sort": {"_id": 1}}
    elif group_type == "hour":
        group = {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%dT%H:00:00", "date": "$dt"}},
                "total": {"$sum": "$value"}
            }
        }
        sort = {"$sort": {"_id": 1}}

    pipeline = [match, sort, group, sort]

    result = collection.aggregate(pipeline)

    if group_type == "day":
        ataset = []
        labels = []

        # Generate labels based on dt_from and dt_to
        delta = dt_to - dt_from
        for i in range((delta.days + 1)):
            date = dt_from + timedelta(days=i)
            labels.append(date.strftime("%Y-%m-%dT%H:00:00"))

        # Fill dataset with zeros
        dataset = [0] * len(labels)

        for doc in result:
            label_index = labels.index(doc["_id"])
            dataset[label_index] = doc["total"]

        return {"dataset": dataset, "labels": labels}

    if group_type == "month":
        dataset = []
        labels = []

        for doc in result:
            dataset.append(doc["total"])
            labels.append(doc["_id"])

        return {"dataset": dataset, "labels": labels}

    if group_type == "hour":
        dataset = []
        labels = []

        # Generate labels based on dt_from and dt_to
        delta = dt_to - dt_from
        for i in range(25):  # Assuming dt_from and dt_to are datetime objects
            date = dt_from + timedelta(hours=i)
            labels.append(date.strftime("%Y-%m-%dT%H:00:00"))

        # Fill dataset with zeros
        dataset = [0] * len(labels)

        for doc in result:
            label_index = labels.index(doc["_id"])
            dataset[label_index] = doc["total"]

        return {"dataset": dataset, "labels": labels}




# Хэндлер на текстовые сообщения
@dp.message()
async def handle_data(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data["dt_from"]
        dt_to = data["dt_upto"]
        group_type = data["group_type"]
        result = await aggregate_data(dt_from, dt_to, group_type)
        await message.answer(json.dumps(result))
    except Exception as e:
        await message.answer(f"Error: {str(e)}")

# Запуск процесса поллинга новых апдейтов
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())