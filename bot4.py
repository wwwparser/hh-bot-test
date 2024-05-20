import asyncio
import json
from datetime import datetime, timedelta
from pymongo import MongoClient

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

    # with open('match.json', 'w') as f:
    #     json.dump(match, f, indent=4)

    if group_type == "month":
        group = {
            "$group": {
                "_id": {"month": {"$month": "$dt"}},
                "total": {"$sum": "$value"}
            }
        }
    elif group_type == "day":
        group = {
            "$group": {
                "_id": {"day": {"$dayOfMonth": "$dt"}},
                "total": {"$sum": "$value"}
            }
        }
    elif group_type == "hour":
        group = {
            "$group": {
                "_id": {"hour": {"$hour": "$dt"}},
                "total": {"$sum": "$value"}
            }
        }

    # with open('group.json', 'w') as f:
    #     json.dump(group, f, indent=4)

    pipeline = [match, group]

    result = collection.aggregate(pipeline)

    dataset = []
    labels = []

    for doc in result:
        dataset.append(doc["total"])
        labels.append(doc["_id"])

    return {"dataset": dataset, "labels": labels}

async def main():
    data1 = {
        "dt_from": "2022-09-01T00:00:00",
        "dt_to": "2022-12-31T23:59:00",
        "group_type": "month"
    }

    result1 = await aggregate_data(**data1)
    print(result1)

    data2 = {
        "dt_from": "2022-10-01T00:00:00",
        "dt_to": "2022-11-30T23:59:00",
        "group_type": "day"
    }

    result2 = await aggregate_data(**data2)
    print(result2)

    data3 = {
        "dt_from": "2022-02-01T00:00:00",
        "dt_to": "2022-02-02T00:00:00",
        "group_type": "hour"
    }

    result3 = await aggregate_data(**data3)
    print(result3)

if __name__ == '__main__':
    asyncio.run(main())