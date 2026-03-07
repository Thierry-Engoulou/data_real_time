import os
import math
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import certifi

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["meteo_douala"]
col = db["donnees_meteo"]

docs = col.find({})
ops = []
count_fixed = 0

for doc in docs:
    updates = {}
    for k, v in doc.items():
        if isinstance(v, float) and math.isnan(v):
            updates[k] = None
    
    if updates:
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": updates}))
        count_fixed += 1

if ops:
    # process in batches of 1000
    batch_size = 1000
    for i in range(0, len(ops), batch_size):
        col.bulk_write(ops[i:i+batch_size])

print(f"Fixed {count_fixed} documents containing NaN values.")
