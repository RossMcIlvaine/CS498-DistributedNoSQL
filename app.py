from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from pydantic import BaseModel

URI = "mongodb+srv://rossmcilvaine_db_user:kva5KCF4Fcfvi5WO@ev-cluster.rd0irza.mongodb.net/?appName=ev-cluster"

client     = MongoClient(URI)
collection = client["ev_db"]["vehicles"]

app = FastAPI()

class EVRecord(BaseModel):
    class Config:
        extra = "allow"   # accept any fields the caller sends

# Fast but Unsafe Write
@app.post("/insert-fast")
def insert_fast(record: EVRecord):
    col = collection.with_options(write_concern=WriteConcern(w=1))
    result = col.insert_one(record.model_dump())
    return {"inserted_id": str(result.inserted_id)}

# Highly Durable Write
@app.post("/insert-safe")
def insert_safe(record: EVRecord):
    col = collection.with_options(write_concern=WriteConcern("majority"))
    result = col.insert_one(record.model_dump())
    return {"inserted_id": str(result.inserted_id)}

# Strongly Consistent Read
@app.get("/count-tesla-primary")
def count_tesla_primary():
    col   = collection.with_options(read_preference=ReadPreference.PRIMARY)
    count = col.count_documents({"Make": "TESLA"})
    return {"count": count}

# Eventually Consistent Analytical Read
@app.get("/count-bmw-secondary")
def count_bmw_secondary():
    col   = collection.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)
    count = col.count_documents({"Make": "BMW"})
    return {"count": count}
