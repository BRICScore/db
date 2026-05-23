from pymongo import AsyncMongoClient, MongoClient
import os
from celery.signals import worker_process_init, worker_process_shutdown


async def connect_to_db():
    MONGO_URI = os.getenv("MONGO_URI")
    client = AsyncMongoClient(MONGO_URI, maxPoolSize=10)
    if client:
        return client
    else:
        raise Exception("Failed to connect to DB")

db_worker_client = None

@worker_process_init.connect
def init_worker(**kwargs):
    MONGO_URI=os.getenv("MONGO_URI")
    global db_worker_client
    db_worker_client = MongoClient(MONGO_URI)

@worker_process_shutdown.connect
def close_worker(**kwargs):
    global db_worker_client
    if db_worker_client:
        db_worker_client.close()

def get_db_for_worker():
    return db_worker_client["brics"]


