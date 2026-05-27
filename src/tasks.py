import subprocess
from .celery_app import worker_app
from pathlib import Path
from .db import get_db_for_worker
import os
from .models import MeasurementMetadata, ModelMetadata
import zipfile
import tempfile
from .utils import jsonl_to_bson, zip_directory
import shutil
from bson import ObjectId
import datetime
import json

MAX_UNCOMPRESSED_SIZE = 1500 * 1024 * 1024

@worker_app.task
def handle_measurement_upload(tmp_file: str, metadata_dict: str):
    db = get_db_for_worker()
    coll = db.get_collection("measurements")
    FILEPATH = os.getenv("FILE_PATH")

    try:
        metadata = MeasurementMetadata(**json.loads(metadata_dict))
        tmp_dir = Path(tempfile.mkdtemp())
        with zipfile.ZipFile(tmp_file, "r") as zf:
            total = sum(info.file_size for info in zf.infolist())
            if total > MAX_UNCOMPRESSED_SIZE:
                raise Exception()
            zf.extractall(tmp_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        Path(tmp_file).unlink(missing_ok=True)
        raise

    try:
        jsonl_to_bson(tmp_dir / "raw", tmp_dir / "raw_new")
        jsonl_to_bson(tmp_dir / "clean", tmp_dir / "clean_new")
        jsonl_to_bson(tmp_dir / "features", tmp_dir / "features_new")

    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        Path(tmp_file).unlink(missing_ok=True)
        raise

    try:
        os.replace(tmp_dir / "raw_new", metadata.filepath_raw)
        os.replace(tmp_dir / "clean_new", metadata.filepath_clean)
        os.replace(tmp_dir / "features_new", metadata.filepath_features)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        Path(tmp_file).unlink(missing_ok=True)
        raise

    try:
        result = coll.update_one({"_id": ObjectId(metadata.id)}, {"$set": metadata.model_dump(by_alias=True)}, upsert=True)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        Path(tmp_file).unlink(missing_ok=True)
        raise

    shutil.rmtree(tmp_dir, ignore_errors=True)
    Path(tmp_file).unlink(missing_ok=True)

@worker_app.task
def handle_model_upload(tmp_file: str, metadata_dict: str):
    db = get_db_for_worker()
    coll = db.get_collection("models")

    try:
        metadata = ModelMetadata(**json.loads(metadata_dict))
        tmp_dir = Path(tempfile.mkdtemp())
        with zipfile.ZipFile(tmp_file, "r") as zf:
            total = sum(info.file_size for info in zf.infolist())
            if total > MAX_UNCOMPRESSED_SIZE:
                raise Exception()
            zf.extractall(tmp_dir)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        Path(tmp_file).unlink(missing_ok=True)
        raise

    try:
        os.replace(tmp_dir / "model_pth", metadata.filepath_pth)
        os.replace(tmp_dir / "model_weights", metadata.filepath_weights)
        os.replace(tmp_dir / "model_scaler", metadata.filepath_scaler)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        Path(tmp_file).unlink(missing_ok=True)
        raise

    try:
        result = coll.update_one({"_id": ObjectId(metadata.id)}, {"$set": metadata.model_dump(by_alias=True)}, upsert=True)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        Path(tmp_file).unlink(missing_ok=True)
        raise

    shutil.rmtree(tmp_dir, ignore_errors=True)
    Path(tmp_file).unlink(missing_ok=True)


@worker_app.task
def backup_mongodb_and_files():
    BACKUP_DIR_ENV = os.getenv("BACKUP_DIR")
    if not BACKUP_DIR_ENV:
        raise RuntimeError()
    MODEL_DIR_ENV = os.getenv("MODEL_PATH")
    if not BACKUP_DIR_ENV:
        raise RuntimeError()
    DATASET_DIR_ENV = os.getenv("FILE_PATH")
    if not DATASET_DIR_ENV:
        raise RuntimeError()
    backup_dir = Path(BACKUP_DIR_ENV)
    dataset_dir = Path(DATASET_DIR_ENV)
    model_dir = Path(MODEL_DIR_ENV)
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir_mongo = backup_dir / f"mongo_{ts}"
    out_dir_mongo.mkdir(parents=True, exist_ok=True)

    uri = os.getenv("MONGO_URI")

    subprocess.run([
        "mongodump",
        f"--uri={uri}",
        f"--out={out_dir_mongo}"
    ], check=True)

    out_dir_files = backup_dir / f"files_{ts}.zip"
    zip_directory(dataset_dir, out_dir_files)

    out_dir_model = backup_dir / f"model_{ts}.zip"

    zip_directory(model_dir, out_dir_model)

    return {
    "mongo_backup": str(out_dir_mongo),
    "files_backup": str(out_dir_files),
    "model_backup": str(out_dir_model),
    "timestamp": ts
    }
