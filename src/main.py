from contextlib import asynccontextmanager
import json
import bson as bs
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Query
from starlette.background import BackgroundTask
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
from .utils import jsonl_to_bson, bson_to_jsonl, zip_directory, plain_json
import tempfile
from .db import connect_to_db
import os
from typing import Any, List, Mapping, Optional
from .models import MeasurementMetadata
import shutil
import dotenv
from pymongo.asynchronous.collection import AsyncCollection
from .tasks import handle_measurement_upload


@asynccontextmanager
async def lifespan(app: FastAPI):
    dotenv.load_dotenv(".env")
    app.state.client = await connect_to_db()
    app.state.FILE_PATH=Path(os.getenv("FILE_PATH"))
    app.state.FILE_PATH.mkdir(exist_ok=True)
    app.state.db = app.state.client.get_database("brics")
    yield
    await app.state.client.close()


app = FastAPI(title="BRICS API",lifespan=lifespan)

@app.get("/test")
async def test():
    return {"status": "ok"}

@app.put("/measurement/upload")
async def uploadMeasurement(measurement_file_zip: UploadFile = File(...),
                            measurement_metadata: str = Form(...)):
    
    try:
        metadata_dict = json.loads(measurement_metadata)
        if not isinstance(metadata_dict, dict):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Metadata must be a JSON object")
    
    measurement_coll: AsyncCollection = app.state.db.get_collection("measurements")
    
    if "_id" not in metadata_dict:
        raise HTTPException(400, "Missing ID from metadata")
    
    try:
        _id = bs.ObjectId(metadata_dict["_id"])
    except Exception:
        _id = bs.ObjectId()

    existing_measurement = await measurement_coll.find_one({"_id": _id})

    if existing_measurement:
        measurement_id = str(existing_measurement["_id"])
    else:
        measurement_id = str(_id)

    filepath_raw: Path = app.state.FILE_PATH / f"{measurement_id}_raw"
    filepath_clean: Path = app.state.FILE_PATH / f"{measurement_id}_clean"
    filepath_features: Path = app.state.FILE_PATH / f"{measurement_id}_features"
    metadata_dict["_id"] = measurement_id
    metadata_dict["filepath_raw"] = str(filepath_raw)
    metadata_dict["filepath_clean"] = str(filepath_clean)
    metadata_dict["filepath_features"] = str(filepath_features)
    metadata = MeasurementMetadata(**metadata_dict)

    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    shutil.copyfileobj(measurement_file_zip.file, tmp_file)
    handle_measurement_upload.delay(tmp_file.name, metadata.model_dump())
    
    measurement_file_zip.file.close()
    return_json = metadata.model_dump_json(by_alias=True)
                
    return JSONResponse(content=return_json, status_code=202)

@app.get("/measurement/download")
async def downloadMeasurements( person_id: Optional[str] = Query(None),  
                                length_min: Optional[int] = Query(0),
                                length_max: Optional[int] = Query(86400000),
                                age_min: Optional[int] = Query(0),
                                age_max: Optional[int] = Query(100),
                                level: List[str] = Query(["raw", "clean", "features"]),
                                gender: Optional[List[str]] = Query(None),
                                activity: Optional[List[str]] = Query(None),
                                condition: Optional[List[str]] = Query(None),
                                health: Optional[List[str]] = Query(None),
                                weight_min: Optional[int] = Query(0),
                                weight_max: Optional[int] = Query(200),
                                height_min: Optional[int] = Query(0),
                                height_max: Optional[int] = Query(250)):

    coll: AsyncCollection  = app.state.db.get_collection("measurements")

    query: dict[str, Any] = {}

    if person_id:
        query["labels.person_data.person_id"] = person_id

    query["duration_ms"] = {"$gte": length_min, "$lte": length_max}

    query["labels.person_data.weight"] = {"$gte": weight_min, "$lte": weight_max}
    query["labels.person_data.height"] = {"$gte": height_min, "$lte": height_max}
    query["labels.person_data.age"] = {"$gte": age_min, "$lte": age_max}

    if gender:
        query["labels.person_data.gender"] = {"$in": gender}

    if activity:
        query["labels.activity"] = {"$in": activity}

    if condition:
        query["labels.person_data.condition"] = {"$in": condition}

    if health:
        query["labels.person_data.health"] = {"$in": health}

    measurementIndexes = coll.find(query)

    tmp_dir = Path(tempfile.mkdtemp())
    dataset_dir = tmp_dir / "dataset"
    dataset_dir.mkdir()

    allowed_levels = ["raw", "clean", "features"]
    if not set(level).issubset(allowed_levels):
        raise HTTPException(400, "Invalid level value")

    for type in level:
        (dataset_dir / type).mkdir(parents=True, exist_ok=True)
    try:
        async for index in measurementIndexes:
            measurement = MeasurementMetadata(**index)
            if "clean" in level:
                temp_file_path = dataset_dir / "clean" / Path(measurement.filepath_clean).name
                await bson_to_jsonl(Path(measurement.filepath_clean), temp_file_path, measurement)
            if "raw" in level:
                temp_file_path = dataset_dir / "raw" / Path(measurement.filepath_raw).name
                await bson_to_jsonl(Path(measurement.filepath_raw), temp_file_path, measurement)
            if "features" in level:
                temp_file_path = dataset_dir / "features" / Path(measurement.filepath_features).name
                await bson_to_jsonl(Path(measurement.filepath_features), temp_file_path, measurement)
                
        zip_path = tmp_dir / "measurements_dataset.zip"
        zip_directory(dataset_dir, zip_path)

        return FileResponse(
            zip_path, filename=f"measurements_dataset.zip", media_type="application/zip", background=BackgroundTask(shutil.rmtree, tmp_dir)
        )
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

@app.delete("/measurement/delete")
async def deleteMeasurement(measurement_id: str = Query(None)):
    coll: AsyncCollection = app.state.db.get_collection("measurements")

    filepath_raw: Path = app.state.FILE_PATH / f"{measurement_id}_raw"
    filepath_clean: Path = app.state.FILE_PATH / f"{measurement_id}_clean"
    filepath_features: Path = app.state.FILE_PATH / f"{measurement_id}_features"

    try:
        oid = bs.ObjectId(measurement_id)
    except Exception:
        raise HTTPException(400, "Invalid measurement_id")


    measurement = await coll.find_one_and_delete({"_id": oid})

    if measurement:
        filepath_raw.unlink(missing_ok=True)
        filepath_clean.unlink(missing_ok=True)
        filepath_features.unlink(missing_ok=True)

    return JSONResponse(content=measurement, status_code=200)


