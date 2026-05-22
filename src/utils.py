import json
from pathlib import Path
from typing import Optional
import zipfile
import bson
from fastapi import UploadFile
from .models import MeasurementMetadata
import aiofiles

async def jsonl_to_bson(src: Path, dst: Path):
    async with aiofiles.open(dst, "wb") as f_out, aiofiles.open(src, "r") as f_in:
        data = await f_in.read()
        for line in data.splitlines():
            if not line: 
                continue
            doc = json.loads(line)
            await f_out.write(bson.BSON.encode(doc))

async def bson_to_jsonl(src: Path, dst: Path, metadata: MeasurementMetadata):
    async with aiofiles.open(dst, "w", encoding="utf-8") as f_out:
        dict = metadata.model_dump_json(by_alias=True)
        await f_out.write(dict)
        await f_out.write("\n")
        with open(src, "rb") as f_in:
            for doc in bson.decode_file_iter(f_in):
                line = json.dumps(doc)
                await f_out.write(line + "\n")

async def plain_json(src: Path, dst: Path, metadata: Optional[MeasurementMetadata] = None):
    async with aiofiles.open(dst, "w", encoding="UTF-8") as f_out, aiofiles.open(src, "r") as f_in:
        if metadata:
            f_out.write(metadata.model_dump_json(by_alias=True))
        for line in f_in.readlines():
            if not line:
                continue
            f_out.write(line + "\n")
        

def zip_directory(src_dir: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for file in src_dir.rglob("*"):
            if file.is_file():
                zf.write(
                    file,
                    arcname=file.relative_to(src_dir)
                )
