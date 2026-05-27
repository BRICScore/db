from pydantic import BaseModel, Field, ConfigDict, field_serializer
from typing import Literal, Optional
from bson import ObjectId

class BioData(BaseModel):
    person_id: str
    age: int = Field(..., ge=0, le=100)
    gender: Literal["male", "female"]
    health: str
    condition: Literal["sedentary", "regular", "active", "extreme"]
    weight: int
    height: int

class LabelsData(BaseModel):
    activity: str
    person_data: BioData

class MeasurementMetadata(BaseModel):

    model_config = ConfigDict(arbitrary_types_allowed=True, json_encoders={ObjectId: str}, populate_by_name=True)

    id: ObjectId = Field(alias="_id")
    timestamp: float
    duration_ms: int
    filepath_raw: str
    filepath_clean: str
    filepath_features: str
    labels: LabelsData


class ModelMetadata(BaseModel):

    model_config = ConfigDict(populate_by_name=True)

    id: ObjectId = Field(alias="_id")
    timestamp: float
    filepath_weights: str
    filepath_pth: str
    filepath_scaler: str


