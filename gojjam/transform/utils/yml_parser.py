import yaml
from pathlib import Path
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Union, Any

class DBConn(BaseModel):
    type: Optional[str] = None 
    host: str
    port: int
    database: str
    user: str
    password: str
    schema_name: str = Field(..., alias="schema")

class SinkItem(BaseModel):
    name: str
    type: str
    source_folder: str
    config: DBConn

class SinkConfig(BaseModel):
    version: str
    sinks: List[SinkItem]

class TransformConfigRef(BaseModel):
    ref: str

class TransformItem(BaseModel):
    name: str
    source_folder: str
    config: Union[TransformConfigRef, DBConn]

class TransformConfig(BaseModel):
    version: str
    config: List[TransformItem]


def get_resolved_transforms(trans_path: str = "transform-config.yml", sink_path: str = "sink.yml"):
    with open(trans_path, "r") as f:
        t_raw = yaml.safe_load(f)
        t_data = TransformConfig(**t_raw)

    s_data = None
    if any(isinstance(item.config, TransformConfigRef) for item in t_data.config):
        with open(sink_path, "r") as f:
            s_raw = yaml.safe_load(f)
            s_data = SinkConfig(**s_raw)

    resolved_items = []

    for item in t_data.config:
        if isinstance(item.config, TransformConfigRef):
            ref_name = item.config.ref.split(".")[-1]
            match = next((s for s in s_data.sinks if s.name == ref_name), None)
            
            if not match:
                raise ValueError(f"Reference '{ref_name}' not found in {sink_path}")

    
            db_config = match.config
            db_config.type = match.type 

            resolved_items.append({
                "transform_name": item.name,
                "transform_source": item.source_folder,
                "sink_source_folder": match.source_folder,
                "db_config": db_config
            })
        else:
       
            if not item.config.type:
                 raise ValueError(f"Direct transform config '{item.name}' is missing a 'type' field.")
            
            resolved_items.append({
                "transform_name": item.name,
                "transform_source": item.source_folder,
                "sink_source_folder": None,
                "db_config": item.config
            })

    return resolved_items

