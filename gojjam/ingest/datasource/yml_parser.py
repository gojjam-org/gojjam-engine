import yaml
from pathlib import Path
from pydantic import BaseModel, HttpUrl, Field, ValidationError, AnyUrl
from typing import List, Optional, Any, Union

class ConfigRef(BaseModel):
    ref: str

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

class Cursor(BaseModel):
    calculated_model_name: Optional[str] = None
    calculated_model_folder_path: Optional[str] = None
    calculated_model_column_name: Optional[str] = None
    value_location: Optional[str] = None
    inital_value: Optional[Any] = None
    db_config: Optional[Union[ConfigRef, DBConn, Any]] = None

class DataSource(BaseModel):
    name: str
    type: str
    endpoint: Optional[Union[HttpUrl, str]] = None
    auth_type: Optional[str] = "bearer"
    api_key: Optional[str] = None
    username: Optional[str] = ""
    password: Optional[str] = ""
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    path: Optional[str] = None
    root_folder: Optional[str] = None
    
    # Azure Config
    azure_blob_storage_connection_string: Optional[str] = None
    container_name: Optional[str] = None

    # S3 Config
    bucket_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region: Optional[str] = None

    # Cursor Config
    cursor: Optional[Cursor] = None

class Config(BaseModel):
    version: str
    sources: List[DataSource]


def get_config(
    config_path: str = "gojjam_ingest_sources.yml", 
    sink_path: str = "sink.yml"
) -> Config:

    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(
            f"Configuration file '{config_path}' not found."
        )

    with open(path, "r") as f:
        try:
            raw_data = yaml.safe_load(f)
            if not raw_data:
                raise ValueError(f"'{config_path}' is empty.")
            
            config = Config(**raw_data)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML in {config_path}: {e}")
        except ValidationError as e:
            raise ValueError(f"Configuration validation failed: {e}")

    has_refs = any(
        source.cursor and isinstance(source.cursor.db_config, ConfigRef) 
        for source in config.sources
    )

    if has_refs:
        sink_file = Path(sink_path)
        if not sink_file.exists():
            raise FileNotFoundError(f"Source configuration contains refs, but {sink_path} was not found.")
        
        with open(sink_file, "r") as f:
            sink_raw = yaml.safe_load(f)
            sink_data = SinkConfig(**sink_raw)
            sinks_map = {s.name: s for s in sink_data.sinks}

        for source in config.sources:
            if source.cursor and isinstance(source.cursor.db_config, ConfigRef):
                ref_str = source.cursor.db_config.ref
                ref_name = ref_str.split(".")[-1]
                
                match = sinks_map.get(ref_name)
                if not match:
                    raise ValueError(f"Reference '{ref_str}' not found in {sink_path}")

                resolved_db = match.config.model_copy()
                resolved_db.type = match.type 
                
                # Replace the ConfigRef object with the actual DBConn object
                source.cursor.db_config = resolved_db

    return config

def get_source_map(config: Config):
    return {source.name: source for source in config.sources}