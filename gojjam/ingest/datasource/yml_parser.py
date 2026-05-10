import yaml
from pathlib import Path
from pydantic import BaseModel, HttpUrl, SecretStr, ValidationError, Field
from typing import List, Optional

class DataSource(BaseModel):
    name: str
    type: str
    endpoint: Optional[HttpUrl] = None
    auth_type: Optional[str] = "bearer"
    api_key: Optional[str] = None
    username: Optional[str] =""
    password: Optional[str] =""
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_: Optional[str] = Field(default=None,alias="schema")
    path: Optional[str] =None
    root_folder: Optional[str] = None
    azure_blob_storage_connection_string: Optional[str] = None
    container_name: Optional[str] = None

    # s3 config
    bucket_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region:Optional[str] = None

class Config(BaseModel):
    version: str
    sources: List[DataSource]

def get_config(config_path: str = "gojjam_ingest_sources.yml") -> Config:
    path = Path(config_path)
   
    if not path.exists():
        raise FileNotFoundError(
            f"Configuration file '{config_path}' not found. "
            "Please ensure it exists in the root directory."
        )

   
    with open(path, "r") as f:
        try:
            raw_data = yaml.safe_load(f)
            if not raw_data:
                raise ValueError(f"'{config_path}' is empty.")
            
            return Config(**raw_data)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML in {config_path}: {e}")
        except ValidationError as e:
            raise ValueError(f"Configuration validation failed: {e}")


def get_source_map(config: Config):
    return {source.name: source for source in config.sources}