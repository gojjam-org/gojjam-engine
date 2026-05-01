import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class SinkEntry(BaseModel):
    name: str
    type: str
    source_folder: str
    config: Dict[str, Any] = Field(default_factory=dict)

class SinkConfig(BaseModel):
    version: str = "1.0"
    sinks: List[SinkEntry]

def get_sink_config(sink_yaml_path: str = "./sink.yml") -> List[Dict[str, Any]]:

    path = Path(sink_yaml_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Sink configuration file '{sink_yaml_path}' not found.")

    with open(path, "r", encoding="utf-8") as f:
        try:
            raw_data = yaml.safe_load(f)
            if not raw_data:
                raise ValueError(f"'{sink_yaml_path}' is empty.")
            
            validated_data = SinkConfig(**raw_data)
            
            return [sink.model_dump() for sink in validated_data.sinks]
            
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML in {sink_yaml_path}: {e}")
        except Exception as e:
            raise ValueError(f"Validation failed for {sink_yaml_path}: {e}")