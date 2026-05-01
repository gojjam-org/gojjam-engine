import requests
import pyarrow as pa
import pandas as pd
from requests.auth import HTTPBasicAuth
from gojjam.ingest.extractors.base_extract import BaseExtractor

class HTTPExtractor(BaseExtractor):
    def extract(self):
        source_cfg = self.model_info["source_config"]
        auth = None
        
        if getattr(source_cfg, "auth_type", None) == "basic":
            auth = HTTPBasicAuth(source_cfg.username, source_cfg.password)

        try:
            response = requests.get(str(source_cfg.endpoint), auth=auth)
            response.raise_for_status()
            full_data = response.json()
        except Exception as e:
            print(f"❌ API Request Failed: {e}")
            return

        if isinstance(full_data, dict):
            key = list(full_data.keys())[0] if len(full_data) == 1 else None
            data_list = full_data[key] if key and isinstance(full_data[key], list) else [full_data]
        else:
            data_list = full_data

        batch_size = 1000 
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i : i + batch_size]
            batch_df = pd.DataFrame(batch)
            yield pa.Table.from_pandas(batch_df)