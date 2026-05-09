import requests
import pyarrow as pa
import pandas as pd
from requests.auth import HTTPBasicAuth,HTTPDigestAuth,AuthBase
from gojjam.ingest.extractors.base_extract import BaseExtractor


class JWTAuth(AuthBase):
    def __init__(self,token:str):
        self.token = token
    def __call__(self, r):
        token_str = str(self.token)
        prefix = "Bearer "
        auth_value = token_str if token_str.startswith(prefix) else f"{prefix}{token_str}"
        r.headers["Authorization"] = auth_value
        return r
    
class HTTPExtractor(BaseExtractor):

    def _get_auth_strategy(self, source_cfg):
       
        auth_type = getattr(source_cfg, "auth_type", None)
        auth_map = {
            "basic": lambda cfg: HTTPBasicAuth(cfg.username, cfg.password),
            "digest": lambda cfg: HTTPDigestAuth(cfg.username, cfg.password),
            "jwt": lambda cfg: JWTAuth(cfg.api_key) 
        }
        strategy = auth_map.get(auth_type)
        return strategy(source_cfg) if strategy else None

    def extract(self):
        source_cfg = self.model_info["source_config"]
        auth_obj = self._get_auth_strategy(source_cfg)

        try:
            with requests.Session() as session:
                session.auth = auth_obj
                response = session.get(str(source_cfg.endpoint))
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