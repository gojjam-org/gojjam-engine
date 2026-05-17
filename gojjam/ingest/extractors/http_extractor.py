import requests
import pyarrow as pa
import pandas as pd
import logging
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
from requests.auth import HTTPBasicAuth, HTTPDigestAuth, AuthBase
from gojjam.ingest.extractors.base_extract import BaseExtractor

class JWTAuth(AuthBase):
    def __init__(self, token: str):
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

    def format_url_from_dataframe(self, template_str: str, df: pd.DataFrame, **extra_context) -> str:
        if df.empty:
            raise ValueError("DataFrame is empty; cannot extract values.")
        
        raw_context = {**df.iloc[0].to_dict(), **extra_context}
        
        def clean_value(v):
            if isinstance(v, bool): return str(v).lower()
            return "" if pd.isna(v) else str(v)

        cleaned_context = {k: clean_value(v) for k, v in raw_context.items()}
        
        try:
            formatted_url = template_str.format(**cleaned_context)
        except KeyError as e:
            raise KeyError(f"Template requires {e}, but it wasn't found in context.")

        url_parts = list(urlparse(formatted_url))
        query = dict(parse_qsl(url_parts[4]))
        query.update({k: v for k, v in cleaned_context.items() if f"{{{k}}}" not in template_str})
        url_parts[4] = urlencode(query)
        
        return urlunparse(url_parts)

    def _execute_and_yield(self, session, method, url, **kwargs):
        """Centralized request execution and DataFrame conversion."""
        response = session.request(method, url, **kwargs)
        response.raise_for_status()
        full_data = response.json()
        if isinstance(full_data, dict):
            key = list(full_data.keys())[0] if len(full_data) == 1 else None
            data_list = full_data[key] if key and isinstance(full_data[key], list) else [full_data]
        else:
            data_list = full_data

        batch_size = 1000
        for i in range(0, len(data_list), batch_size):
            batch_df = pd.DataFrame(data_list[i : i + batch_size])
            yield pa.Table.from_pandas(batch_df)

    def _strategy_query(self, session, source_cfg, cursor, current_page):
        base_endpoint = str(source_cfg.endpoint).rstrip("/")
        url = self.format_url_from_dataframe(
            template_str=cursor.value_location.location,
            df=current_page,
            endpoint=base_endpoint
        )
        yield from self._execute_and_yield(session, "GET", url)

    def _strategy_body(self, session, source_cfg, cursor, current_page):
        row_data = current_page.iloc[0].to_dict()
        payload = {
            loc_key: row_data.get(col_name) 
            for loc_key, col_name in cursor.value_location.location.items()
        }
        yield from self._execute_and_yield(
            session, "GET", str(source_cfg.endpoint), json=payload
        )

    def _strategy_none(self, session, source_cfg, *args):
        yield from self._execute_and_yield(session, "GET", str(source_cfg.endpoint))

    def extract(self, current_page: pd.DataFrame = None):
        source_cfg = self.model_info["source_config"]
        auth_obj = self._get_auth_strategy(source_cfg)
        cursor = getattr(source_cfg, "cursor", None)
        strategy_map = {
            "QUERY": self._strategy_query,
            "BODY": self._strategy_body
        }
        
        handler = strategy_map.get(getattr(cursor.value_location, "type", None)) if cursor else self._strategy_none
        
        if not handler:
            logging.error(f"Unsupported cursor type: {cursor.value_location.type}")
            return

        with requests.Session() as session:
            session.auth = auth_obj
            try:
                yield from handler(session, source_cfg, cursor, current_page)
            except Exception as e:
                logging.error(f"❌ Extraction Failed: {e}")