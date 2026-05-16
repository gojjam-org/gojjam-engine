from gojjam.ingest.engines.base_ingest_engine import BaseIngestEngine
from importlib.metadata import entry_points

class IngestEngineFactory:
    @staticmethod
    def get_ingest_engine(model_config) -> BaseIngestEngine:
        source_config = model_config.get("source_config")
        cursor = getattr(source_config, "cursor", None)
        eps = entry_points(group="gojjam.ingest.engines")

        if cursor:
            cursor_type = getattr(cursor,"cursor_type",None)
            
            plugin = next((ep for ep in eps if ep.name == str(cursor_type).lower()), None)
            if not plugin:
                raise ImportError(f"{cursor_type} ingest engine plugin not found.")
        else:
            plugin = next((ep for ep in eps if ep.name == "default"), None)
            if not plugin:
                raise ImportError("Default ingest engine plugin not found.")

        engine_class = plugin.load()
        return engine_class(model_config)