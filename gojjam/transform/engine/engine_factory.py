import importlib.metadata
from gojjam.transform.engine.base_transform_engine import BaseTransformEngine
class TransformEngineFactory:
    @staticmethod
    def get_engine(db_config) -> BaseTransformEngine:
        eps = importlib.metadata.entry_points(group='gojjam.transform.engines')
        
        match = next((ep for ep in eps if ep.name == db_config.type), None)
        
        if not match:
            raise ValueError(
                f"Unsupported database type: '{db_config.type}'. "
                f"Ensure 'gojjam-{db_config.type}' is installed."
            )

        engine_class = match.load()
        return engine_class(db_config)