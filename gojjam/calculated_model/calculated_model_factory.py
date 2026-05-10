from importlib.metadata import entry_points
from gojjam.calculated_model.base_calculated_model import BaseCalculatedModel
class CalculatedModelFactory:
    @staticmethod
    def get_calculated_model(config) -> BaseCalculatedModel:
        type = getattr(config,"type").lower()
        eps = entry_points(group="gojjam.calculated_model")
        plugin = next((ep for ep in eps if ep.name == type), None)
        
        if not plugin:
            raise ImportError(
                f"❌ Extractor '{type}' not found. "
            )

        calculated_model_class = plugin.load()
        return calculated_model_class(config)
