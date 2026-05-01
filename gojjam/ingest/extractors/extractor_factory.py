from importlib.metadata import entry_points

class ExtractorFactory:
    @staticmethod
    def get_extractor(model_info):
        source_config = model_info["source_config"]
        source_type = getattr(source_config, "type", "http").lower()
        eps = entry_points(group='gojjam.ingest.extractors')
        plugin = next((ep for ep in eps if ep.name == source_type), None)
        
        if not plugin:
            raise ImportError(
                f"❌ Extractor '{source_type}' not found. "
                f"Make sure you've installed the extra: pip install gojjam-engine[{source_type}]"
            )

        extractor_class = plugin.load()
        return extractor_class(model_info)