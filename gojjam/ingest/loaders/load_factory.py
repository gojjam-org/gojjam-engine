from importlib.metadata import entry_points

class LoadFactory:
    @staticmethod
    def get_loader(sink_config):
        source_type = sink_config.get("type", "postgres").lower()
        eps = entry_points(group='gojjam.ingest.loaders')
        plugin = next((ep for ep in eps if ep.name == source_type), None)
        
        if not plugin:
            raise ImportError(
                f"❌ Extractor '{source_type}' not found. "
                f"Make sure you've installed the extra: pip install gojjam-engine[{source_type}]"
            )

        loader_class = plugin.load()
        return loader_class(sink_config)