from sqlglot import exp, parse_one
from gojjam.ingest.engines.ingest_engine_factory import IngestEngineFactory
from gojjam.ingest.sink.yml_parser import get_sink_config
from gojjam.ingest.model.model_loader import get_sql_models
from gojjam.ingest.datasource.yml_parser import get_config, get_source_map

class GojjamIngestRunner:
    def __init__(self, datasource_path, sink_path):
        self.sources = get_source_map(get_config(datasource_path,sink_path)) 
        self.sinks = get_sink_config(sink_path)
        self.models = get_sql_models(self.sinks)
        self.calculated_models = dict()

    def resolve_namespace(self,sql, virtual_namespace, real_schema,database_type):
        expression = parse_one(sql, read=database_type)
        
        for table in expression.find_all(exp.Table):
            if table.db == virtual_namespace:
                table.set("db", real_schema)
                
        return expression.sql(dialect=database_type)
    
    
    def run_all(self):
        for source_name, source_cfg in self.sources.items():
            
            matching_models = [
                m for m in self.models 
                if m["namespace"] == source_name 
                    or 
                (m["namespace"] is None and m["base_table_name"] == source_name)
            ]

            if not matching_models:
                continue

            print(f"🚀 Processing Source: {source_name} ({len(matching_models)} models)")

            for model in matching_models:
                model["source_config"] = source_cfg
                if source_cfg.type in ["postgres", "duckdb"] and model["namespace"]:
                    model["sql_code"] = self.resolve_namespace(model["sql_code"], model["namespace"],source_cfg.schema,source_cfg.type)
              
                
                engine = IngestEngineFactory.get_ingest_engine(model)
                engine.run()
    