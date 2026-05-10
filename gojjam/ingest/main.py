import logging
from pathlib import Path
from sqlglot import exp, parse_one
from gojjam.ingest.engine.estl import estl
from gojjam.ingest.sink.yml_parser import get_sink_config
from gojjam.ingest.model.model_loader import get_sql_models
from gojjam.ingest.datasource.yml_parser import get_config, get_source_map
from gojjam.calculated_model.calculated_model_factory import CalculatedModelFactory

class GojjamIngestEngine:
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
    

    def get_calculated_model_query(self, folder_path, file_name):

        if not file_name.endswith(".sql"):
            file_name = f"{file_name}.sql"

        file_path = Path(folder_path) / file_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"Query file not found at: {file_path}")
    
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read().strip()
            
        return query
    
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
                # INC = PERM
                    # Check if model need a calc model
                    # If so find calc model with the needed name
                    # Run the sql query to get the calc model
                    # Attached result to the dict
                    # pass the value into estl
              
                if getattr(source_cfg,"cursor"):
                    cursor = source_cfg.cursor
                    calculator = CalculatedModelFactory.get_calculated_model(cursor.db_config)                    
                    query = self.get_calculated_model_query(cursor.calculated_model_folder_path,cursor.calculated_model_name)
                  
                    current_page = calculator.calculate(query,cursor)
                
                    if source_cfg.type == 'http':
                        base_endpoint = str(source_cfg.endpoint).rstrip("/")
                        formatted_url = cursor.value_location.format(
                            endpoint=base_endpoint,
                            value=current_page
                        )

                        source_cfg.endpoint = formatted_url
                        print(f"Target URL: {formatted_url}")
                model["source_config"] = source_cfg
                if source_cfg.type in ["postgres", "duckdb"] and model["namespace"]:
                    model["sql_code"] = self.resolve_namespace(model["sql_code"], model["namespace"],source_cfg.schema,source_cfg.type)
              
                
                estl(model)