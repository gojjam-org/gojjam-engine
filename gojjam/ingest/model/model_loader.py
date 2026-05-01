from pathlib import Path
from sqlglot import exp,parse_one

def get_sql_models(sink_configs: list):
    all_discovered_models = []

    for sink in sink_configs:
        models_dir = sink.get("source_folder")
        path = Path(models_dir)

        if not path.exists():
            print(f"⚠️ Warning: Folder '{models_dir}' for sink '{sink['name']}' not found. Skipping.")
            continue

        for sql_file in path.rglob("*.sql"):
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_code = f.read()
                try:
                    parsed = parse_one(sql_code)
           
                    table_expr = parsed.find(exp.Table)
                    
                    namespace = table_expr.db if table_expr and table_expr.db else None
                    base_table = table_expr.name if table_expr else None

        
                    all_discovered_models.append({
                        "model_name": sql_file.stem,
                        "sql_code": sql_code,
                        "base_table_name": base_table,
                        "namespace": namespace,
                        "sink_info": {
                            "sink_name": sink['name'],
                            "type": sink['type'],
                            "schema": sink['config'].get('schema'),
                            "target_table": sql_file.stem,
                            "db_config": sink['config'] 
                        }
                    })
                except Exception as e:
                    print(f"⚠️ Error parsing {sql_file.name} in {models_dir}: {e}")

    return all_discovered_models