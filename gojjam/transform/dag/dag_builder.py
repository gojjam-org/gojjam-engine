import sqlglot
from sqlglot import exp
from graphlib import TopologicalSorter
from pathlib import Path

class DagBuilder:
    def __init__(self,source_folder):
        self.source_folder = source_folder
    
    def build_dag_order(self):
        dag = {}
        folder_path = Path(self.source_folder)
        all_models = {f.stem.lower() for f in folder_path.glob("*.sql")}

        for sql_file in folder_path.glob("*.sql"):
            model_name = sql_file.stem.lower()
            sql_code = sql_file.read_text()
            
            dependencies = set()
            try:
                parsed = sqlglot.parse_one(sql_code, read="postgres")
                for table in parsed.find_all(exp.Table):
                    table_name = table.name.lower()
                    if table_name in all_models and table_name != model_name:
                        dependencies.add(table_name)
            except Exception as e:
                print(f"⚠️ Warning: Could not parse {model_name}. Error: {e}")
            
            dag[model_name] = dependencies

        ts = TopologicalSorter(dag)
        return list(ts.static_order())
