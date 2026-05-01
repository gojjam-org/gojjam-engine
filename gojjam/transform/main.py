from pathlib import Path
from gojjam.transform.utils.yml_parser import get_resolved_transforms
from gojjam.transform.engine.engine_factory import TransformEngineFactory
from gojjam.transform.dag.dag_builder import DagBuilder

class GojjamTransformEngine:
    def __init__(self, trans_path="transform-config.yml", sink_path="sink.yml"):
        self.resolved_configs = get_resolved_transforms(trans_path, sink_path)
        self.execution_plans = {}

    def deploy_and_register(self):
        for item in self.resolved_configs:
            name = item["transform_name"]
            source = Path(item["transform_source"])
            db_config = item["db_config"]
            
            engine = TransformEngineFactory.get_engine(db_config)
            dag_builder = DagBuilder(source)
            order = dag_builder.build_dag_order()

            self.execution_plans[name] = {
                "order": order,
                "engine": engine, 
                "source": source
            }

            print(f"🛠️ Registering procedures for: {name} ({db_config.type})")
            
            for model in order:
                sql_path = source / f"{model}.sql"
                sql_content = sql_path.read_text().strip().rstrip(';')
                engine.register_procedure(model, sql_content)
            
            print(f"✅ All procedures registered successfully in {db_config.database}")

    def run_all(self):
        for plan_name, plan in self.execution_plans.items():
            print(f"🚀 Starting Execution Plan: {plan_name}")
            engine = plan["engine"]
            for proc in plan["order"]:
                print(f"   ➡️ Executing CALL update_{proc}()")
                engine.execute_procedure(proc)
            engine.close()
            print(f"✨ Plan '{plan_name}' completed successfully.")
