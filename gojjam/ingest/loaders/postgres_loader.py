import pyarrow as pa
from sqlalchemy import create_engine, text
from gojjam.ingest.loaders.base_loader import BaseLoader

class PostgresLoader(BaseLoader):
    
    def load(self, data: pa.Table, is_first_chunk: bool = True):
        db_conf = self.sink_config["db_config"] 
        engine_url = f"postgresql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['database']}"
        engine = create_engine(engine_url)

        target_table = self.sink_config["target_table"]
        target_schema = self.sink_config["schema"]

        df = data.to_pandas()

        with engine.begin() as connection:
            if is_first_chunk:
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema};"))
                mode = 'replace'
            else:
                mode = 'append'

            df.to_sql(
                name=target_table,
                con=connection,
                schema=target_schema,
                if_exists=mode, 
                index=False
            )