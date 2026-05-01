import psycopg2
from gojjam.transform.engine.base_transform_engine import BaseTransformEngine

class PostgresTransformEngine(BaseTransformEngine):
    def __init__(self, db_config):
        self.config = db_config
        self.conn = None

    def connect(self):
        """Manages the connection state."""
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                options=f"-c search_path={self.config.schema_name}"
            )
        return self.conn

    def get_materialization_sql(self, model_name, sql_content):
        return f"""
        CREATE OR REPLACE PROCEDURE {self.config.schema_name}.update_{model_name}()
        LANGUAGE plpgsql AS $$
        BEGIN
            EXECUTE 'DROP TABLE IF EXISTS {self.config.schema_name}.{model_name} CASCADE';
            EXECUTE 'CREATE TABLE {self.config.schema_name}.{model_name} AS {sql_content.replace("'", "''")}';
        END; $$;
        """

    def register_procedure(self, name, sql_content):
        sql = self.get_materialization_sql(name, sql_content)
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

    def execute_procedure(self, name):
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(f"CALL {self.config.schema_name}.update_{name}();")
        conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()