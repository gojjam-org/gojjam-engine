import logging
import psycopg2
from psycopg2 import errors
from gojjam.calculated_model.base_calculated_model import BaseCalculatedModel

class PostgresCalculator(BaseCalculatedModel):

    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
    
    def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                options=f"-c search_path={self.db_config.schema_name}"
            )
        return self.conn

    def calculate(self, query, cursor):
        logging.info(f"Executing calculation for: {cursor.calculated_model_name}")
        
        conn = self.connect()
        db_cursor = conn.cursor()
        
        try:
            db_cursor.execute(query)
            res = db_cursor.fetchone()
            
            if res is None or res[0] is None:
                return cursor.inital_value
            
            return res[0]

        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            
            table = cursor.calculated_model_name
            column = cursor.calculated_model_column_name or "current_page"
            init_val = cursor.inital_value

            logging.warning(f"Table '{table}' not found. Initializing via CTAS with value: {init_val}")
            
            try:
                create_as_query = f"CREATE TABLE {table} AS SELECT %s AS {column};"
                
                db_cursor.execute(create_as_query, (init_val,))
                conn.commit()
                
                return init_val
                
            except Exception as create_error:
                conn.rollback()
                logging.error(f"Failed to dynamically initialize table {table}: {create_error}")
                raise create_error

        except Exception as e:
            conn.rollback()
            logging.error(f"Postgres calculation failed: {e}")
            raise e
            
        finally:
            db_cursor.close()