import logging
import pandas as pd  # Added pandas import
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

    def calculate(self, query, cursor) -> pd.DataFrame:
        logging.info(f"Executing calculation for: {cursor.calculated_model_name}")
        
        conn = self.connect()
        
        try:
            # Use pandas to execute the query and return a DataFrame
            df = pd.read_sql_query(query, conn)
            
            # If the dataframe is empty, handle initialization logic
            if df.empty:
                # Returns an empty DataFrame with the expected columns or the initial value
                return pd.DataFrame([cursor.inital_value], columns=[cursor.calculated_model_column_name or "result"])
            
            return df

        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            
            table = cursor.calculated_model_name
            column = cursor.calculated_model_column_name or "current_page"
            init_val = cursor.inital_value

            logging.warning(f"Table '{table}' not found. Initializing via CTAS with value: {init_val}")
            
            try:
                # We still use a standard cursor for DDL (CREATE TABLE)
                with conn.cursor() as db_cursor:
                    create_as_query = f"CREATE TABLE {table} AS SELECT %s AS {column};"
                    db_cursor.execute(create_as_query, (init_val,))
                    conn.commit()
                
                # Return the initial value as a single-row DataFrame
                return pd.DataFrame([{column: init_val}])
                
            except Exception as create_error:
                conn.rollback()
                logging.error(f"Failed to dynamically initialize table {table}: {create_error}")
                raise create_error

        except Exception as e:
            conn.rollback()
            logging.error(f"Postgres calculation failed: {e}")
            raise e
        
        finally:
            if self.conn:
                self.conn.close()