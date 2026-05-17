import logging
import pandas as pd
import psycopg2
from psycopg2 import sql
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
    
        fallback_data = {
            col.name: col.initial_value
            for col in cursor.calculated_model_column_names
        }
        
        try:
           
            with conn.cursor() as db_cursor:
                db_cursor.execute(query)
                rows = db_cursor.fetchall()
                colnames = [desc[0] for desc in db_cursor.description]
                df = pd.DataFrame(rows, columns=colnames)
                
            if df.empty:
                logging.warning(f"Query for {cursor.calculated_model_name} returned no rows. Using fallback.")
                return pd.DataFrame([fallback_data])
            
            return df

        except psycopg2.errors.UndefinedTable:
            conn.rollback()
            
            table_name = cursor.calculated_model_name
            
            try:
                with conn.cursor() as db_cursor:
             
                    select_parts = []
                    initial_values = []
                    
                    for col in cursor.calculated_model_column_names:
    

                        select_parts.append(
                            sql.SQL("{} AS {}").format(
                                sql.Placeholder(), 
                                sql.Identifier(col.name)
                            )
                        )
                        initial_values.append(col.initial_value)
                    
                    create_query = sql.SQL("CREATE TABLE {} AS SELECT ").format(
                        sql.Identifier(table_name)
                    ) + sql.SQL(", ").join(select_parts)
                    
                   
                    db_cursor.execute(create_query, initial_values)
                    conn.commit()
                
              
                return pd.DataFrame([fallback_data])
                
            except Exception as create_error:
                conn.rollback()
                logging.error(f"Failed to dynamically initialize table {table_name}: {create_error}")
                logging.error(create_error)
                raise create_error

        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logging.error(f"Postgres calculation failed: {e}")
            raise e
        
        finally:
            if self.conn:
                self.conn.close()