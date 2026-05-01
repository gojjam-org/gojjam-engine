import psycopg2
import pyarrow as pa
import pandas as pd
from gojjam.ingest.extractors.base_extract import BaseExtractor

class PostgresExtractor(BaseExtractor):
    def extract(self):
        db_conf = self.model_info["source_config"]
        sql_code = self.model_info["sql_code"]
        itersize = 2000 

        conn = None
        try:
            conn = psycopg2.connect(
                host=db_conf.host,
                port=db_conf.port,
                database=db_conf.database,
                user=db_conf.username,
                password=db_conf.password
            )
            conn.autocommit = True 
            
            with conn.cursor() as cur:
                schema = getattr(db_conf, "schema", "public")
                cur.execute(f"SET search_path TO {schema}, public;")
        
                cur.execute(sql_code)
                
                if cur.description is None:
                    return

                colnames = [desc[0] for desc in cur.description]
                
                while True:
                    rows = cur.fetchmany(itersize)
                    if not rows:
                        break
                    
                    batch_df = pd.DataFrame(rows, columns=colnames)
                    yield pa.Table.from_pandas(batch_df)

        except Exception as e:
            print(f"❌ Postgres Extraction Failed: {e}")
            raise e
        finally:
            if conn:
                conn.close()