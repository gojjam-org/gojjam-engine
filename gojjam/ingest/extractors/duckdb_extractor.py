import duckdb
import pandas as pd
from gojjam.ingest.extractors.base_extract import BaseExtractor

class DuckDbExtractor(BaseExtractor):
    def extract(self) -> pd.DataFrame:
        db_conf = self.model_info["source_config"]
        sql_code = self.model_info["sql_code"]
        
        db_path = getattr(db_conf, 'database', ':memory:')
        
        con = None
        try:
            con = duckdb.connect(database=db_path, read_only=True)
            
            print(f"🦆 Connected to DuckDB Source: {db_path}")
            schema = getattr(db_conf, 'schema', 'main')
            con.execute(f"SET search_path = '{schema}'")
            df = con.execute(sql_code).df()
            
            return df

        except Exception as e:
            print(f"❌ DuckDB Extraction Failed: {e}")
            raise e
        finally:
            if con:
                con.close()
                print("🦆 DuckDB connection closed.")