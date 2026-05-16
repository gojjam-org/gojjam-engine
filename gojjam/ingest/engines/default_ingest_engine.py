import duckdb
import pyarrow as pa
from gojjam.ingest.engines.base_ingest_engine import BaseIngestEngine
from gojjam.ingest.extractors.extractor_factory import ExtractorFactory
from gojjam.ingest.loaders.load_factory import LoadFactory

class DefaultIngestEngine(BaseIngestEngine):

    def run(self):
        try:
            source_type = self.model['source_config'].type
            first_chunk = True
            if source_type in ['postgres', 'duckdb', 'snowflake']:
                print(f"⚡ Push-Down Mode: {source_type} is executing the transform.")
                for arrow_batch in self.extractor.extract():
                    self.loader.load(arrow_batch, is_first_chunk=first_chunk)
                    first_chunk = False
            else:
                con = duckdb.connect(database=':memory:')
                base_table = self.model.get("base_table_name")
                
                try:
                    for raw_arrow_table in self.extractor.extract():
                        con.register(base_table, raw_arrow_table)
                        result_reader = con.execute(self.model["sql_code"]).fetch_record_batch()
                        
                        for batch in result_reader:
                            chunk_table = pa.Table.from_batches([batch])
                            self.loader.load(chunk_table, is_first_chunk=first_chunk)
                            first_chunk = False
                finally:
                    con.close()

            print(f"✅ Gojjam ingest complete: {self.model.get('model_name')}")

        except Exception as e:
            print(f"❌ Execution Error in Gojjam ingest: {e}")
            raise e