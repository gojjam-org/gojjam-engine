import duckdb
import logging
import pyarrow as pa
from gojjam.ingest.engines.base_ingest_engine import BaseIngestEngine
from gojjam.ingest.engines.cursor_ingest_engine import CursorIngestEngine

class SyncCursorIngestEngine(BaseIngestEngine,CursorIngestEngine):
    
    def __init__(self, model):
        BaseIngestEngine.__init__(self, model)
        CursorIngestEngine.__init__(self, model)

    def run(self):
        controller = True
        while controller:
            query = self.get_calculated_model_query(self.cursor.calculated_model_folder_path, self.cursor.calculated_model_name)
            current_result = self.calculator.calculate(query, self.cursor)
            con = duckdb.connect(database=':memory:')
            base_table = self.model.get("base_table_name")
            first_chunk = True
            data_processed_in_loop = False

            for extraction_result in self.extractor.extract(current_result):
                if len(extraction_result) > 0:
                    data_processed_in_loop = True
                    con.register(base_table, extraction_result)
                    result_reader = con.execute(self.model["sql_code"]).fetch_record_batch()
                    
                    for batch in result_reader:
                        chunk_table = pa.Table.from_batches([batch])
                        success = self.loader.load(chunk_table, is_first_chunk=first_chunk)
                        first_chunk = False
                        
                        if success:
                            if not self.update_cursor(current_result):
                                controller = False
                else:
                    controller = False

            if not data_processed_in_loop:
                controller = False