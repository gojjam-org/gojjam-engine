import duckdb
import pyarrow as pa
from gojjam.ingest.extractors.extractor_factory import ExtractorFactory
from gojjam.ingest.loaders.load_factory import LoadFactory

def estl(model_bundle):
    extractor = ExtractorFactory.get_extractor(model_bundle)
    loader = LoadFactory.get_loader(model_bundle["sink_info"])
    
    source_type = model_bundle['source_config'].type
    first_chunk = True

    try:
        if source_type in ['postgres', 'duckdb', 'snowflake']:
            print(f"⚡ Push-Down Mode: {source_type} is executing the transform.")
            for arrow_batch in extractor.extract():
                loader.load(arrow_batch, is_first_chunk=first_chunk)
                first_chunk = False
        else:
            print(f"🦆 Sidecar Mode: Using local DuckDB for transformation.")
            con = duckdb.connect(database=':memory:')
            base_table = model_bundle.get("base_table_name")
            
            try:
                for raw_arrow_table in extractor.extract():
                    con.register(base_table, raw_arrow_table)
                    result_reader = con.execute(model_bundle["sql_code"]).fetch_record_batch()
                    
                    for batch in result_reader:
                        chunk_table = pa.Table.from_batches([batch])
                        loader.load(chunk_table, is_first_chunk=first_chunk)
                        first_chunk = False
            finally:
                con.close()

        print(f"✅ Gojjam ingest complete: {model_bundle.get('model_name')}")

    except Exception as e:
        print(f"❌ Execution Error in Gojjam ingest: {e}")
        raise e