from pathlib import Path
import logging
import duckdb
import pyarrow as pa
from gojjam.ingest.extractors.extractor_factory import ExtractorFactory
from gojjam.ingest.loaders.load_factory import LoadFactory
from gojjam.calculated_model.calculated_model_factory import CalculatedModelFactory

def get_calculated_model_query(folder_path, file_name):

        if not file_name.endswith(".sql"):
            file_name = f"{file_name}.sql"

        file_path = Path(folder_path) / file_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"Query file not found at: {file_path}")
    
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read().strip()
            
        return query

def estl(model_bundle):


    if getattr(model_bundle["source_config"],"cursor"):

        cursor = model_bundle["source_config"].cursor

        if cursor.cursor_type == "INC":
            calculator = CalculatedModelFactory.get_calculated_model(cursor.db_config) 
            query = get_calculated_model_query(cursor.calculated_model_folder_path,cursor.calculated_model_name)
            current_result = calculator.calculate(query,cursor)
            extractor = ExtractorFactory.get_extractor(model_bundle)
            loader = LoadFactory.get_loader(model_bundle["sink_info"])
            cursor_loader = LoadFactory.get_loader({
                'type': cursor.db_config.type,
                'db_config':cursor.db_config.model_dump(),
                'target_table': cursor.calculated_model_name,
                'schema': cursor.db_config.schema_name
            })
            first_chunk = True
            con = duckdb.connect(database=':memory:')
            base_table = model_bundle.get("base_table_name")
            for extraction_result in extractor.extract(current_result.at[0,cursor.calculated_model_column_name]):
                if len(extraction_result) > 0:
                    con.register(base_table, extraction_result)
                    result_reader = con.execute(model_bundle["sql_code"]).fetch_record_batch()
                    for batch in result_reader:
                        chunk_table = pa.Table.from_batches([batch])
                        loaded = loader.load(chunk_table, is_first_chunk=first_chunk)
                        loaded = True
                        if loaded:
                            if cursor.state == "STATEFULL":
                                cursor_table = pa.Table.from_pandas(current_result)
                                res = cursor_loader.load(cursor_table,is_first_chunk=True)

        if cursor.cursor_type == "SYNC":
            controller = True    
            calculator = CalculatedModelFactory.get_calculated_model(cursor.db_config) 
            query = get_calculated_model_query(cursor.calculated_model_folder_path,cursor.calculated_model_name)
            while controller:
                current_result = calculator.calculate(query,cursor)
           
                extractor = ExtractorFactory.get_extractor(model_bundle)
                loader = LoadFactory.get_loader(model_bundle["sink_info"])
                cursor_loader = LoadFactory.get_loader({
                    'type': cursor.db_config.type,
                    'db_config':cursor.db_config.model_dump(),
                    'target_table': cursor.calculated_model_name,
                    'schema': cursor.db_config.schema_name
                    })
                first_chunk = True
                con = duckdb.connect(database=':memory:')
                base_table = model_bundle.get("base_table_name")
                for extraction_result in extractor.extract(current_result.at[0,cursor.calculated_model_column_name]):
                    if len(extraction_result) > 0:
                        con.register(base_table, extraction_result)
                        result_reader = con.execute(model_bundle["sql_code"]).fetch_record_batch()
                        
                        for batch in result_reader:
                            chunk_table = pa.Table.from_batches([batch])
                            loaded = loader.load(chunk_table, is_first_chunk=first_chunk)
                            loaded = True
                            if loaded:
                                if cursor.state == "STATEFULL":
                                    cursor_table = pa.Table.from_pandas(current_result)
                                    res = cursor_loader.load(cursor_table,is_first_chunk=True)
                            else:
                                controller = False
                    else:
                        controller = False
                        
    else:
        try:
            extractor = ExtractorFactory.get_extractor(model_bundle)
            loader = LoadFactory.get_loader(model_bundle["sink_info"])
            
            source_type = model_bundle['source_config'].type
            first_chunk = True
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