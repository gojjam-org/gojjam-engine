import pyarrow as pa
from pathlib import Path
from abc import ABC,abstractmethod
from gojjam.calculated_model.calculated_model_factory import CalculatedModelFactory
from gojjam.ingest.extractors.extractor_factory import ExtractorFactory
from gojjam.ingest.loaders.load_factory import LoadFactory

class CursorIngestEngine(ABC):
 
    def __init__(self,model):
        self.model = model
        self.cursor = self.model["source_config"].cursor
        self.calculator = CalculatedModelFactory.get_calculated_model(self.cursor.db_config)
        if self.cursor.state == "STATEFULL":
            self.cursor_loader = LoadFactory.get_loader({
                'type': self.cursor.db_config.type,
                'db_config':self.cursor.db_config.model_dump(),
                'target_table': self.cursor.calculated_model_name,
                'schema': self.cursor.db_config.schema_name
            })
       
    def get_calculated_model_query(self,folder_path, file_name):

        if not file_name.endswith(".sql"):
            file_name = f"{file_name}.sql"

        file_path = Path(folder_path) / file_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"Query file not found at: {file_path}")
    
        with open(file_path, "r", encoding="utf-8") as f:
            query = f.read().strip()
            
        return query

    def update_cursor(self,current_result):
        if self.cursor.state == "STATEFULL":
            cursor_table = pa.Table.from_pandas(current_result)
            return self.cursor_loader.load(cursor_table,is_first_chunk=True)
        else:
            pass


    

