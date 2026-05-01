import pyarrow.csv as pv
import pyarrow as pa
from gojjam.ingest.extractors.base_extract import BaseExtractor

class CSVExtractor(BaseExtractor):
    def extract(self):

        file_path = self.model_info["source_config"].path
        try:
            print(f"📄 Reading CSV: {file_path}")
        
            with pv.open_csv(file_path) as reader:
                for batch in reader:
                    yield pa.Table.from_batches([batch])
                    
        except Exception as e:
            print(f"❌ CSV Extraction Failed: {e}")
            raise e