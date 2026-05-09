import json
import pandas as pd
import pyarrow as pa
from azure.storage.blob import BlobServiceClient
from gojjam.ingest.extractors.base_extract import BaseExtractor

class AzureBlobStorageExtractor(BaseExtractor):
    def extract(self):
        db_conf = self.model_info["source_config"]
        root_folder = db_conf.root_folder
        connection_string = db_conf.azure_blob_storage_connection_string
        container_name = db_conf.container_name

        batch_size = 2000 
        pending_records = []

        client = BlobServiceClient.from_connection_string(connection_string)
        
        try:
            container_client = client.get_container_client(container_name)
            
    
            blobs = container_client.list_blobs(name_starts_with=root_folder)

            for blob in blobs:

                if blob.size == 0:
                    continue

                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob().readall()
                data = json.loads(stream)


                if isinstance(data, dict):
                    records = [data]
                elif isinstance(data, list):
                    records = data
                else:
                    continue

                pending_records.extend(records)

        
                while len(pending_records) >= batch_size:
                    batch = pending_records[:batch_size]
                    pending_records = pending_records[batch_size:]
                    
                    batch_df = pd.DataFrame(batch)
                    yield pa.Table.from_pandas(batch_df)

            if pending_records:
                yield pa.Table.from_pandas(pd.DataFrame(pending_records))

        except Exception as e:
            print(f"❌ Azure Blob Extraction Failed")
            raise e