import json
import boto3
import pandas as pd
import pyarrow as pa
from gojjam.ingest.extractors.base_extract import BaseExtractor

class S3Extractor(BaseExtractor):
    def extract(self):
        db_conf = self.model_info["source_config"]
        bucket_name = db_conf.bucket_name
        root_folder = db_conf.root_folder  
        
        batch_size = 2000 
        pending_records = []

        s3_client = boto3.client(
            's3',
            aws_access_key_id=db_conf.aws_access_key_id,
            aws_secret_access_key=db_conf.aws_secret_access_key,
            endpoint_url=str(getattr(db_conf, "endpoint", None)),
            region_name=getattr(db_conf, "region", "us-east-1")
        )
        
       
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=root_folder)
            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    if obj["Size"] == 0 or obj["Key"].endswith("/"):
                        continue

                    response = s3_client.get_object(Bucket=bucket_name, Key=obj["Key"])
                    content = response['Body'].read().decode('utf-8')
                    data = json.loads(content)

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
            print(f"❌ S3 Extraction Failed")
            raise e