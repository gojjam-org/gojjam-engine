import pyarrow as pa
from gojjam.ingest.loaders.base_loader import BaseLoader

class TerminalLoader(BaseLoader):
    def __init__(self, sink_config):
        super().__init__(sink_config)
        self.total_rows = 0

    def load(self, data: pa.Table, is_first_chunk: bool = True):
        if is_first_chunk:
            print("\n" + "═"*60)
            print(f"🖥️  TERMINAL STREAM: {self.sink_config.get('name', 'Gojjam Output')}")
            print("═"*60)
            self.total_rows = 0

        if data.num_rows == 0:
            if is_first_chunk:
                print("⚠️  The resulting dataset is empty.")
            return

        df_chunk = data.to_pandas()
        
        self.total_rows += len(df_chunk)
        print(df_chunk.to_string(index=False, header=is_first_chunk))
        
        print(f"--- Chunk Processed: {len(df_chunk)} rows (Total so far: {self.total_rows}) ---")

