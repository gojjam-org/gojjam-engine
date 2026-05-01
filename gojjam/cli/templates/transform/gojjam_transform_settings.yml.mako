version: "1.0"
config:
  - name: "main_warehouse_transform"
    source_folder: "./transform"
    config:
      ref: "sinks.postgres_silver"
