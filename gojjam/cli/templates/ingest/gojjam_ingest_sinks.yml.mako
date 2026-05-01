version: "1.0"
sinks:
  - name: "postgres_silver"
    type: "postgres"
    source_folder: "./ingest"
    config:
      host: "localhost"
      port: 5432
      database: "warehouse"
      user: "admin"
      password: "password"
      schema: "public"