# Gojjam

**The Lightweight, SQL-First Data Engine for Ingestion and Transformation.**

![Status](https://img.shields.io/badge/status-MVP%20%2F%20Alpha-orange)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

> **⚠️ Work in Progress:** Gojjam is currently in **MVP/Alpha**. The core engine is stable, but connector features are limited. Not yet recommended for production-critical workloads.

Gojjam is a high-performance data orchestration engine designed to decouple **Ingestion** from **Transformation**. Built on the modern data stack (Apache Arrow, DuckDB, and Pydantic V2), it allows developers to build production-grade data pipelines using simple YAML configurations and pure SQL models.

### 🚀 Why Gojjam?

Most data tools force you into a monolith. Gojjam splits the workload:

- **Decoupled Ingest:** Land data from APIs, DBs, or S3 into your warehouse without writing Python boilerplate.
- **SQL-First Transform:** Execute high-performance transformations using SQL-pushdown logic.
- **Local-First DX:** A CLI-driven experience that works as well on your laptop as it does in production.

---

## 🛠️ Installation

```bash
pip install gojjam
```

---

## 📖 Quick Start

Gojjam includes a built-in scaffolding engine to get you running in seconds.

### 1. Initialize a Project

```bash
gojjam init
```

This creates a partitioned project structure:

- `gojjam_ingest_sources.yml`: Define your data sources.
- `gojjam_ingest_sinks.yml`: Define where your data lands.
- `ingest/`: Put your raw extraction SQL here.
- `transform/`: Put your warehouse transformation SQL here.

### 2. Run the Pipeline

```bash
# Run all ingest and transformation tasks
gojjam run --all
```

---

## 💡 The Gojjam Philosophy

Gojjam treats **APIs as virtual SQL tables**. Instead of writing complex Python scripts to handle pagination and nested JSON, you define the schema and let the engine handle the heavy lifting. This allows data engineers to focus on the **logic** of the data rather than the **plumbing** of the connection.

---

## 🔌 Supported Connectors

Gojjam is designed for extensibility. We currently support the following extraction and loading modules:

| Connector            | Type             | Status / Description                                                              |
| :------------------- | :--------------- | :-------------------------------------------------------------------------------- |
| **HTTP / REST**      | Extractor        | **Alpha:** Supports JSON payloads via Basic Auth. (JWT & Pagination coming soon). |
| **PostgreSQL**       | Extractor/Loader | **Stable:** High-speed relational data movement via `psycopg2`.                   |
| **CSV / Flat Files** | Extractor        | **Stable:** Local-first data ingestion for quick analysis.                        |
| **DuckDB**           | Extractor        | **Stable:** In-process analytical extraction for fast prototyping.                |
| **Terminal**         | Loader           | **Stable:** Debug mode: stream results directly to your console.                  |
