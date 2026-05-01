from abc import ABC, abstractmethod
from typing import Any

class BaseTransformEngine(ABC):
    """
    The abstract base class for all Gojjam transformation engines.
    Every database (Postgres, Snowflake, BigQuery) must implement these methods.
    """

    def __init__(self, db_config: Any):
        self.config = db_config
        self.conn = None

    @abstractmethod
    def connect(self) -> Any:
        """
        Establish and return a connection to the database.
        Should handle its own internal connection pooling or singleton logic.
        """
        pass

    @abstractmethod
    def get_materialization_sql(self, model_name: str, sql_content: str) -> str:
        """
        Takes the raw SQL and returns the DDL (Data Definition Language) 
        required to wrap it in a stored procedure for that specific database.
        """
        pass

    @abstractmethod
    def register_procedure(self, name: str, sql_content: str):
        """The DB-specific way to create/replace a procedure."""
        pass

    @abstractmethod
    def execute_procedure(self, name: str):
        """The DB-specific way to CALL/EXEC a procedure."""
        pass

    def close(self):
        """Standard cleanup method to close the connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()