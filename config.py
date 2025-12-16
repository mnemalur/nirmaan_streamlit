"""
Configuration management for Clinical Cohort Assistant
Loads environment variables and provides configuration object
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabricksConfig:
    # Databricks Connection
    host: str = os.getenv("DATABRICKS_HOST")
    token: str = os.getenv("DATABRICKS_TOKEN")
    
    # Genie Space
    space_id: str = os.getenv("GENIE_SPACE_ID")
    
    # Patient Data
    patient_catalog: str = os.getenv("PATIENT_CATALOG")
    patient_schema: str = os.getenv("PATIENT_SCHEMA")
    
    # Vector Search
    vector_catalog: str = os.getenv("VECTOR_CATALOG") or os.getenv("PATIENT_CATALOG")
    vector_schema: str = os.getenv("VECTOR_SCHEMA")
    vector_function: str = os.getenv("VECTOR_FUNCTION", "standard_code_lookup")  # Default fallback
    
    # SQL Warehouse
    warehouse_id: str = os.getenv("SQL_WAREHOUSE_ID")
    
    @property
    def patient_table_prefix(self) -> str:
        """Returns catalog.schema format for patient tables"""
        return f"{self.patient_catalog}.{self.patient_schema}"
    
    @property
    def vector_function_fqn(self) -> str:
        """Returns fully qualified name for vector function"""
        return f"{self.vector_catalog}.{self.vector_schema}.{self.vector_function}"


# Create global config instance
config = DatabricksConfig()

