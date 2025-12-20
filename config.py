"""
Configuration management for Clinical Cohort Assistant
Loads environment variables and provides configuration object
Automatically detects Databricks runtime and uses workspace context
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Detect if running in Databricks runtime
try:
    from pyspark.dbutils import DBUtils
    from pyspark import SparkContext
    _spark_context = SparkContext.getOrCreate()
    _dbutils = DBUtils(_spark_context)
    IN_DATABRICKS = True
except:
    IN_DATABRICKS = False
    _dbutils = None
    _spark_context = None


@dataclass
class DatabricksConfig:
    # Databricks Connection
    host: str = None
    token: str = None
    
    # Genie Space
    space_id: str = None
    
    # Patient Data
    patient_catalog: str = None
    patient_schema: str = None
    
    # Vector Search
    vector_catalog: str = None
    vector_schema: str = None
    vector_function: str = "standard_code_lookup"  # Default fallback
    
    # Cohort Tables (for dimension analysis)
    cohort_catalog: str = "pasrt_uat"  # Default to pasrt_uat
    cohort_schema: str = "pas_temp_cohort"  # Default schema for temp cohorts
    
    # SQL Warehouse
    warehouse_id: str = None
    
    def __post_init__(self):
        """Initialize from Databricks workspace or environment variables"""
        if IN_DATABRICKS:
            self._init_from_databricks()
        else:
            self._init_from_env()
    
    def _init_from_databricks(self):
        """Initialize from Databricks workspace context (no explicit token needed)"""
        try:
            # Get workspace URL from Spark context or environment
            if _spark_context:
                spark_conf = _spark_context.getConf()
                # Try to get workspace URL from Spark config
                workspace_url = spark_conf.get("spark.databricks.workspaceUrl", None)
                if workspace_url:
                    self.host = f"https://{workspace_url}" if not workspace_url.startswith("http") else workspace_url
                else:
                    self.host = os.getenv("DATABRICKS_HOST")
            else:
                self.host = os.getenv("DATABRICKS_HOST")
            
            # In Databricks runtime, token is handled automatically by WorkspaceClient
            # Only set token if explicitly provided (for cross-workspace access)
            self.token = os.getenv("DATABRICKS_TOKEN")  # Can be None - WorkspaceClient will use runtime auth
            
            # Try to get config from dbutils.secrets first, then environment
            if _dbutils:
                try:
                    self.space_id = _dbutils.secrets.get(scope="tokens", key="genie_space_id")
                except:
                    self.space_id = os.getenv("GENIE_SPACE_ID")
                
                try:
                    self.patient_catalog = _dbutils.secrets.get(scope="tokens", key="patient_catalog")
                except:
                    self.patient_catalog = os.getenv("PATIENT_CATALOG")
                
                try:
                    self.patient_schema = _dbutils.secrets.get(scope="tokens", key="patient_schema")
                except:
                    self.patient_schema = os.getenv("PATIENT_SCHEMA")
                
                try:
                    self.vector_schema = _dbutils.secrets.get(scope="tokens", key="vector_schema")
                except:
                    self.vector_schema = os.getenv("VECTOR_SCHEMA")
                
                try:
                    self.warehouse_id = _dbutils.secrets.get(scope="tokens", key="warehouse_id")
                except:
                    self.warehouse_id = os.getenv("SQL_WAREHOUSE_ID")
            else:
                # Fallback to environment variables
                self.space_id = os.getenv("GENIE_SPACE_ID")
                self.patient_catalog = os.getenv("PATIENT_CATALOG")
                self.patient_schema = os.getenv("PATIENT_SCHEMA")
                self.vector_schema = os.getenv("VECTOR_SCHEMA")
                self.warehouse_id = os.getenv("SQL_WAREHOUSE_ID")
            
            # Vector catalog defaults to patient catalog if not set
            self.vector_catalog = os.getenv("VECTOR_CATALOG") or self.patient_catalog
            
        except Exception as e:
            # Fallback to environment variables if Databricks init fails
            self._init_from_env()
    
    def _init_from_env(self):
        """Initialize from environment variables (local development)"""
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.space_id = os.getenv("GENIE_SPACE_ID")
        self.patient_catalog = os.getenv("PATIENT_CATALOG")
        self.patient_schema = os.getenv("PATIENT_SCHEMA")
        self.vector_catalog = os.getenv("VECTOR_CATALOG") or os.getenv("PATIENT_CATALOG")
        self.vector_schema = os.getenv("VECTOR_SCHEMA")
        self.vector_function = os.getenv("VECTOR_FUNCTION", "standard_code_lookup")
        self.cohort_catalog = os.getenv("COHORT_CATALOG", "pasrt_uat")
        self.cohort_schema = os.getenv("COHORT_SCHEMA", "pas_temp_cohort")
        self.warehouse_id = os.getenv("SQL_WAREHOUSE_ID")
    
    @property
    def patient_table_prefix(self) -> str:
        """Returns catalog.schema format for patient tables"""
        return f"{self.patient_catalog}.{self.patient_schema}"
    
    @property
    def vector_function_fqn(self) -> str:
        """Returns fully qualified name for vector function"""
        return f"{self.vector_catalog}.{self.vector_schema}.{self.vector_function}"
    
    @property
    def cohort_table_prefix(self) -> str:
        """Returns catalog.schema format for cohort tables"""
        return f"{self.cohort_catalog}.{self.cohort_schema}"
    
    @property
    def is_databricks_runtime(self) -> bool:
        """Check if running in Databricks runtime"""
        return IN_DATABRICKS


# Create global config instance
config = DatabricksConfig()

