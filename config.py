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
        """
        Initialize from .env file first, then fall back to Databricks workspace context.
        Priority: .env file > dbutils.secrets > Spark config > defaults
        """
        try:
            # Priority 1: Always check .env file first (already loaded via load_dotenv())
            # Priority 2: Try dbutils.secrets if .env doesn't have it
            # Priority 3: Try Spark config
            # Priority 4: Use defaults
            
            # DATABRICKS_HOST: .env > Spark config
            self.host = os.getenv("DATABRICKS_HOST")
            if not self.host and _spark_context:
                spark_conf = _spark_context.getConf()
                workspace_url = spark_conf.get("spark.databricks.workspaceUrl", None)
                if workspace_url:
                    self.host = f"https://{workspace_url}" if not workspace_url.startswith("http") else workspace_url
            
            # DATABRICKS_TOKEN: .env > dbutils.secrets (optional in Databricks runtime)
            self.token = os.getenv("DATABRICKS_TOKEN")
            if not self.token and _dbutils:
                try:
                    self.token = _dbutils.secrets.get(scope="tokens", key="databricks_token")
                except:
                    pass  # Token is optional in Databricks runtime
            
            # GENIE_SPACE_ID: .env > dbutils.secrets
            self.space_id = os.getenv("GENIE_SPACE_ID")
            if not self.space_id and _dbutils:
                try:
                    self.space_id = _dbutils.secrets.get(scope="tokens", key="genie_space_id")
                except:
                    pass
            
            # PATIENT_CATALOG: .env > dbutils.secrets > default
            self.patient_catalog = os.getenv("PATIENT_CATALOG")
            if not self.patient_catalog and _dbutils:
                try:
                    self.patient_catalog = _dbutils.secrets.get(scope="tokens", key="patient_catalog")
                except:
                    pass
            
            # PATIENT_SCHEMA: .env > dbutils.secrets
            self.patient_schema = os.getenv("PATIENT_SCHEMA")
            if not self.patient_schema and _dbutils:
                try:
                    self.patient_schema = _dbutils.secrets.get(scope="tokens", key="patient_schema")
                except:
                    pass
            
            # VECTOR_SCHEMA: .env > dbutils.secrets
            self.vector_schema = os.getenv("VECTOR_SCHEMA")
            if not self.vector_schema and _dbutils:
                try:
                    self.vector_schema = _dbutils.secrets.get(scope="tokens", key="vector_schema")
                except:
                    pass
            
            # VECTOR_CATALOG: .env > PATIENT_CATALOG
            self.vector_catalog = os.getenv("VECTOR_CATALOG") or self.patient_catalog
            
            # VECTOR_FUNCTION: .env > default
            self.vector_function = os.getenv("VECTOR_FUNCTION", "standard_code_lookup")
            
            # COHORT_CATALOG: .env > default
            self.cohort_catalog = os.getenv("COHORT_CATALOG", "pasrt_uat")
            
            # COHORT_SCHEMA: .env > default
            self.cohort_schema = os.getenv("COHORT_SCHEMA", "pas_temp_cohort")
            
            # SQL_WAREHOUSE_ID: .env > dbutils.secrets
            self.warehouse_id = os.getenv("SQL_WAREHOUSE_ID")
            if not self.warehouse_id and _dbutils:
                try:
                    self.warehouse_id = _dbutils.secrets.get(scope="tokens", key="warehouse_id")
                except:
                    pass
            
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

