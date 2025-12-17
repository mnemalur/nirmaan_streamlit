"""
Databricks-optimized Configuration
Automatically uses workspace context when running inside Databricks
"""

import os
from dataclasses import dataclass

# Try to import dbutils (only available in Databricks runtime)
try:
    from pyspark.dbutils import DBUtils
    from pyspark import SparkContext
    _spark_context = SparkContext.getOrCreate()
    _dbutils = DBUtils(_spark_context)
    IN_DATABRICKS = True
except:
    IN_DATABRICKS = False
    _dbutils = None


@dataclass
class DatabricksConfig:
    """Configuration that works both locally and in Databricks"""
    
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
    vector_function: str = "standard_code_lookup"
    
    # SQL Warehouse
    warehouse_id: str = None
    
    def __post_init__(self):
        """Initialize from environment or Databricks workspace"""
        if IN_DATABRICKS:
            self._init_from_databricks()
        else:
            self._init_from_env()
    
    def _init_from_databricks(self):
        """Initialize from Databricks workspace context"""
        try:
            # Get workspace URL from Spark context
            spark_conf = _spark_context.getConf()
            self.host = spark_conf.get("spark.databricks.workspaceUrl", os.getenv("DATABRICKS_HOST"))
            
            # Token is automatically handled by Databricks runtime
            # Use dbutils.secrets for secure token storage if needed
            try:
                self.token = _dbutils.secrets.get(scope="tokens", key="databricks_token")
            except:
                # Fallback to environment variable
                self.token = os.getenv("DATABRICKS_TOKEN")
            
            # Get config from Spark conf or environment
            self.space_id = spark_conf.get("spark.databricks.genie.spaceId", os.getenv("GENIE_SPACE_ID"))
            self.patient_catalog = spark_conf.get("spark.databricks.patient.catalog", os.getenv("PATIENT_CATALOG", "main"))
            self.patient_schema = spark_conf.get("spark.databricks.patient.schema", os.getenv("PATIENT_SCHEMA", "clinical"))
            self.vector_catalog = spark_conf.get("spark.databricks.vector.catalog", os.getenv("VECTOR_CATALOG", self.patient_catalog))
            self.vector_schema = spark_conf.get("spark.databricks.vector.schema", os.getenv("VECTOR_SCHEMA"))
            self.warehouse_id = spark_conf.get("spark.databricks.warehouse.id", os.getenv("SQL_WAREHOUSE_ID"))
            
        except Exception as e:
            # Fallback to environment variables
            self._init_from_env()
    
    def _init_from_env(self):
        """Initialize from environment variables (local or manual config)"""
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.space_id = os.getenv("GENIE_SPACE_ID")
        self.patient_catalog = os.getenv("PATIENT_CATALOG")
        self.patient_schema = os.getenv("PATIENT_SCHEMA")
        self.vector_catalog = os.getenv("VECTOR_CATALOG") or self.patient_catalog
        self.vector_schema = os.getenv("VECTOR_SCHEMA")
        self.warehouse_id = os.getenv("SQL_WAREHOUSE_ID")
    
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



