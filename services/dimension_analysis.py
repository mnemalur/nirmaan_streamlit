"""
Dimension Analysis Service
Creates efficient cohort temp tables and runs parallel dimension analysis queries
"""

from databricks.sql import connect
from config import config
from typing import Dict, List, Optional, Tuple
import time
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


class DimensionAnalysisService:
    """Service for creating cohort temp tables and running dimension analysis"""
    
    def __init__(self):
        """Initialize connection to SQL Warehouse"""
        if not config.host:
            raise ValueError("DATABRICKS_HOST is required")
        if not config.token:
            raise ValueError("DATABRICKS_TOKEN is required")
        if not config.warehouse_id:
            raise ValueError("SQL_WAREHOUSE_ID is required")
        
        self.server_hostname = config.host.replace("https://", "").replace("http://", "")
        self.http_path = f"/sql/1.0/warehouses/{config.warehouse_id}"
        
        # Get cohort table prefix (with fallback if config not set)
        try:
            self.cohort_table_prefix = config.cohort_table_prefix  # pasrt_uat.pas_temp_cohort
        except AttributeError:
            # Fallback if property doesn't exist
            cohort_catalog = getattr(config, 'cohort_catalog', 'pasrt_uat')
            cohort_schema = getattr(config, 'cohort_schema', 'pas_temp_cohort')
            self.cohort_table_prefix = f"{cohort_catalog}.{cohort_schema}"
        
    def detect_cohort_structure(self, genie_sql: str) -> Tuple[bool, bool]:
        """
        Detect if Genie SQL returns medrec_key and/or patient_key by analyzing the SQL
        
        Args:
            genie_sql: SQL query from Genie
        
        Returns:
            Tuple of (has_medrec_key, has_patient_key)
        """
        sql_lower = genie_sql.lower()
        has_medrec = 'medrec' in sql_lower or 'med_rec' in sql_lower
        has_patient = 'patient' in sql_lower and ('key' in sql_lower or 'id' in sql_lower)
        
        logger.info(f"Detected cohort structure from SQL - has_medrec: {has_medrec}, has_patient: {has_patient}")
        return has_medrec, has_patient
    
    def create_cohort_table_from_sql(self, session_id: str, genie_sql: str) -> Dict:
        """
        Create efficient temp table directly from Genie SQL in pasrt_uat.pas_temp_cohort
        
        This is much more efficient than extracting data - uses the full SQL query
        to create the table, getting the complete cohort, not just sample data.
        
        Optimizations:
        - Delta table format for better performance
        - Clustered on join keys (detected from SQL)
        - Auto-optimization enabled
        - Uses CREATE TABLE AS SELECT for efficiency
        
        Args:
            session_id: User session ID
            genie_sql: SQL query from Genie
        
        Returns:
            {
                'cohort_table': 'pasrt_uat.pas_temp_cohort.cohort_xyz',
                'cohort_id': 'cohort_xyz',
                'count': 1234,
                'has_medrec_key': True/False
            }
        """
        # Generate a simpler, readable table name
        # Format: cohort_YYYYMMDD_HHMMSS (e.g., cohort_20241217_143022)
        # This is more readable and reusable than UUID-based names
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cohort_id = f"cohort_{timestamp}"
        
        # Unity Catalog table names need backticks for safety (handles special chars like hyphens)
        # Format: `catalog`.`schema`.`table_name`
        catalog_schema = self.cohort_table_prefix.replace('.', '`.`')
        cohort_table_quoted = f"`{catalog_schema}`.`{cohort_id}`"  # For SQL execution
        cohort_table = f"{self.cohort_table_prefix}.{cohort_id}"  # For logging/display/return
        
        logger.info(f"Creating cohort table from Genie SQL: {cohort_table}")
        
        if not genie_sql:
            raise ValueError("Genie SQL is required")
        
        # Detect structure from SQL
        has_medrec, has_patient = self.detect_cohort_structure(genie_sql)
        
        # Build CREATE TABLE AS SELECT - Databricks SQL syntax
        # Format: CREATE OR REPLACE TABLE `catalog`.`schema`.`table` USING DELTA TBLPROPERTIES (...) AS SELECT ...
        # Unity Catalog table names need backticks to handle special characters
        create_sql = (
            f"CREATE OR REPLACE TABLE {cohort_table_quoted}\n"
            f"USING DELTA\n"
            f"TBLPROPERTIES (\n"
            f"  'delta.autoOptimize.optimizeWrite' = 'true',\n"
            f"  'delta.autoOptimize.autoCompact' = 'true',\n"
            f"  'delta.deletedFileRetentionDuration' = 'interval 1 days'\n"
            f")\n"
            f"AS\n"
            f"{genie_sql}"
        )
        
        # Execute table creation
        with connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=config.token,
        ) as conn:
            with conn.cursor() as cursor:
                # Ensure we're using the correct catalog
                catalog = config.cohort_catalog
                schema = config.cohort_schema
                logger.info(f"Using catalog: {catalog}, schema: {schema}")
                
                # Set catalog and schema context (if needed)
                try:
                    cursor.execute(f"USE CATALOG {catalog}")
                    cursor.execute(f"USE SCHEMA {schema}")
                    logger.info(f"Set catalog context to {catalog}.{schema}")
                except Exception as context_error:
                    logger.warning(f"Could not set catalog/schema context (may not be needed): {context_error}")
                
                logger.info(f"Executing CREATE TABLE AS SELECT for {cohort_table}")
                logger.info(f"SQL (first 1000 chars):\n{create_sql[:1000]}...")
                logger.info(f"Full SQL length: {len(create_sql)} chars")
                try:
                    cursor.execute(create_sql)
                except Exception as sql_error:
                    logger.error(f"SQL execution error: {sql_error}")
                    logger.error(f"Error type: {type(sql_error)}")
                    logger.error(f"Full SQL that failed:\n{create_sql}")
                    raise
                
                # Get count
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {cohort_table_quoted}")
                count = cursor.fetchone()[0]
                
                logger.info(f"Cohort table created: {cohort_table} with {count} rows")
                
                # Optionally add clustering after table creation (if supported)
                # This can be done later if needed for optimization
        
        # Return unquoted table name for easier use in subsequent queries
        # Callers should add backticks when using in SQL: `catalog`.`schema`.`table`
        return {
            'cohort_table': cohort_table,           # Return unquoted for display/logging
            'cohort_table_quoted': cohort_table_quoted,  # Return quoted for SQL execution
            'cohort_id': cohort_id,
            'count': count,
            'has_medrec_key': has_medrec
        }
    
    def create_cohort_table_from_dataframe(self, session_id: str, df: pd.DataFrame) -> Dict:
        """
        Alternative: Create cohort table from DataFrame (more efficient for large datasets)
        Uses INSERT INTO instead of VALUES clause
        
        Args:
            session_id: User session ID
            df: DataFrame with medrec_key and/or patient_key columns
        
        Returns:
            Same as create_cohort_table
        """
        # Use same simplified naming scheme as create_cohort_table_from_sql
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cohort_id = f"cohort_{timestamp}"
        
        # Unity Catalog table names need backticks for safety
        catalog_schema = self.cohort_table_prefix.replace('.', '`.`')
        cohort_table_quoted = f"`{catalog_schema}`.`{cohort_id}`"
        cohort_table = f"{self.cohort_table_prefix}.{cohort_id}"
        
        logger.info(f"Creating cohort table from DataFrame: {cohort_table}")
        
        # Check if DataFrame already has the correct column names (from create_cohort_table)
        # or if we need to find them (from direct DataFrame input)
        if 'medrec_key' in df.columns and 'patient_key' in df.columns:
            # Already has correct column names
            cohort_df = df[['medrec_key', 'patient_key']].copy()
            has_medrec = True
        elif 'patient_key' in df.columns:
            # Only patient_key
            cohort_df = df[['patient_key']].copy()
            has_medrec = False
        else:
            # Need to find columns
            medrec_col = None
            patient_col = None
            
            for col in df.columns:
                col_lower = str(col).lower()
                if 'medrec' in col_lower or 'med_rec' in col_lower:
                    medrec_col = col
                if 'patient' in col_lower and ('key' in col_lower or 'id' in col_lower):
                    patient_col = col
            
            if not patient_col:
                raise ValueError(f"Could not find patient_key column in DataFrame. Columns: {list(df.columns)}")
            
            has_medrec = medrec_col is not None
            
            # Prepare DataFrame with only the keys we need
            if has_medrec:
                cohort_df = df[[medrec_col, patient_col]].copy()
                cohort_df.columns = ['medrec_key', 'patient_key']
            else:
                cohort_df = df[[patient_col]].copy()
                cohort_df.columns = ['patient_key']
        
        # Clean and prepare data
        cohort_df = cohort_df.dropna(subset=['patient_key'])  # Remove rows without patient_key
        if has_medrec:
            cohort_df['medrec_key'] = cohort_df['medrec_key'].astype('Int64')  # Nullable integer
        cohort_df['patient_key'] = cohort_df['patient_key'].astype('Int64')
        
        # Create table structure first
        if has_medrec:
            create_sql = f"""
            CREATE OR REPLACE TABLE {cohort_table_quoted}
            USING DELTA
            CLUSTER BY (medrec_key, patient_key)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 1 days'
            )
            AS
            SELECT 
                CAST(medrec_key AS BIGINT) AS medrec_key,
                CAST(patient_key AS BIGINT) AS patient_key
            FROM VALUES (CAST(NULL AS BIGINT), CAST(NULL AS BIGINT))
            WHERE 1=0
            """
        else:
            create_sql = f"""
            CREATE OR REPLACE TABLE {cohort_table_quoted}
            USING DELTA
            CLUSTER BY (patient_key)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 1 days'
            )
            AS
            SELECT 
                CAST(patient_key AS BIGINT) AS patient_key
            FROM VALUES (CAST(NULL AS BIGINT))
            WHERE 1=0
            """
        
        with connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=config.token,
        ) as conn:
            with conn.cursor() as cursor:
                # Create empty table
                logger.info(f"Creating table structure: {cohort_table}")
                cursor.execute(create_sql)
                
                # For large datasets, use batch INSERT with optimal batch size
                logger.info(f"Inserting {len(cohort_df)} rows into {cohort_table}")
                
                batch_size = 10000  # Optimal batch size for Databricks
                total_rows = len(cohort_df)
                
                if has_medrec:
                    insert_sql = f"INSERT INTO {cohort_table} (medrec_key, patient_key) VALUES (?, ?)"
                    # Process in batches
                    for i in range(0, total_rows, batch_size):
                        batch_df = cohort_df.iloc[i:i+batch_size]
                        values = [
                            (int(row['medrec_key']) if pd.notna(row['medrec_key']) else None, 
                             int(row['patient_key'])) 
                            for _, row in batch_df.iterrows()
                        ]
                        cursor.executemany(insert_sql, values)
                        if (i + batch_size) % 50000 == 0:
                            logger.info(f"Inserted {min(i + batch_size, total_rows)}/{total_rows} rows")
                else:
                    insert_sql = f"INSERT INTO {cohort_table} (patient_key) VALUES (?)"
                    # Process in batches
                    for i in range(0, total_rows, batch_size):
                        batch_df = cohort_df.iloc[i:i+batch_size]
                        values = [(int(row['patient_key']),) for _, row in batch_df.iterrows()]
                        cursor.executemany(insert_sql, values)
                        if (i + batch_size) % 50000 == 0:
                            logger.info(f"Inserted {min(i + batch_size, total_rows)}/{total_rows} rows")
                
                conn.commit()
                logger.info(f"Completed inserting {total_rows} rows")
                
                # Get count
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {cohort_table_quoted}")
                count = cursor.fetchone()[0]
                
                logger.info(f"Cohort table created: {cohort_table} with {count} rows")
        
        return {
            'cohort_table': cohort_table,
            'cohort_id': cohort_id,
            'count': count,
            'has_medrec_key': has_medrec
        }
