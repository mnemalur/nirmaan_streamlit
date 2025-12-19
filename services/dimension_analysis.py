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
        Detect if Genie SQL returns medrec_key and/or pat_key by analyzing the SQL
        
        Args:
            genie_sql: SQL query from Genie
        
        Returns:
            Tuple of (has_medrec_key, has_pat_key)
        """
        sql_lower = genie_sql.lower()
        has_medrec = 'medrec' in sql_lower or 'med_rec' in sql_lower
        has_pat = 'pat_key' in sql_lower or ('pat' in sql_lower and 'key' in sql_lower)
        
        logger.info(f"Detected cohort structure from SQL - has_medrec: {has_medrec}, has_pat_key: {has_pat}")
        return has_medrec, has_pat
    
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
                'has_medrec_key': True/False,
            'has_pat_key': True/False
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
            df: DataFrame with medrec_key and/or pat_key columns
        
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
        if 'medrec_key' in df.columns and 'pat_key' in df.columns:
            # Already has correct column names
            cohort_df = df[['medrec_key', 'pat_key']].copy()
            has_medrec = True
        elif 'pat_key' in df.columns:
            # Only pat_key
            cohort_df = df[['pat_key']].copy()
            has_medrec = False
        else:
            # Need to find columns
            medrec_col = None
            pat_col = None
            
            for col in df.columns:
                col_lower = str(col).lower()
                if 'medrec' in col_lower or 'med_rec' in col_lower:
                    medrec_col = col
                if 'pat_key' in col_lower or (col_lower == 'pat' and 'key' in df.columns):
                    pat_col = col
                elif 'pat' in col_lower and 'key' in col_lower:
                    pat_col = col
            
            if not pat_col:
                raise ValueError(f"Could not find pat_key column in DataFrame. Columns: {list(df.columns)}")
            
            has_medrec = medrec_col is not None
            
            # Prepare DataFrame with only the keys we need
            if has_medrec:
                cohort_df = df[[medrec_col, pat_col]].copy()
                cohort_df.columns = ['medrec_key', 'pat_key']
            else:
                cohort_df = df[[pat_col]].copy()
                cohort_df.columns = ['pat_key']
        
        # Clean and prepare data
        cohort_df = cohort_df.dropna(subset=['pat_key'])  # Remove rows without pat_key
        if has_medrec:
            cohort_df['medrec_key'] = cohort_df['medrec_key'].astype('Int64')  # Nullable integer
        cohort_df['pat_key'] = cohort_df['pat_key'].astype('Int64')
        
        # Create table structure first
        if has_medrec:
            create_sql = f"""
            CREATE OR REPLACE TABLE {cohort_table_quoted}
            USING DELTA
            CLUSTER BY (medrec_key, pat_key)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 1 days'
            )
            AS
            SELECT 
                CAST(medrec_key AS BIGINT) AS medrec_key,
                CAST(pat_key AS BIGINT) AS pat_key
            FROM VALUES (CAST(NULL AS BIGINT), CAST(NULL AS BIGINT))
            WHERE 1=0
            """
        else:
            create_sql = f"""
            CREATE OR REPLACE TABLE {cohort_table_quoted}
            USING DELTA
            CLUSTER BY (pat_key)
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 1 days'
            )
            AS
            SELECT 
                CAST(pat_key AS BIGINT) AS pat_key
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
                    insert_sql = f"INSERT INTO {cohort_table} (medrec_key, pat_key) VALUES (?, ?)"
                    # Process in batches
                    for i in range(0, total_rows, batch_size):
                        batch_df = cohort_df.iloc[i:i+batch_size]
                        values = [
                            (int(row['medrec_key']) if pd.notna(row['medrec_key']) else None, 
                             int(row['pat_key'])) 
                            for _, row in batch_df.iterrows()
                        ]
                        cursor.executemany(insert_sql, values)
                        if (i + batch_size) % 50000 == 0:
                            logger.info(f"Inserted {min(i + batch_size, total_rows)}/{total_rows} rows")
                else:
                    insert_sql = f"INSERT INTO {cohort_table} (pat_key) VALUES (?)"
                    # Process in batches
                    for i in range(0, total_rows, batch_size):
                        batch_df = cohort_df.iloc[i:i+batch_size]
                        values = [(int(row['pat_key']),) for _, row in batch_df.iterrows()]
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
    
    def _execute_query(self, sql: str) -> List[Dict]:
        """
        Execute a SQL query and return results as list of dictionaries
        
        Args:
            sql: SQL query string
            
        Returns:
            List of dictionaries with query results
        """
        with connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=config.token,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
    
    def analyze_dimensions(
        self, 
        cohort_table: str, 
        has_medrec_key: bool = False,
        use_dynamic: bool = True
    ) -> Dict:
        """
        Analyze cohort across all dimensions in parallel
        
        Dimensions analyzed:
        - Age groups
        - Gender
        - Race
        - Ethnicity
        - Visit level (Inpatient/Outpatient)
        - Admit Source
        - Admit Type
        - Urban/Rural
        - Teaching status
        - Bed count groups
        
        Args:
            cohort_table: Full table name (catalog.schema.table) - will be quoted in SQL
            has_medrec_key: Whether cohort table has medrec_key column
            use_dynamic: If True, use dynamic schema discovery + LLM (default: True)
            
        Returns:
            Dictionary with dimension analysis results
        """
        # If dynamic mode requested, delegate to dynamic service
        if use_dynamic:
            try:
                # Try to use pre-initialized service from session state (if available)
                # This reuses the cached schema discovery
                dynamic_service = None
                if hasattr(self, '_cached_dynamic_service'):
                    dynamic_service = self._cached_dynamic_service
                else:
                    from services.dynamic_dimension_analysis import DynamicDimensionAnalysisService
                    dynamic_service = DynamicDimensionAnalysisService()
                
                return dynamic_service.analyze_dimensions_dynamically(
                    cohort_table=cohort_table,
                    has_medrec_key=has_medrec_key
                    # Note: No use_genie parameter - we use LLM in parallel, not Genie
                )
            except Exception as e:
                logger.warning(f"Dynamic dimension analysis failed, falling back to hardcoded: {str(e)}")
                # Fall through to hardcoded queries
        # Quote table name for SQL
        catalog_schema_table = cohort_table.split('.')
        if len(catalog_schema_table) == 3:
            cohort_table_quoted = f"`{catalog_schema_table[0]}`.`{catalog_schema_table[1]}`.`{catalog_schema_table[2]}`"
        else:
            cohort_table_quoted = cohort_table
        
        # Determine join key - cohort table uses medrec_key or pat_key
        # phd_de_patdemo table uses pat_key (or medrec_key if that's what cohort has)
        if has_medrec_key:
            cohort_join_key = "medrec_key"
            patdemo_join_key = "medrec_key"  # phd_de_patdemo may have medrec_key
        else:
            cohort_join_key = "pat_key"
            patdemo_join_key = "pat_key"  # phd_de_patdemo uses pat_key
        
        # Build all dimension queries
        queries = {}
        
        # 1. Age Groups
        queries['age_groups'] = f"""
            SELECT 
                CASE 
                    WHEN d.age < 18 THEN '<18'
                    WHEN d.age BETWEEN 18 AND 34 THEN '18-34'
                    WHEN d.age BETWEEN 35 AND 49 THEN '35-49'
                    WHEN d.age BETWEEN 50 AND 64 THEN '50-64'
                    WHEN d.age BETWEEN 65 AND 79 THEN '65-79'
                    ELSE '80+'
                END as age_group,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            WHERE d.age IS NOT NULL
            GROUP BY age_group
            ORDER BY 
                CASE age_group
                    WHEN '<18' THEN 1
                    WHEN '18-34' THEN 2
                    WHEN '35-49' THEN 3
                    WHEN '50-64' THEN 4
                    WHEN '65-79' THEN 5
                    WHEN '80+' THEN 6
                END
        """
        
        # 2. Gender
        queries['gender'] = f"""
            SELECT 
                CASE 
                    WHEN d.gender = 'M' THEN 'Male'
                    WHEN d.gender = 'F' THEN 'Female'
                    ELSE COALESCE(d.gender, 'Unknown')
                END as gender,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            GROUP BY gender
            ORDER BY patient_count DESC
        """
        
        # 3. Race
        queries['race'] = f"""
            SELECT 
                COALESCE(d.race, 'Unknown') as race,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            GROUP BY race
            ORDER BY patient_count DESC
        """
        
        # 4. Ethnicity
        queries['ethnicity'] = f"""
            SELECT 
                COALESCE(d.ethnicity, 'Unknown') as ethnicity,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            GROUP BY ethnicity
            ORDER BY patient_count DESC
        """
        
        # 5. Visit Level (Inpatient/Outpatient) - using phd_de_patdemo (I_O_IND column)
        # NOTE: phd_de_patdemo IS the encounter table - patient_key represents encounters/visits
        queries['visit_level'] = f"""
            SELECT 
                COALESCE(d.I_O_IND, 'Unknown') as visit_level,
                COUNT(DISTINCT d.{patdemo_join_key}) as encounter_count,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            WHERE d.I_O_IND IS NOT NULL
            GROUP BY visit_level
            ORDER BY encounter_count DESC
        """
        
        # 6. Admit Source - using phd_de_patdemo (PAT_TYPE column)
        queries['admit_source'] = f"""
            SELECT 
                COALESCE(d.PAT_TYPE, 'Unknown') as admit_source,
                COUNT(DISTINCT d.{patdemo_join_key}) as encounter_count,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            WHERE d.PAT_TYPE IS NOT NULL
            GROUP BY admit_source
            ORDER BY encounter_count DESC
        """
        
        # 7. Admit Type - using phd_de_patdemo (ADM_TYPE column)
        queries['admit_type'] = f"""
            SELECT 
                COALESCE(d.ADM_TYPE, 'Unknown') as admit_type,
                COUNT(DISTINCT d.{patdemo_join_key}) as encounter_count,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            WHERE d.ADM_TYPE IS NOT NULL
            GROUP BY admit_type
            ORDER BY encounter_count DESC
        """
        
        # 8. Urban/Rural (use phd_de_patdemo as bridge to provider table)
        queries['urban_rural'] = f"""
            SELECT 
                CASE 
                    WHEN p.location_type IN ('Urban', 'URBAN') THEN 'Urban'
                    WHEN p.location_type IN ('Rural', 'RURAL') THEN 'Rural'
                    ELSE COALESCE(p.location_type, 'Unknown')
                END as location_type,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            JOIN {config.patient_table_prefix}.provider p
                ON COALESCE(d.PROV_ID, d.PROVIDER_KEY) = COALESCE(p.PROV_ID, p.PROVIDER_KEY)
            GROUP BY location_type
            ORDER BY patient_count DESC
        """
        
        # 9. Teaching Status (use phd_de_patdemo as bridge to provider table)
        queries['teaching'] = f"""
            SELECT 
                CASE 
                    WHEN p.teaching_flag = 1 OR p.teaching_flag = 'Y' OR UPPER(p.teaching_flag) = 'YES' THEN 'Teaching'
                    WHEN p.teaching_flag = 0 OR p.teaching_flag = 'N' OR UPPER(p.teaching_flag) = 'NO' THEN 'Non-Teaching'
                    ELSE 'Unknown'
                END as teaching_status,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            JOIN {config.patient_table_prefix}.provider p
                ON COALESCE(d.PROV_ID, d.PROVIDER_KEY) = COALESCE(p.PROV_ID, p.PROVIDER_KEY)
            GROUP BY teaching_status
            ORDER BY patient_count DESC
        """
        
        # 10. Bed Count Groups (use phd_de_patdemo as bridge to provider table)
        queries['bed_count'] = f"""
            SELECT 
                CASE 
                    WHEN p.bed_count < 100 THEN '<100'
                    WHEN p.bed_count BETWEEN 100 AND 299 THEN '100-299'
                    WHEN p.bed_count BETWEEN 300 AND 499 THEN '300-499'
                    WHEN p.bed_count >= 500 THEN '500+'
                    ELSE 'Unknown'
                END as bed_count_group,
                COUNT(DISTINCT c.{cohort_join_key}) as patient_count,
                ROUND(COUNT(DISTINCT c.{cohort_join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{cohort_join_key})) OVER(), 2) as percentage
            FROM {cohort_table_quoted} c
            JOIN {config.patient_table_prefix}.phd_de_patdemo d 
                ON c.{cohort_join_key} = d.{patdemo_join_key}
            JOIN {config.patient_table_prefix}.provider p
                ON COALESCE(d.PROV_ID, d.PROVIDER_KEY) = COALESCE(p.PROV_ID, p.PROVIDER_KEY)
            WHERE p.bed_count IS NOT NULL
            GROUP BY bed_count_group
            ORDER BY 
                CASE bed_count_group
                    WHEN '<100' THEN 1
                    WHEN '100-299' THEN 2
                    WHEN '300-499' THEN 3
                    WHEN '500+' THEN 4
                    ELSE 5
                END
        """
        
        # Execute all queries in parallel
        results = {}
        errors = {}
        
        def execute_dimension_query(dim_name: str, sql: str):
            """Execute a single dimension query and return result"""
            try:
                result = self._execute_query(sql)
                return (dim_name, result, None)
            except Exception as e:
                logger.error(f"Error executing {dim_name} query: {str(e)}")
                return (dim_name, [], str(e))
        
        # Use ThreadPoolExecutor for parallel execution
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(execute_dimension_query, name, sql): name 
                      for name, sql in queries.items()}
            
            for future in as_completed(futures):
                dim_name, result, error = future.result()
                if error:
                    errors[dim_name] = error
                    results[dim_name] = []
                else:
                    results[dim_name] = result
        
        logger.info(f"Dimension analysis completed. Success: {len([r for r in results.values() if r])}, Errors: {len(errors)}")
        
        return {
            'dimensions': results,
            'errors': errors,
            'cohort_table': cohort_table
        }
