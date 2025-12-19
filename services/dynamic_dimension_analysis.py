"""
Dynamic Dimension Analysis Service
Uses schema discovery + LLM to generate dimension queries dynamically in parallel
Avoids Genie's linear limitation by generating all SQL queries upfront, then executing in parallel
"""

from typing import Dict, List, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.schema_discovery import SchemaDiscoveryService
from services.dimension_analysis import DimensionAnalysisService
from services.intent_service import IntentService
from services.sql_validator import SQLValidator
from config import config

logger = logging.getLogger(__name__)


class DynamicDimensionAnalysisService:
    """
    Service that dynamically generates dimension analysis queries using:
    1. Schema discovery to understand available tables/columns (cached)
    2. LLM to generate ALL SQL queries in parallel (not Genie - Genie is linear)
    3. Execute all queries in parallel using ThreadPoolExecutor
    """
    
    def __init__(self):
        self.schema_service = SchemaDiscoveryService()
        self.dimension_service = DimensionAnalysisService()
        self.intent_service = IntentService()
        self.sql_validator = SQLValidator()
        self._schema_cache = {}  # Cache schema info to avoid repeated discovery
        self._schema_context_cache = {}  # Cache formatted schema context
        self._exact_column_cache = {}  # Cache exact column names for each dimension
    
    def get_schema_context(self, catalog: str, schema: str, dimension_name: Optional[str] = None, use_cache: bool = True) -> str:
        """
        Get schema context for LLM (cached for performance)
        
        Args:
            catalog: Catalog name
            schema: Schema name
            dimension_name: Optional dimension name for table recommendations
            use_cache: Whether to use cached schema info
            
        Returns:
            Formatted schema context string
        """
        # For dimension-specific context, don't use cache (needs recommendations)
        if dimension_name:
            return self.schema_service.get_schema_context_for_llm(catalog, schema, dimension_name)
        
        # For general context, use cache
        cache_key = f"{catalog}.{schema}"
        
        if use_cache and cache_key in self._schema_context_cache:
            logger.info(f"Using cached schema context for {cache_key}")
            return self._schema_context_cache[cache_key]
        
        schema_context = self.schema_service.get_schema_context_for_llm(catalog, schema)
        
        if use_cache:
            self._schema_context_cache[cache_key] = schema_context
            logger.info(f"Cached schema context for {cache_key}")
        
        return schema_context
    
    def generate_dimension_query_with_llm_parallel(
        self,
        dimension_spec: Dict,
        cohort_table: str,
        schema_context: str,
        has_medrec_key: bool = False
    ) -> Tuple[str, Optional[str]]:
        """
        Generate a single dimension SQL query using LLM (designed for parallel execution)
        
        Args:
            dimension_spec: Dictionary with 'name' and 'description' keys
            cohort_table: Full cohort table name
            schema_context: Pre-discovered schema context string
            has_medrec_key: Whether cohort has medrec_key column
            
        Returns:
            Tuple of (dimension_name, sql_query) or (dimension_name, None) if failed
        """
        dimension_name = dimension_spec['name']
        dimension_description = dimension_spec['description']
        
        join_key = "medrec_key" if has_medrec_key else "pat_key"
        
        # Quote cohort table name
        catalog_schema_table = cohort_table.split('.')
        if len(catalog_schema_table) == 3:
            cohort_table_quoted = f"`{catalog_schema_table[0]}`.`{catalog_schema_table[1]}`.`{catalog_schema_table[2]}`"
        else:
            cohort_table_quoted = cohort_table
        
        # Get dimension-specific schema context with table recommendations
        dimension_schema_context = self.get_schema_context(
            config.patient_catalog,
            config.patient_schema,
            dimension_name=dimension_name,
            use_cache=False  # Don't cache dimension-specific context
        )
        
        # Get table recommendations for this dimension
        dimension_table_mapping = self.schema_service.get_dimension_table_mapping(
            config.patient_catalog,
            config.patient_schema
        )
        recommended_tables = dimension_table_mapping.get(dimension_name, [])
        
        # Get EXACT column names from actual tables (handles BED_GRP vs BED_COUNT, etc.)
        # Use cache if available, otherwise discover and cache
        cache_key = f"{config.patient_catalog}.{config.patient_schema}.{dimension_name}"
        if cache_key in self._exact_column_cache:
            exact_columns = self._exact_column_cache[cache_key]
            logger.info(f"Using cached exact columns for {dimension_name}: {exact_columns}")
        else:
            exact_columns = self.schema_service.get_exact_column_names_for_dimension(
                config.patient_catalog,
                config.patient_schema,
                dimension_name
            )
            self._exact_column_cache[cache_key] = exact_columns
            logger.info(f"Cached exact columns for {dimension_name}: {exact_columns}")
        
        prompt = f"""You are a SQL expert for Databricks Unity Catalog. Generate a SQL query for dimension analysis.

Dimension: {dimension_name}
Description: {dimension_description}

Cohort Table: {cohort_table_quoted}
Join Key: {join_key}
Patient Schema: {config.patient_catalog}.{config.patient_schema}

⚠️ CRITICAL: Table Selection Rules:
"""
        
        # SIMPLIFIED: Explicit table mapping with EXACT column names
        if dimension_name in ['age_groups', 'gender', 'race', 'ethnicity', 'visit_level', 'admit_source', 'admit_type']:
            prompt += f"⚠️ **CRITICAL**: For '{dimension_name}', you **MUST use phd_de_patdemo table**\n"
            prompt += f"- Table: {config.patient_catalog}.{config.patient_schema}.phd_de_patdemo (use alias 'd')\n"
            # Use exact column names if discovered
            if exact_columns:
                if 'age_column' in exact_columns:
                    prompt += f"- **EXACT column for age**: d.{exact_columns['age_column']}\n"
                if 'gender_column' in exact_columns:
                    prompt += f"- **EXACT column for gender**: d.{exact_columns['gender_column']}\n"
                if 'race_column' in exact_columns:
                    prompt += f"- **EXACT column for race**: d.{exact_columns['race_column']}\n"
                if 'ethnicity_column' in exact_columns:
                    prompt += f"- **EXACT column for ethnicity**: d.{exact_columns['ethnicity_column']}\n"
                if 'visit_level_column' in exact_columns:
                    prompt += f"- **EXACT column for visit_level**: d.{exact_columns['visit_level_column']}\n"
                if 'admit_source_column' in exact_columns:
                    prompt += f"- **EXACT column for admit_source**: d.{exact_columns['admit_source_column']}\n"
                if 'admit_type_column' in exact_columns:
                    prompt += f"- **EXACT column for admit_type**: d.{exact_columns['admit_type_column']}\n"
            else:
                # Fallback to generic names if discovery failed
                if dimension_name in ['age_groups', 'gender', 'race', 'ethnicity']:
                    prompt += f"- Columns: age, gender, race, ethnicity\n"
                else:
                    prompt += f"- Columns: visit_type, visit_level, admit_source, admit_type\n"
        elif dimension_name in ['urban_rural', 'teaching', 'bed_count']:
            prompt += f"⚠️ **CRITICAL**: For '{dimension_name}', you **MUST use TWO JOINS** (bridge pattern):\n"
            prompt += f"1. First join cohort → phd_de_patdemo: ON c.{join_key} = d.{join_key} (alias 'd')\n"
            # Use exact prov_id column if discovered
            prov_id_col = exact_columns.get('prov_id_column', 'prov_id')
            prompt += f"2. Second join phd_de_patdemo → provider: ON COALESCE(d.{prov_id_col}, d.provider_key) = COALESCE(p.{prov_id_col}, p.provider_key) (alias 'p')\n"
            prompt += f"- Provider table: {config.patient_catalog}.{config.patient_schema}.provider\n"
            # Use exact provider columns if discovered
            if exact_columns:
                if 'location_type_column' in exact_columns:
                    prompt += f"- **EXACT column for location_type**: p.{exact_columns['location_type_column']}\n"
                if 'teaching_flag_column' in exact_columns:
                    prompt += f"- **EXACT column for teaching_flag**: p.{exact_columns['teaching_flag_column']}\n"
                if 'bed_count_column' in exact_columns:
                    prompt += f"- **EXACT column for bed_count**: p.{exact_columns['bed_count_column']}\n"
            else:
                prompt += f"- Provider columns: location_type, teaching_flag, bed_count\n"
            prompt += f"- Note: prov_id and provider_key are the same - use COALESCE to handle either\n"
        else:
            # Fallback to recommended tables if provided
            if recommended_tables:
                prompt += f"- For '{dimension_name}', use table: {recommended_tables[0]}\n"
            else:
                prompt += f"- For '{dimension_name}', use the most appropriate table\n"
        
        prompt += f"""
Schema Information:
{dimension_schema_context}

⚠️ CRITICAL: Databricks SQL Syntax Requirements:
1. Unity Catalog table references: 
   - Cohort table: {cohort_table_quoted} (use as-is, already properly quoted)
   - Patient tables: Use catalog.schema.tablename format (NO backticks for standard names)
   - Example: {config.patient_catalog}.{config.patient_schema}.patdemo
2. JOIN syntax: Use standard SQL JOIN (INNER JOIN or JOIN)
3. Window functions: Use OVER() for percentage calculations - SUM(COUNT(...)) OVER()
4. CASE statements: Use standard SQL CASE WHEN ... THEN ... ELSE ... END
5. Column aliases: Use AS keyword for aliases
6. NULL handling: Use COALESCE() or IS NOT NULL checks

Example SQL Templates:

For PATIENT/VISIT dimensions (age_groups, gender, race, ethnicity, visit_level, admit_source, admit_type):
```sql
SELECT 
    CASE 
        WHEN d.age < 18 THEN '<18'
        WHEN d.age BETWEEN 18 AND 34 THEN '18-34'
        WHEN d.age BETWEEN 35 AND 49 THEN '35-49'
        WHEN d.age BETWEEN 50 AND 64 THEN '50-64'
        WHEN d.age BETWEEN 65 AND 79 THEN '65-79'
        ELSE '80+'
    END as age_group,
    COUNT(DISTINCT c.{join_key}) as patient_count,
    ROUND(COUNT(DISTINCT c.{join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{join_key})) OVER(), 2) as percentage
FROM {cohort_table_quoted} c
JOIN {config.patient_catalog}.{config.patient_schema}.phd_de_patdemo d 
    ON c.{join_key} = d.{join_key}
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
```

For SITE dimensions (urban_rural, teaching, bed_count) - USE BRIDGE JOIN:
```sql
SELECT 
    CASE 
        WHEN p.location_type IN ('Urban', 'URBAN') THEN 'Urban'
        WHEN p.location_type IN ('Rural', 'RURAL') THEN 'Rural'
        ELSE COALESCE(p.location_type, 'Unknown')
    END as location_type,
    COUNT(DISTINCT c.{join_key}) as patient_count,
    ROUND(COUNT(DISTINCT c.{join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{join_key})) OVER(), 2) as percentage
FROM {cohort_table_quoted} c
JOIN {config.patient_catalog}.{config.patient_schema}.phd_de_patdemo d 
    ON c.{join_key} = d.{join_key}
JOIN {config.patient_catalog}.{config.patient_schema}.provider p 
    ON COALESCE(d.prov_id, d.provider_key) = COALESCE(p.prov_id, p.provider_key)
WHERE p.location_type IS NOT NULL
GROUP BY location_type
ORDER BY patient_count DESC
```

CRITICAL Syntax Rules:
- Cohort table: Use exactly as shown: {cohort_table_quoted} (already has backticks)
- Patient tables: Use catalog.schema.tablename format (NO backticks) - Example: {config.patient_catalog}.{config.patient_schema}.phd_de_patdemo
- JOIN: Standard SQL JOIN syntax
- Window functions: SUM(COUNT(DISTINCT ...)) OVER() for percentage
- CASE: Standard SQL CASE WHEN ... THEN ... ELSE ... END
- GROUP BY: Use the dimension column name (e.g., age_group)
- ORDER BY: Use CASE for proper ordering of grouped values

Generate a SQL query following these rules:

1. **Table Selection (CRITICAL):**
   - If dimension is: age_groups, gender, race, ethnicity, visit_level, admit_source, admit_type
     → **MUST use**: {config.patient_catalog}.{config.patient_schema}.phd_de_patdemo
     → Join: ON c.{join_key} = d.{join_key} (use alias 'd' for phd_de_patdemo)
   
   - If dimension is: urban_rural, teaching, bed_count
     → **MUST use**: {config.patient_catalog}.{config.patient_schema}.provider
     → **CRITICAL**: Use phd_de_patdemo as bridge! Join pattern:
       1. First join: Cohort → phd_de_patdemo (ON c.{join_key} = d.{join_key}, alias 'd')
       2. Second join: phd_de_patdemo → provider (ON COALESCE(d.prov_id, d.provider_key) = COALESCE(p.prov_id, p.provider_key), alias 'p')
     → Note: prov_id and provider_key are the same - use COALESCE to handle either column name

2. **Column Selection (USE EXACT COLUMN NAMES FROM ABOVE):**
   - You MUST use the exact column names specified in the "EXACT column" lines above
   - Do NOT guess column names - use ONLY the exact names provided
   - For example, if it says "EXACT column for bed_count: p.BED_GRP", use p.BED_GRP (NOT p.bed_count)
   - age_groups → Use the EXACT age column name shown above
   - gender → Use the EXACT gender column name shown above
   - race → Use the EXACT race column name shown above
   - ethnicity → Use the EXACT ethnicity column name shown above
   - visit_level → Use the EXACT visit_level column name shown above
   - admit_source → Use the EXACT admit_source column name shown above
   - admit_type → Use the EXACT admit_type column name shown above
   - urban_rural → Use the EXACT location_type column name shown above from provider table
   - teaching → Use the EXACT teaching_flag column name shown above from provider table
   - bed_count → Use the EXACT bed_count column name shown above from provider table (may be BED_GRP, BED_COUNT, etc.)

3. **SQL Structure:**
   - Use Unity Catalog format: catalog.schema.tablename (NO backticks)
   - Groups by the {dimension_name} dimension (create appropriate CASE statements)
   - Counts distinct patients: COUNT(DISTINCT c.{join_key}) AS patient_count
   - Calculates percentages: ROUND(COUNT(DISTINCT c.{join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{join_key})) OVER(), 2) AS percentage
   - For visit_level, admit_source, admit_type: Also add COUNT(DISTINCT encounter_id) AS encounter_count (if encounter_id exists)
   - Orders results appropriately
   - Handles NULL values: Use COALESCE or IS NOT NULL

⚠️ CRITICAL: Table and Column Mapping (SIMPLIFIED):
- **Patient/Vist dimensions** (age_groups, gender, race, ethnicity, visit_level, admit_source, admit_type):
  * Table: **patdemo** (use {config.patient_catalog}.{config.patient_schema}.patdemo)
  * Columns in patdemo: age, gender, race, ethnicity, visit_type, visit_level, admit_source, admit_type
  * Join: ON c.{join_key} = d.pat_key (or d.medrec_key if cohort uses medrec_key)

- **Site dimensions** (urban_rural, teaching, bed_count):
  * Table: **provider** (use {config.patient_catalog}.{config.patient_schema}.provider)
  * Columns in provider: location_type, teaching_flag, bed_count
  * Join: ON c.{join_key} = p.pat_key (or p.medrec_key if cohort uses medrec_key)

⚠️ CRITICAL: Column naming requirements (must match exactly):
- Dimension column name: 
  * age_groups → 'age_group' (from patdemo.age with CASE statement)
  * gender → 'gender' (from patdemo.gender)
  * race → 'race' (from patdemo.race)
  * ethnicity → 'ethnicity' (from patdemo.ethnicity)
  * visit_level → 'visit_level' (from patdemo.visit_level or patdemo.visit_type)
  * admit_source → 'admit_source' (from patdemo.admit_source)
  * admit_type → 'admit_type' (from patdemo.admit_type)
  * urban_rural → 'location_type' (from provider.location_type)
  * teaching → 'teaching_status' (from provider.teaching_flag with CASE)
  * bed_count → 'bed_count_group' (from provider.bed_count with CASE)
- Count column: Always name it 'patient_count' (or 'encounter_count' for visit_level, admit_source, admit_type)
- Percentage column: Always name it 'percentage'

The visualization code expects these exact column names!

Return ONLY the SQL query, no markdown code blocks, no explanations, just the SQL.
The query must follow Databricks Unity Catalog SQL syntax exactly as shown in the example above.
"""
        
        try:
            response = self.intent_service.llm.invoke(prompt)
            sql = response.content.strip()
            
            # Clean up SQL (remove markdown code blocks if present)
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            sql = sql.strip()
            
            # Get expected tables for this dimension
            dimension_table_mapping = self.schema_service.get_dimension_table_mapping(
                config.patient_catalog,
                config.patient_schema
            )
            expected_tables = dimension_table_mapping.get(dimension_name, [])
            
            # Validate generated SQL (including table selection)
            is_valid, warnings, validation_details = self.sql_validator.validate_dimension_sql(
                sql, dimension_name, cohort_table_quoted, expected_tables
            )
            
            if warnings:
                logger.warning(f"SQL validation warnings for {dimension_name}: {warnings}")
            
            if not is_valid:
                logger.error(f"SQL validation failed for {dimension_name}. Warnings: {warnings}")
                # Log the SQL for debugging
                logger.error(f"Generated SQL:\n{sql}")
                return (dimension_name, None)
            
            logger.info(f"✓ Generated and validated SQL for {dimension_name}")
            return (dimension_name, sql)
            
        except Exception as e:
            logger.error(f"Error generating {dimension_name} query with LLM: {str(e)}")
            return (dimension_name, None)
    
    
    def analyze_dimensions_dynamically(
        self,
        cohort_table: str,
        has_medrec_key: bool = False,
        dimension_specs: Optional[List[Dict]] = None
    ) -> Dict:
        """
        Dynamically analyze dimensions using schema discovery + LLM (parallel SQL generation)
        
        Process:
        1. Get schema context (cached if available)
        2. Generate ALL dimension SQL queries in parallel using LLM
        3. Execute ALL queries in parallel using ThreadPoolExecutor
        4. Return results
        
        Args:
            cohort_table: Full cohort table name
            has_medrec_key: Whether cohort has medrec_key column
            dimension_specs: List of dimension specifications. If None, uses defaults.
            
        Returns:
            Dictionary with dimension analysis results
        """
        # Default dimension specifications
        if dimension_specs is None:
            dimension_specs = [
                {
                    'name': 'age_groups',
                    'description': 'Group patients by age ranges: <18, 18-34, 35-49, 50-64, 65-79, 80+'
                },
                {
                    'name': 'gender',
                    'description': 'Count patients by gender (Male/Female)'
                },
                {
                    'name': 'race',
                    'description': 'Count patients by race'
                },
                {
                    'name': 'ethnicity',
                    'description': 'Count patients by ethnicity'
                },
                {
                    'name': 'visit_level',
                    'description': 'Count encounters by visit level (Inpatient/Outpatient/Emergency/etc)'
                },
                {
                    'name': 'admit_source',
                    'description': 'Count encounters by admission source'
                },
                {
                    'name': 'admit_type',
                    'description': 'Count encounters by admission type'
                },
                {
                    'name': 'urban_rural',
                    'description': 'Count patients by location type (Urban/Rural)'
                },
                {
                    'name': 'teaching',
                    'description': 'Count patients by teaching hospital status (Teaching/Non-Teaching)'
                },
                {
                    'name': 'bed_count',
                    'description': 'Group patients by hospital bed count ranges: <100, 100-299, 300-499, 500+'
                }
            ]
        
        # Step 1: Get schema context (cached)
        logger.info("Getting schema context (cached if available)...")
        try:
            schema_context = self.get_schema_context(
                config.patient_catalog,
                config.patient_schema,
                use_cache=True
            )
            logger.info(f"Schema context ready ({len(schema_context)} chars)")
        except Exception as e:
            logger.error(f"Failed to get schema context: {str(e)}")
            raise ValueError(f"Schema discovery failed: {str(e)}. Please check your DATABRICKS_HOST, DATABRICKS_TOKEN, and SQL_WAREHOUSE_ID configuration.")
        
        # Step 2: Generate ALL dimension SQL queries in parallel using LLM
        logger.info(f"Generating {len(dimension_specs)} dimension SQL queries in parallel...")
        generated_queries = {}
        query_generation_errors = {}
        validation_results = {}  # Store validation results for each dimension
        
        def generate_single_query(spec):
            """Generate SQL for a single dimension"""
            return self.generate_dimension_query_with_llm_parallel(
                spec, cohort_table, schema_context, has_medrec_key
            )
        
        # Generate all queries in parallel
        with ThreadPoolExecutor(max_workers=len(dimension_specs)) as executor:
            futures = {executor.submit(generate_single_query, spec): spec['name'] 
                      for spec in dimension_specs}
            
            for future in as_completed(futures):
                dim_name, sql = future.result()
                if sql:
                    # Get expected tables for this dimension
                    dimension_table_mapping = self.schema_service.get_dimension_table_mapping(
                        config.patient_catalog,
                        config.patient_schema
                    )
                    expected_tables = dimension_table_mapping.get(dim_name, [])
                    
                    # Validate SQL before storing (including table selection)
                    is_valid, warnings, validation_details = self.sql_validator.validate_dimension_sql(
                        sql, dim_name, cohort_table, expected_tables
                    )
                    validation_results[dim_name] = validation_details
                    
                    if is_valid:
                        generated_queries[dim_name] = sql
                        logger.info(f"✓ Generated and validated SQL for {dim_name}")
                    else:
                        query_generation_errors[dim_name] = f"SQL validation failed: {', '.join(warnings)}"
                        logger.error(f"✗ SQL validation failed for {dim_name}: {warnings}")
                        logger.error(f"Generated SQL:\n{sql}")
                else:
                    query_generation_errors[dim_name] = "Failed to generate SQL query"
                    logger.warning(f"✗ Failed to generate SQL for {dim_name}")
        
        logger.info(f"SQL generation complete: {len(generated_queries)}/{len(dimension_specs)} successful")
        
        # Log validation summary
        if validation_results:
            valid_count = sum(1 for v in validation_results.values() if v.get('is_valid', False))
            logger.info(f"SQL validation: {valid_count}/{len(validation_results)} passed")
        
        # Step 3: Execute ALL queries in parallel
        logger.info(f"Executing {len(generated_queries)} queries in parallel...")
        results = {}
        execution_errors = {}
        
        def execute_single_query(dim_name: str, sql: str):
            """Execute a single dimension query"""
            try:
                result = self.dimension_service._execute_query(sql)
                return (dim_name, result, None)
            except Exception as e:
                logger.error(f"Error executing {dim_name} query: {str(e)}")
                return (dim_name, [], str(e))
        
        # Execute all queries in parallel
        with ThreadPoolExecutor(max_workers=len(generated_queries)) as executor:
            futures = {executor.submit(execute_single_query, name, sql): name 
                      for name, sql in generated_queries.items()}
            
            for future in as_completed(futures):
                dim_name, result, error = future.result()
                if error:
                    execution_errors[dim_name] = error
                    results[dim_name] = []
                else:
                    results[dim_name] = result
                    logger.info(f"✓ Executed {dim_name}: {len(result)} rows")
        
        logger.info(f"Query execution complete: {len(results)}/{len(generated_queries)} successful")
        
        # Combine all errors
        all_errors = {**query_generation_errors, **execution_errors}
        
        return {
            'dimensions': results,
            'errors': all_errors,
            'generated_queries': generated_queries,  # Include generated SQL for debugging
            'validation_results': validation_results,  # Include validation details
            'cohort_table': cohort_table
        }
