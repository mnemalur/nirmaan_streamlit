"""
Dynamic Dimension Analysis Service
Uses schema discovery + LLM to generate dimension queries dynamically in parallel
Avoids Genie's linear limitation by generating all SQL queries upfront, then executing in parallel
"""

from typing import Dict, List, Optional
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
        
        join_key = "medrec_key" if has_medrec_key else "patient_key"
        
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
        
        prompt = f"""You are a SQL expert for Databricks Unity Catalog. Generate a SQL query for dimension analysis.

Dimension: {dimension_name}
Description: {dimension_description}

Cohort Table: {cohort_table_quoted}
Join Key: {join_key}
Patient Schema: {config.patient_catalog}.{config.patient_schema}

⚠️ CRITICAL: Table Selection Rules:
"""
        
        if recommended_tables:
            prompt += f"- For '{dimension_name}', you MUST use one of these tables: {', '.join(recommended_tables)}\n"
            prompt += f"- Primary recommended table: **{recommended_tables[0]}**\n"
        else:
            prompt += f"- For '{dimension_name}', use the most appropriate table based on column names\n"
        
        # Add dimension-specific guidance
        if dimension_name in ['age_groups', 'gender', 'race', 'ethnicity', 'urban_rural', 'teaching', 'bed_count']:
            prompt += "- These are PATIENT DEMOGRAPHICS - use the demographics/patient table (usually 'patdemo')\n"
        elif dimension_name in ['visit_level', 'admit_source', 'admit_type']:
            prompt += "- These are ENCOUNTER/VISIT attributes - use the encounter/visit table\n"
        elif dimension_name == 'procedures':
            prompt += "- This is PROCEDURE data - use the procedure table (usually 'patcpt')\n"
        
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

Example SQL Template (for age_groups dimension):
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
JOIN {config.patient_catalog}.{config.patient_schema}.patdemo d 
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

CRITICAL Syntax Rules:
- Cohort table: Use exactly as shown: {cohort_table_quoted}
- Patient tables: Use catalog.schema.tablename (NO backticks) - Example: {config.patient_catalog}.{config.patient_schema}.patdemo
- JOIN: Standard SQL JOIN syntax
- Window functions: SUM(COUNT(DISTINCT ...)) OVER() for percentage
- CASE: Standard SQL CASE WHEN ... THEN ... ELSE ... END
- GROUP BY: Use the dimension column name (e.g., age_group)
- ORDER BY: Use CASE for proper ordering of grouped values

Generate a SQL query that:
1. **MUST** join the cohort table ({cohort_table_quoted}) with the RECOMMENDED table(s) above using {join_key}
2. Uses Unity Catalog format: `catalog`.`schema`.`table` with backticks
3. Groups by the {dimension_name} dimension (create appropriate CASE statements for grouping)
4. Counts distinct patients using COUNT(DISTINCT c.{join_key})
5. Calculates percentages using window functions: ROUND(COUNT(DISTINCT c.{join_key}) * 100.0 / SUM(COUNT(DISTINCT c.{join_key})) OVER(), 2)
6. Orders results appropriately
7. Handles NULL values appropriately (use COALESCE or IS NOT NULL)

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
        schema_context = self.get_schema_context(
            config.patient_catalog,
            config.patient_schema,
            use_cache=True
        )
        logger.info(f"Schema context ready ({len(schema_context)} chars)")
        
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
