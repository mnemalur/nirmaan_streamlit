"""
Schema Discovery Service
Discovers Unity Catalog schema metadata and provides it to LLM/Genie for dynamic SQL generation
"""

from databricks.sql import connect
from config import config
from typing import Dict, List, Optional
import logging
import json

logger = logging.getLogger(__name__)


class SchemaDiscoveryService:
    """Service to discover Unity Catalog schema metadata"""
    
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
    
    def _execute_query(self, sql: str) -> List[Dict]:
        """Execute SQL and return results as list of dictionaries"""
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
    
    def discover_tables(self, catalog: str, schema: str) -> List[Dict]:
        """
        Discover all tables in a catalog.schema
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of table metadata dictionaries
        """
        sql = f"""
        SELECT 
            TABLE_CATALOG,
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_TYPE,
            COMMENT
        FROM information_schema.tables
        WHERE TABLE_CATALOG = '{catalog}'
          AND TABLE_SCHEMA = '{schema}'
        ORDER BY TABLE_NAME
        """
        
        try:
            return self._execute_query(sql)
        except Exception as e:
            logger.error(f"Error discovering tables: {str(e)}")
            return []
    
    def discover_columns(self, catalog: str, schema: str, table: str) -> List[Dict]:
        """
        Discover all columns for a specific table, including comments from Databricks metadata
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            List of column metadata dictionaries with COLUMN_NAME, DATA_TYPE, COMMENT, etc.
        """
        sql = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            ORDINAL_POSITION,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT
        FROM information_schema.columns
        WHERE TABLE_CATALOG = '{catalog}'
          AND TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        
        try:
            columns = self._execute_query(sql)
            # Log column comments if available (helpful for LLM)
            for col in columns:
                if col.get('COMMENT'):
                    logger.debug(f"Column {table}.{col['COLUMN_NAME']}: {col['COMMENT']}")
            return columns
        except Exception as e:
            logger.error(f"Error discovering columns for {table}: {str(e)}")
            return []
    
    def identify_table_purpose(self, table_name: str, columns: List[Dict]) -> List[str]:
        """
        Identify the purpose/type of a table based on its name and columns
        
        Args:
            table_name: Table name
            columns: List of column dictionaries
            
        Returns:
            List of table purposes (e.g., ['demographics', 'procedures', 'encounters'])
        """
        purposes = []
        table_lower = table_name.lower()
        column_names = [col['COLUMN_NAME'].lower() for col in columns]
        
        # Check table name patterns
        if 'demo' in table_lower or 'patient' in table_lower:
            purposes.append('demographics')
        if 'cpt' in table_lower or 'procedure' in table_lower:
            purposes.append('procedures')
        if 'icd' in table_lower or 'diagnosis' in table_lower:
            purposes.append('diagnoses')
        if 'encounter' in table_lower or 'visit' in table_lower:
            purposes.append('encounters')
        if 'lab' in table_lower or 'test' in table_lower:
            purposes.append('labs')
        if 'med' in table_lower or 'drug' in table_lower or 'rx' in table_lower:
            purposes.append('medications')
        
        # Check column patterns
        demo_columns = ['age', 'gender', 'race', 'ethnicity', 'birth', 'sex']
        if any(col in ' '.join(column_names) for col in demo_columns):
            purposes.append('demographics')
        
        proc_columns = ['cpt', 'procedure', 'proc_code']
        if any(col in ' '.join(column_names) for col in proc_columns):
            purposes.append('procedures')
        
        encounter_columns = ['encounter', 'visit', 'admit', 'discharge', 'visit_type']
        if any(col in ' '.join(column_names) for col in encounter_columns):
            purposes.append('encounters')
        
        # Remove duplicates
        return list(set(purposes))
    
    def get_dimension_table_mapping(self, catalog: str, schema: str) -> Dict[str, List[str]]:
        """
        Map dimensions to recommended tables (simplified - uses patdemo and provider tables)
        
        Simple mapping:
        - Patient-level dimensions (age, gender, race, ethnicity) → patdemo
        - Visit-level dimensions (visit_level, admit_source, admit_type) → patdemo  
        - Site-level dimensions (urban_rural, teaching, bed_count) → provider
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            Dictionary mapping dimension names to list of recommended table names
        """
        # Discover available tables
        tables = self.discover_tables(catalog, schema)
        table_names = [t['TABLE_NAME'].lower() for t in tables]
        
        # Find phd_de_patdemo table (case-insensitive)
        patdemo_table = None
        provider_table = None
        
        for table_info in tables:
            table_name_lower = table_info['TABLE_NAME'].lower()
            if 'phd_de_patdemo' in table_name_lower or table_name_lower == 'phd_de_patdemo':
                patdemo_table = table_info['TABLE_NAME']
            if 'provider' in table_name_lower or table_name_lower == 'provider':
                provider_table = table_info['TABLE_NAME']
        
        # If not found, try exact match
        if not patdemo_table:
            # Check exact match first
            for table_info in tables:
                if table_info['TABLE_NAME'].lower() == 'phd_de_patdemo':
                    patdemo_table = table_info['TABLE_NAME']
                    break
            # If still not found, use first table with 'patdemo' or 'demo'
            if not patdemo_table:
                for table_info in tables:
                    table_name_lower = table_info['TABLE_NAME'].lower()
                    if 'patdemo' in table_name_lower or 'demo' in table_name_lower:
                        patdemo_table = table_info['TABLE_NAME']
                        break
        
        if not provider_table:
            # Check exact match first for phd_de_providers
            for table_info in tables:
                table_name_lower = table_info['TABLE_NAME'].lower()
                if table_name_lower == 'phd_de_providers':
                    provider_table = table_info['TABLE_NAME']
                    break
            # If still not found, try variations
            if not provider_table:
                for table_info in tables:
                    table_name_lower = table_info['TABLE_NAME'].lower()
                    if 'phd_de_provider' in table_name_lower or table_name_lower == 'phd_de_providers':
                        provider_table = table_info['TABLE_NAME']
                        break
            # Last resort: look for provider
            if not provider_table:
                for table_info in tables:
                    table_name_lower = table_info['TABLE_NAME'].lower()
                    if 'provider' in table_name_lower:
                        provider_table = table_info['TABLE_NAME']
                        break
        
        # Default mappings (simplified)
        dimension_to_tables = {
            # Patient-level dimensions → patdemo
            'age_groups': [patdemo_table] if patdemo_table else [],
            'gender': [patdemo_table] if patdemo_table else [],
            'race': [patdemo_table] if patdemo_table else [],
            'ethnicity': [patdemo_table] if patdemo_table else [],
            
            # Visit-level dimensions → patdemo (visit info is often in patient demo table)
            'visit_level': [patdemo_table] if patdemo_table else [],
            'admit_source': [patdemo_table] if patdemo_table else [],
            'admit_type': [patdemo_table] if patdemo_table else [],
            
            # Site-level dimensions → provider
            'urban_rural': [provider_table] if provider_table else [],
            'teaching': [provider_table] if provider_table else [],
            'bed_count': [provider_table] if provider_table else [],
            
            # Other dimensions (for future use)
            'procedures': [],
            'diagnoses': [],
            'labs': [],
            'medications': []
        }
        
        logger.info(f"Dimension table mapping - phd_de_patdemo: {patdemo_table}, phd_de_providers: {provider_table}")
        
        return dimension_to_tables
    
    def get_exact_column_names_for_dimension(
        self, 
        catalog: str, 
        schema: str, 
        dimension_name: str
    ) -> Dict[str, str]:
        """
        Get exact column names from actual tables for a specific dimension.
        This ensures we use the real column names (e.g., BED_GRP instead of BED_COUNT).
        
        Args:
            catalog: Catalog name
            schema: Schema name
            dimension_name: Dimension name (e.g., 'age_groups', 'bed_count')
            
        Returns:
            Dictionary mapping logical names to actual column names:
            {
                'source_column': 'actual_column_name',  # e.g., 'bed_count': 'BED_GRP'
                'join_key': 'actual_join_column',  # e.g., 'prov_id': 'PROV_ID'
            }
        """
        dimension_table_mapping = self.get_dimension_table_mapping(catalog, schema)
        recommended_tables = dimension_table_mapping.get(dimension_name, [])
        
        if not recommended_tables:
            logger.warning(f"No table mapping found for dimension: {dimension_name}")
            return {}
        
        # Get columns from the recommended table(s)
        # For site dimensions, we also need phd_de_patdemo for the bridge join
        table_columns = {}
        column_comments = {}  # Store column comments for LLM context
        for table_name in recommended_tables:
            if table_name:
                columns = self.discover_columns(catalog, schema, table_name)
                table_columns[table_name] = {col['COLUMN_NAME']: col for col in columns}
                # Store comments for this table
                column_comments[table_name] = {
                    col['COLUMN_NAME']: col.get('COMMENT', '') 
                    for col in columns if col.get('COMMENT')
                }
        
        # For site dimensions, also get phd_de_patdemo columns (needed for bridge join)
        if dimension_name in ['urban_rural', 'teaching', 'bed_count']:
            # Find phd_de_patdemo table (for bridge join)
            tables = self.discover_tables(catalog, schema)
            patdemo_table = None
            for table_info in tables:
                table_name_lower = table_info['TABLE_NAME'].lower()
                if 'phd_de_patdemo' in table_name_lower or table_name_lower == 'phd_de_patdemo':
                    patdemo_table = table_info['TABLE_NAME']
                    break
            
            if patdemo_table and patdemo_table not in table_columns:
                columns = self.discover_columns(catalog, schema, patdemo_table)
                table_columns[patdemo_table] = {col['COLUMN_NAME']: col for col in columns}
                # Store comments for phd_de_patdemo too
                column_comments[patdemo_table] = {
                    col['COLUMN_NAME']: col.get('COMMENT', '') 
                    for col in columns if col.get('COMMENT')
                }
            
            # Also find phd_de_providers table (target table for site dimensions)
            providers_table = None
            for table_info in tables:
                table_name_lower = table_info['TABLE_NAME'].lower()
                if table_name_lower == 'phd_de_providers' or 'phd_de_provider' in table_name_lower:
                    providers_table = table_info['TABLE_NAME']
                    break
            
            if providers_table and providers_table not in table_columns:
                columns = self.discover_columns(catalog, schema, providers_table)
                table_columns[providers_table] = {col['COLUMN_NAME']: col for col in columns}
                column_comments[providers_table] = {
                    col['COLUMN_NAME']: col.get('COMMENT', '') 
                    for col in columns if col.get('COMMENT')
                }
        
        result = {}
        
        # Helper to find table by name (case-insensitive)
        def find_table(table_name_pattern):
            for table_name in table_columns.keys():
                if table_name_pattern.lower() in table_name.lower():
                    return table_columns[table_name]
            return None
        
        # Map dimension to expected columns and find actual matches
        # NOTE: phd_de_patdemo is ENCOUNTER-CENTRIC (patient_key = encounter/visit)
        # Patient-level: GENDER, race, HISPANIC_IND
        # Visit-level: I_O_IND, ADM_TYPE, PAT_TYPE
        
        if dimension_name == 'gender':
            cols = find_table('phd_de_patdemo')
            if cols:
                if 'GENDER' in cols:
                    result['gender_column'] = 'GENDER'
                else:
                    for col_name in cols.keys():
                        if col_name.upper() == 'GENDER' or 'gender' in col_name.lower():
                            result['gender_column'] = col_name
                            break
        
        elif dimension_name == 'race':
            cols = find_table('phd_de_patdemo')
            if cols:
                if 'race' in cols:
                    result['race_column'] = 'race'
                else:
                    for col_name in cols.keys():
                        if col_name.lower() == 'race' or col_name.upper() == 'RACE':
                            result['race_column'] = col_name
                            break
        
        elif dimension_name == 'ethnicity':
            cols = find_table('phd_de_patdemo')
            if cols:
                if 'HISPANIC_IND' in cols:
                    result['ethnicity_column'] = 'HISPANIC_IND'
                else:
                    for col_name in cols.keys():
                        if 'hispanic' in col_name.lower() or 'ethnic' in col_name.lower():
                            result['ethnicity_column'] = col_name
                            break
        
        elif dimension_name == 'visit_level':
            cols = find_table('phd_de_patdemo')
            if cols:
                if 'I_O_IND' in cols:
                    result['visit_level_column'] = 'I_O_IND'
                else:
                    for col_name in cols.keys():
                        if 'i_o' in col_name.lower() or ('inpatient' in col_name.lower() and 'outpatient' in col_name.lower()):
                            result['visit_level_column'] = col_name
                            break
        
        elif dimension_name == 'admit_type':
            cols = find_table('phd_de_patdemo')
            if cols:
                if 'ADM_TYPE' in cols:
                    result['admit_type_column'] = 'ADM_TYPE'
                else:
                    for col_name in cols.keys():
                        if col_name.upper() == 'ADM_TYPE' or ('admit' in col_name.lower() and 'type' in col_name.lower()):
                            result['admit_type_column'] = col_name
                            break
        
        elif dimension_name == 'admit_source':
            cols = find_table('phd_de_patdemo')
            if cols:
                if 'PAT_TYPE' in cols:
                    result['admit_source_column'] = 'PAT_TYPE'
                else:
                    for col_name in cols.keys():
                        if col_name.upper() == 'PAT_TYPE' or ('pat' in col_name.lower() and 'type' in col_name.lower()):
                            result['admit_source_column'] = col_name
                            break
        
        elif dimension_name == 'urban_rural':
            # Need provider table columns
            provider_cols = find_table('provider')
            if provider_cols:
                for col_name in provider_cols.keys():
                    if 'location' in col_name.lower() or 'urban' in col_name.lower() or 'rural' in col_name.lower():
                        result['location_type_column'] = col_name
                        break
            # Also need prov_id from phd_de_patdemo for bridge join
            patdemo_cols = find_table('phd_de_patdemo')
            if patdemo_cols:
                # Look for PROVIDER_KEY or PROV_ID (they mean the same thing)
                prov_col = None
                for col_name in patdemo_cols.keys():
                    col_upper = col_name.upper()
                    if col_upper == 'PROVIDER_KEY' or col_upper == 'PROV_ID':
                        prov_col = col_name
                        break
                    elif 'prov' in col_name.lower() and ('id' in col_name.lower() or 'key' in col_name.lower()):
                        prov_col = col_name
                        break
                if prov_col:
                    result['prov_id_column'] = prov_col
        
        elif dimension_name == 'teaching':
            provider_cols = find_table('provider')
            if provider_cols:
                for col_name in provider_cols.keys():
                    if 'teach' in col_name.lower() or 'train' in col_name.lower():
                        result['teaching_flag_column'] = col_name
                        break
            patdemo_cols = find_table('phd_de_patdemo')
            if patdemo_cols:
                # Look for PROVIDER_KEY or PROV_ID (they mean the same thing)
                prov_col = None
                for col_name in patdemo_cols.keys():
                    col_upper = col_name.upper()
                    if col_upper == 'PROVIDER_KEY' or col_upper == 'PROV_ID':
                        prov_col = col_name
                        break
                    elif 'prov' in col_name.lower() and ('id' in col_name.lower() or 'key' in col_name.lower()):
                        prov_col = col_name
                        break
                if prov_col:
                    result['prov_id_column'] = prov_col
        
        elif dimension_name == 'bed_count':
            provider_cols = find_table('provider')
            if provider_cols:
                for col_name in provider_cols.keys():
                    if 'bed' in col_name.lower() or 'beds' in col_name.lower():
                        result['bed_count_column'] = col_name
                        break
            patdemo_cols = find_table('phd_de_patdemo')
            if patdemo_cols:
                # Look for PROVIDER_KEY or PROV_ID (they mean the same thing)
                prov_col = None
                for col_name in patdemo_cols.keys():
                    col_upper = col_name.upper()
                    if col_upper == 'PROVIDER_KEY' or col_upper == 'PROV_ID':
                        prov_col = col_name
                        break
                    elif 'prov' in col_name.lower() and ('id' in col_name.lower() or 'key' in col_name.lower()):
                        prov_col = col_name
                        break
                if prov_col:
                    result['prov_id_column'] = prov_col
        
        # Add column comments to result if available (helpful for LLM)
        for table_name, comments in column_comments.items():
            for col_key, col_name in result.items():
                if col_name in comments and comments[col_name]:
                    result[f"{col_key}_comment"] = comments[col_name]
        
        logger.info(f"Exact columns for {dimension_name}: {result}")
        return result
    
    def get_schema_summary(self, catalog: str, schema: str) -> Dict:
        """
        Get comprehensive schema summary for LLM/Genie context
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            Dictionary with schema summary including:
            - tables: List of tables with their columns
            - key_columns: Common key columns (patient_key, medrec_key, etc.)
            - dimension_columns: Columns that might be useful for dimension analysis
        """
        tables = self.discover_tables(catalog, schema)
        
        schema_info = {
            'catalog': catalog,
            'schema': schema,
            'tables': []
        }
        
        key_columns = []
        dimension_columns = []
        
        # Common patterns for key columns
        key_patterns = ['key', 'id', 'patient', 'medrec', 'encounter']
        dimension_patterns = ['age', 'gender', 'race', 'ethnicity', 'visit', 'admit', 
                            'location', 'teaching', 'bed', 'type', 'source', 'class']
        
        for table_info in tables:
            table_name = table_info['TABLE_NAME']
            columns = self.discover_columns(catalog, schema, table_name)
            
            table_data = {
                'name': table_name,
                'type': table_info.get('TABLE_TYPE', 'BASE TABLE'),
                'comment': table_info.get('COMMENT', ''),
                'columns': []
            }
            
            for col in columns:
                col_name = col['COLUMN_NAME'].lower()
                col_data = {
                    'name': col['COLUMN_NAME'],
                    'type': col['DATA_TYPE'],
                    'nullable': col['IS_NULLABLE'] == 'YES',
                    'comment': col.get('COMMENT', '')
                }
                table_data['columns'].append(col_data)
                
                # Identify key columns
                if any(pattern in col_name for pattern in key_patterns):
                    key_columns.append({
                        'table': table_name,
                        'column': col['COLUMN_NAME']
                    })
                
                # Identify dimension columns
                if any(pattern in col_name for pattern in dimension_patterns):
                    dimension_columns.append({
                        'table': table_name,
                        'column': col['COLUMN_NAME'],
                        'type': col['DATA_TYPE']
                    })
            
            schema_info['tables'].append(table_data)
        
        schema_info['key_columns'] = key_columns
        schema_info['dimension_columns'] = dimension_columns
        
        # Add table purpose mapping
        schema_info['dimension_table_mapping'] = self.get_dimension_table_mapping(catalog, schema)
        
        return schema_info
    
    def get_schema_context_for_llm(self, catalog: str, schema: str, dimension_name: Optional[str] = None) -> str:
        """
        Format schema summary as a string prompt for LLM
        
        Args:
            catalog: Catalog name
            schema: Schema name
            dimension_name: Optional dimension name to provide table recommendations
            
        Returns:
            Formatted string with schema information for LLM context
        """
        schema_info = self.get_schema_summary(catalog, schema)
        
        context = f"# Schema: {catalog}.{schema}\n\n"
        
        # Add table recommendations for specific dimension
        if dimension_name and 'dimension_table_mapping' in schema_info:
            recommended_tables = schema_info['dimension_table_mapping'].get(dimension_name, [])
            if recommended_tables:
                context += f"## ⚠️ IMPORTANT: For dimension '{dimension_name}', use these tables:\n"
                for table in recommended_tables:
                    # Show full Unity Catalog path
                    context += f"  - **`{catalog}`.`{schema}`.`{table}`** (RECOMMENDED - use this exact format)\n"
                context += "\n"
        
        context += "## Tables and Columns:\n\n"
        for table in schema_info['tables']:
            context += f"### Table: {table['name']}\n"
            if table['comment']:
                context += f"Comment: {table['comment']}\n"
            context += "Columns:\n"
            for col in table['columns']:
                nullable = "NULL" if col['nullable'] else "NOT NULL"
                context += f"  - {col['name']} ({col['type']}) {nullable}"
                if col['comment']:
                    context += f" -- {col['comment']}"
                context += "\n"
            context += "\n"
        
        if schema_info['key_columns']:
            context += "## Key Columns (for joins):\n"
            for key_col in schema_info['key_columns']:
                context += f"  - {key_col['table']}.{key_col['column']}\n"
            context += "\n"
        
        if schema_info['dimension_columns']:
            context += "## Dimension Columns (for analysis):\n"
            for dim_col in schema_info['dimension_columns']:
                context += f"  - {dim_col['table']}.{dim_col['column']} ({dim_col['type']})\n"
        
        return context
