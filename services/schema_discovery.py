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
            raise ValueError("DATABricks_HOST is required")
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
        Discover all columns for a specific table
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            List of column metadata dictionaries
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
            return self._execute_query(sql)
        except Exception as e:
            logger.error(f"Error discovering columns for {table}: {str(e)}")
            return []
    
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
