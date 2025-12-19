"""
SQL Validator Service
Validates generated SQL queries for correctness, safety, and schema compliance
"""

import re
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class SQLValidator:
    """Validates SQL queries for dimension analysis"""
    
    def __init__(self):
        self.required_patterns = [
            r'SELECT',  # Must have SELECT
            r'FROM',   # Must have FROM
            r'COUNT',  # Must have COUNT for dimension analysis
            r'GROUP\s+BY',  # Must have GROUP BY
        ]
        
        self.dangerous_patterns = [
            r'DROP\s+TABLE',
            r'DELETE\s+FROM',
            r'TRUNCATE',
            r'ALTER\s+TABLE',
            r'CREATE\s+TABLE',
            r'UPDATE\s+.*\s+SET',
        ]
    
    def validate_sql(
        self, 
        sql: str, 
        dimension_name: str,
        cohort_table: str,
        expected_columns: Optional[List[str]] = None
    ) -> Tuple[bool, List[str]]:
        """
        Validate SQL query for dimension analysis
        
        Args:
            sql: SQL query string to validate
            dimension_name: Name of dimension being analyzed
            cohort_table: Expected cohort table name
            expected_columns: List of expected column names in result (optional)
            
        Returns:
            Tuple of (is_valid, list_of_warnings)
        """
        warnings = []
        sql_upper = sql.upper()
        
        # 1. Check for required SQL patterns
        for pattern in self.required_patterns:
            if not re.search(pattern, sql_upper, re.IGNORECASE):
                warnings.append(f"Missing required SQL pattern: {pattern}")
        
        # 2. Check for dangerous patterns (should not exist)
        for pattern in self.dangerous_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                warnings.append(f"⚠️ DANGEROUS: Found dangerous SQL pattern: {pattern}")
        
        # 3. Check that cohort table is referenced
        cohort_table_clean = cohort_table.replace('`', '').replace('.', r'\.')
        if not re.search(cohort_table_clean, sql, re.IGNORECASE):
            warnings.append(f"⚠️ Cohort table '{cohort_table}' not found in SQL")
        
        # 4. Check for JOIN (should join cohort with patient/encounter tables)
        if 'JOIN' not in sql_upper and 'join' not in sql:
            warnings.append("⚠️ No JOIN found - query may not be joining with patient tables")
        
        # 5. Check for GROUP BY (required for dimension analysis)
        if 'GROUP BY' not in sql_upper:
            warnings.append("⚠️ No GROUP BY found - query may not aggregate correctly")
        
        # 6. Check for COUNT (required for dimension analysis)
        if 'COUNT' not in sql_upper:
            warnings.append("⚠️ No COUNT found - query may not count patients correctly")
        
        # 7. Check for percentage calculation (nice to have)
        if 'PERCENTAGE' not in sql_upper and 'percentage' not in sql.lower():
            # Check for window function pattern
            if not re.search(r'OVER\s*\(\)', sql_upper):
                warnings.append("ℹ️ No percentage calculation found (optional but recommended)")
        
        # 8. Basic syntax check - balanced parentheses
        if sql.count('(') != sql.count(')'):
            warnings.append("⚠️ Unbalanced parentheses detected")
        
        # 9. Check for SQL injection patterns (basic)
        suspicious_patterns = [';', '--', '/*', '*/', 'xp_', 'sp_']
        for pattern in suspicious_patterns:
            if pattern in sql:
                warnings.append(f"⚠️ Suspicious pattern found: '{pattern}'")
        
        # 10. Check expected columns if provided
        if expected_columns:
            sql_lower = sql.lower()
            for col in expected_columns:
                if col.lower() not in sql_lower:
                    warnings.append(f"ℹ️ Expected column '{col}' not found in SELECT clause")
        
        is_valid = len([w for w in warnings if w.startswith('⚠️')]) == 0
        
        return is_valid, warnings
    
    def validate_dimension_sql(
        self,
        sql: str,
        dimension_name: str,
        cohort_table: str,
        expected_tables: Optional[List[str]] = None,
        allowed_columns: Optional[Dict[str, List[str]]] = None
    ) -> Tuple[bool, List[str], Dict]:
        """
        Validate SQL for a specific dimension type
        
        Args:
            sql: SQL query string
            dimension_name: Name of dimension
            cohort_table: Cohort table name
            expected_tables: List of expected table names that should be joined (optional)
            
        Returns:
            Tuple of (is_valid, warnings, validation_details)
        """
        # Dimension-specific expected columns (ONLY the 9 dimensions we support)
        dimension_columns = {
            'gender': ['gender', 'patient_count', 'percentage'],
            'race': ['race', 'patient_count', 'percentage'],
            'ethnicity': ['ethnicity', 'patient_count', 'percentage'],
            'visit_level': ['visit_level', 'encounter_count', 'patient_count', 'percentage'],
            'admit_type': ['admit_type', 'encounter_count', 'patient_count', 'percentage'],
            'admit_source': ['admit_source', 'encounter_count', 'patient_count', 'percentage'],
            'urban_rural': ['location_type', 'patient_count', 'percentage'],
            'teaching': ['teaching_status', 'patient_count', 'percentage'],
            'bed_count': ['bed_count_group', 'patient_count', 'percentage'],
        }
        
        expected_cols = dimension_columns.get(dimension_name, [])
        
        is_valid, warnings = self.validate_sql(
            sql, dimension_name, cohort_table, expected_cols
        )
        
        # Check if correct tables are being used
        sql_upper = sql.upper()
        tables_used = []
        
        # Extract table names from SQL (basic pattern matching)
        # Look for table references after FROM and JOIN
        from_pattern = r'FROM\s+[`"]?(\w+)[`"]?'
        join_pattern = r'JOIN\s+[`"]?(\w+)[`"]?'
        
        tables_used.extend(re.findall(from_pattern, sql_upper))
        tables_used.extend(re.findall(join_pattern, sql_upper))
        tables_used = [t.lower() for t in tables_used if t.lower() != 'c']  # Filter out alias 'c'
        
        # Check if expected tables are used
        table_validation_warnings = []
        if expected_tables:
            expected_tables_lower = [t.lower() for t in expected_tables]
            tables_used_lower = [t.lower() for t in tables_used]
            
            # Check if any expected table is used
            found_expected = any(table in tables_used_lower for table in expected_tables_lower)
            
            if not found_expected:
                table_validation_warnings.append(
                    f"⚠️ Expected table(s) not found: {', '.join(expected_tables)}. "
                    f"Found tables: {', '.join(set(tables_used)) if tables_used else 'none'}"
                )
            else:
                # Check which expected table was used
                used_expected = [t for t in expected_tables_lower if t in tables_used_lower]
                if used_expected:
                    logger.info(f"✓ Correct table(s) used for {dimension_name}: {used_expected}")
        
        warnings.extend(table_validation_warnings)
        
        # Validate that only allowed columns are used (if provided)
        column_validation_warnings = []
        if allowed_columns:
            # Extract column references from SQL
            # Look for patterns like: d.COLUMN_NAME, p.COLUMN_NAME, alias.COLUMN_NAME
            sql_upper = sql.upper()
            
            # Pattern to match table alias.column references (e.g., d.AGE, p.URBAN_RURAL)
            # Also handle backticked columns: d.`AGE`, `d`.`AGE`
            import re
            column_refs = re.findall(r'([dpe])\.([A-Z_][A-Z0-9_]*)', sql_upper)
            
            for alias, col_name in column_refs:
                table_name = None
                if alias == 'd':
                    table_name = 'phd_de_patdemo'
                elif alias == 'p':
                    table_name = 'phd_de_providers'  # Updated: use phd_de_providers, not provider
                elif alias == 'e':
                    column_validation_warnings.append(
                        f"⚠️ INVALID TABLE: 'encounter' table (alias 'e') does not exist. Use phd_de_patdemo (alias 'd') instead."
                    )
                    continue
                
                if table_name and table_name in allowed_columns:
                    allowed_cols = [c.upper() for c in allowed_columns[table_name]]
                    if col_name not in allowed_cols:
                        column_validation_warnings.append(
                            f"⚠️ UNRESOLVED COLUMN: {alias}.{col_name} is not in allowed columns for {table_name}. "
                            f"Allowed columns: {', '.join(allowed_columns[table_name])}"
                        )
                elif table_name:
                    column_validation_warnings.append(
                        f"⚠️ UNKNOWN TABLE: Table '{table_name}' not in allowed tables list"
                    )
        
        warnings.extend(column_validation_warnings)
        
        validation_details = {
            'dimension': dimension_name,
            'has_cohort_table': cohort_table.replace('`', '') in sql.replace('`', ''),
            'has_join': 'JOIN' in sql.upper(),
            'has_group_by': 'GROUP BY' in sql.upper(),
            'has_count': 'COUNT' in sql.upper(),
            'has_percentage': 'percentage' in sql.lower() or 'OVER()' in sql.upper(),
            'tables_used': list(set(tables_used)),
            'expected_tables': expected_tables or [],
            'warnings': warnings,
            'is_valid': is_valid and len(table_validation_warnings) == 0 and len(column_validation_warnings) == 0
        }
        
        return validation_details['is_valid'], warnings, validation_details
    
    def test_sql_syntax(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Test SQL syntax by attempting to parse it (basic check)
        
        Args:
            sql: SQL query string
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Basic syntax checks
        sql_upper = sql.upper().strip()
        
        # Must start with SELECT
        if not sql_upper.startswith('SELECT'):
            return False, "SQL must start with SELECT"
        
        # Check for balanced quotes
        single_quotes = sql.count("'") - sql.count("\\'")
        if single_quotes % 2 != 0:
            return False, "Unbalanced single quotes"
        
        double_quotes = sql.count('"') - sql.count('\\"')
        if double_quotes % 2 != 0:
            return False, "Unbalanced double quotes"
        
        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            return False, "Unbalanced parentheses"
        
        return True, None
