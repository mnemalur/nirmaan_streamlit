"""
Cohort Manager - Handles Delta table materialization and insights queries
Uses tables: patdemo, paticd, patcpt
"""

from databricks.sql import connect
from config import config
from typing import Dict, List
import time
import logging

logger = logging.getLogger(__name__)


class CohortManager:
    def __init__(self):
        """Initialize connection to SQL Warehouse"""
        self.connection = connect(
            server_hostname=config.host.replace("https://", "").replace("http://", ""),
            http_path=f"/sql/1.0/warehouses/{config.warehouse_id}",
            access_token=config.token
        )
    
    def materialize_cohort(self, session_id: str, cohort_sql: str) -> Dict:
        """
        Create temporary Delta table for cohort
        Handles billion-row scale by materializing query results
        
        Args:
            session_id: User session ID
            cohort_sql: SQL query from Genie (returns patient_id and other fields)
        
        Returns:
            {
                'cohort_table': 'delta.`/tmp/clinical_cohorts/cohort_xyz`',
                'cohort_path': '/tmp/clinical_cohorts/cohort_xyz',
                'cohort_id': 'cohort_xyz',
                'count': 237
            }
        """
        
        cohort_id = f"cohort_{session_id}_{int(time.time())}"
        cohort_path = f"/tmp/clinical_cohorts/{cohort_id}"
        cohort_table = f"delta.`{cohort_path}`"
        
        logger.info(f"Materializing cohort: {cohort_id}")
        
        cursor = self.connection.cursor()
        
        try:
            # Create Delta table from Genie's SQL
            create_sql = f"""
            CREATE OR REPLACE TABLE {cohort_table}
            USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.deletedFileRetentionDuration' = 'interval 1 days'
            )
            AS {cohort_sql}
            """
            
            logger.info(f"Executing materialization SQL")
            cursor.execute(create_sql)
            
            # Get count
            logger.info(f"Getting cohort count")
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {cohort_table}")
            count = cursor.fetchone()[0]
            
            logger.info(f"Cohort materialized: {count} patients")
            
            return {
                'cohort_table': cohort_table,
                'cohort_path': cohort_path,
                'cohort_id': cohort_id,
                'count': count
            }
            
        except Exception as e:
            logger.error(f"Error materializing cohort: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def get_demographics(self, cohort_table: str) -> Dict:
        """
        Get patient characteristics: Age, Gender, Race, Ethnicity
        Uses patdemo table
        Returns dict with age_gender, gender, race, ethnicity
        """
        
        # Age and Gender combined
        age_gender_sql = f"""
        SELECT 
            CASE 
                WHEN d.age BETWEEN 50 AND 60 THEN '50-60'
                WHEN d.age BETWEEN 61 AND 70 THEN '61-70'
                WHEN d.age BETWEEN 71 AND 80 THEN '71-80'
                ELSE '81+'
            END as age_group,
            d.gender,
            COUNT(DISTINCT c.patient_id) as count,
            ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / SUM(COUNT(DISTINCT c.patient_id)) OVER(), 1) as percentage
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        GROUP BY age_group, d.gender
        ORDER BY age_group, d.gender
        """
        
        # Gender only
        gender_sql = f"""
        SELECT 
            'Gender' as category,
            CASE WHEN d.gender = 'M' THEN 'Male' ELSE 'Female' END as value,
            COUNT(DISTINCT c.patient_id) as count,
            ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / SUM(COUNT(DISTINCT c.patient_id)) OVER(), 1) as percentage
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        GROUP BY d.gender
        ORDER BY d.gender
        """
        
        # Race
        race_sql = f"""
        SELECT 
            'Race' as category,
            d.race as value,
            COUNT(DISTINCT c.patient_id) as count,
            ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / SUM(COUNT(DISTINCT c.patient_id)) OVER(), 1) as percentage
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        WHERE d.race IS NOT NULL
        GROUP BY d.race
        ORDER BY count DESC
        """
        
        # Ethnicity
        ethnicity_sql = f"""
        SELECT 
            'Ethnicity' as category,
            d.ethnicity as value,
            COUNT(DISTINCT c.patient_id) as count,
            ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / SUM(COUNT(DISTINCT c.patient_id)) OVER(), 1) as percentage
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        WHERE d.ethnicity IS NOT NULL
        GROUP BY d.ethnicity
        ORDER BY count DESC
        """
        
        try:
            age_gender = self._execute_query(age_gender_sql)
            gender = self._execute_query(gender_sql)
        except Exception as e:
            logger.warning(f"Error getting age/gender data: {str(e)}")
            age_gender = []
            gender = []
        
        try:
            race = self._execute_query(race_sql)
        except Exception as e:
            logger.warning(f"Error getting race data: {str(e)}")
            race = []
        
        try:
            ethnicity = self._execute_query(ethnicity_sql)
        except Exception as e:
            logger.warning(f"Error getting ethnicity data: {str(e)}")
            ethnicity = []
        
        return {
            'age_gender': age_gender,
            'gender': gender,
            'race': race,
            'ethnicity': ethnicity
        }
    
    def get_site_breakdown(self, cohort_table: str) -> Dict:
        """
        Get site breakdown: Teaching/Non-teaching, Urban/Rural, Bed Count groups
        Uses patdemo table (assumes site-level attributes are in patdemo or joined table)
        Returns dict with teaching_status, urban_rural, bed_count
        """
        
        # Teaching vs Non-teaching
        teaching_sql = f"""
        SELECT 
            'Teaching Status' as category,
            CASE WHEN d.teaching_flag = 1 THEN 'Teaching' ELSE 'Non-Teaching' END as value,
            COUNT(DISTINCT c.patient_id) as patient_count,
            ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / SUM(COUNT(DISTINCT c.patient_id)) OVER(), 1) as percentage
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        GROUP BY d.teaching_flag
        ORDER BY patient_count DESC
        """
        
        # Urban vs Rural
        urban_rural_sql = f"""
        SELECT 
            'Location Type' as category,
            d.location_type as value,
            COUNT(DISTINCT c.patient_id) as patient_count,
            ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / SUM(COUNT(DISTINCT c.patient_id)) OVER(), 1) as percentage
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        WHERE d.location_type IN ('Urban', 'Rural')
        GROUP BY d.location_type
        ORDER BY patient_count DESC
        """
        
        # Bed count groups
        bed_count_sql = f"""
        SELECT 
            'Bed Count' as category,
            CASE 
                WHEN d.bed_count < 100 THEN 'Small (< 100 beds)'
                WHEN d.bed_count BETWEEN 100 AND 300 THEN 'Medium (100-300 beds)'
                ELSE 'Large (> 300 beds)'
            END as value,
            COUNT(DISTINCT c.patient_id) as patient_count,
            ROUND(COUNT(DISTINCT c.patient_id) * 100.0 / SUM(COUNT(DISTINCT c.patient_id)) OVER(), 1) as percentage
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        WHERE d.bed_count IS NOT NULL
        GROUP BY value
        ORDER BY patient_count DESC
        """
        
        try:
            teaching_status = self._execute_query(teaching_sql)
        except Exception as e:
            logger.warning(f"Error getting teaching status data: {str(e)}")
            teaching_status = []
        
        try:
            urban_rural = self._execute_query(urban_rural_sql)
        except Exception as e:
            logger.warning(f"Error getting urban/rural data: {str(e)}")
            urban_rural = []
        
        try:
            bed_count = self._execute_query(bed_count_sql)
        except Exception as e:
            logger.warning(f"Error getting bed count data: {str(e)}")
            bed_count = []
        
        return {
            'teaching_status': teaching_status,
            'urban_rural': urban_rural,
            'bed_count': bed_count
        }
    
    def get_comorbidities(self, cohort_table: str, top_n: int = 5) -> List[Dict]:
        """
        Get top comorbidities (secondary diagnoses)
        Uses paticd table
        """
        
        sql = f"""
        WITH cohort_diagnoses AS (
            SELECT 
                c.patient_id,
                icd.diagnosis_code,
                icd.diagnosis_description
            FROM {cohort_table} c
            JOIN {config.patient_table_prefix}.paticd icd 
                ON c.patient_id = icd.patient_id
            WHERE icd.diagnosis_type = 'secondary'
        )
        SELECT 
            diagnosis_code,
            diagnosis_description,
            COUNT(DISTINCT patient_id) as patient_count,
            ROUND(COUNT(DISTINCT patient_id) * 100.0 / (
                SELECT COUNT(DISTINCT patient_id) FROM {cohort_table}
            ), 1) as percentage
        FROM cohort_diagnoses
        GROUP BY diagnosis_code, diagnosis_description
        ORDER BY patient_count DESC
        LIMIT {top_n}
        """
        
        return self._execute_query(sql)
    
    def get_admission_trends(self, cohort_table: str) -> List[Dict]:
        """
        Get admission trends by week
        Uses patdemo table
        """
        
        sql = f"""
        SELECT 
            DATE_TRUNC('week', d.admission_date) as week_start,
            COUNT(DISTINCT c.patient_id) as admission_count
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        WHERE d.admission_date IS NOT NULL
        GROUP BY week_start
        ORDER BY week_start
        """
        
        return self._execute_query(sql)
    
    def get_outcomes(self, cohort_table: str) -> Dict:
        """
        Get key outcome metrics
        Uses patdemo table
        """
        
        sql = f"""
        SELECT 
            ROUND(AVG(d.length_of_stay), 1) as avg_los,
            ROUND(SUM(CASE WHEN d.readmission_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as readmission_rate,
            ROUND(SUM(CASE WHEN d.mortality_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as mortality_rate,
            ROUND(SUM(CASE WHEN d.complication_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as complication_rate
        FROM {cohort_table} c
        JOIN {config.patient_table_prefix}.patdemo d ON c.patient_id = d.patient_id
        """
        
        result = self._execute_query(sql)
        return result[0] if result else {}
    
    def cleanup_cohort(self, cohort_table: str):
        """Delete temporary cohort table"""
        
        logger.info(f"Cleaning up cohort table: {cohort_table}")
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {cohort_table}")
            logger.info(f"Cohort table deleted successfully")
        except Exception as e:
            logger.error(f"Error cleaning up cohort: {str(e)}")
        finally:
            cursor.close()
    
    def _execute_query(self, sql: str) -> List[Dict]:
        """
        Execute SQL and return results as list of dicts
        """
        
        cursor = self.connection.cursor()
        
        try:
            logger.info(f"Executing query: {sql[:100]}...")
            cursor.execute(sql)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows and convert to list of dicts
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            logger.info(f"Query returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def __del__(self):
        """Close connection on cleanup"""
        if hasattr(self, 'connection'):
            try:
                self.connection.close()
            except:
                pass



