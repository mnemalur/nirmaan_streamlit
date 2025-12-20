"""
Vector Search Service
Integrates with Databricks UC table function: standard_code_lookup

We call the function through the SQL Warehouse using databricks-sql-connector,
so we don't depend on a specific `functions.execute` method on the Python SDK.
"""

from typing import List, Dict
import logging

from databricks.sql import connect
from config import config

logger = logging.getLogger(__name__)


class VectorSearchService:
    def __init__(self):
        # Validate config before creating client
        if not config.host:
            raise ValueError("DATABRICKS_HOST is required")
        if not config.warehouse_id:
            raise ValueError("SQL_WAREHOUSE_ID is required for vector search")
        
        # In Databricks runtime, token can be obtained from runtime context
        # For local development, token is required
        if not config.is_databricks_runtime and not config.token:
            raise ValueError("DATABRICKS_TOKEN is required when running outside Databricks")
        
        # Get token - use runtime context in Databricks if available
        self.token = config.token
        if config.is_databricks_runtime and not self.token:
            # Try to get token from dbutils if available
            try:
                from pyspark.dbutils import DBUtils
                from pyspark import SparkContext
                spark_context = SparkContext.getOrCreate()
                dbutils = DBUtils(spark_context)
                # Try to get token from secrets
                try:
                    self.token = dbutils.secrets.get(scope="tokens", key="databricks_token")
                except:
                    # If no secret, try to get from environment or use None (runtime will handle)
                    self.token = None
                    logger.warning("No DATABRICKS_TOKEN found, using Databricks runtime authentication")
            except:
                self.token = None
                logger.warning("Could not access dbutils, using Databricks runtime authentication")

        self.server_hostname = config.host.replace("https://", "").replace("http://", "")
        self.http_path = f"/sql/1.0/warehouses/{config.warehouse_id}"
        self.function_fqn = config.vector_function_fqn

    def search_codes(self, clinical_text: str, limit: int = 10) -> List[Dict]:
        """
        Call UC table function: standard_code_lookup via SQL Warehouse.

        Args:
            clinical_text: Natural language description of condition.
            limit: Unused here; the vector function itself is expected to enforce
                   any row limit internally.

        Returns:
            List of dicts with: code, description, vocabulary, confidence
            [
                {
                    'code': 'I21.09',
                    'description': 'ST elevation MI involving LAD',
                    'vocabulary': 'ICD10CM',
                    'confidence': 95
                },
                ...
            ]
        """

        logger.info(f"Vector search for: {clinical_text}")

        # Basic escaping for single quotes in the input to keep the SQL valid.
        escaped = clinical_text.replace("'", "''") if clinical_text else ""

        # Treat the UC function as a table function and invoke it from SQL.
        # Your function is defined to be called like:
        #   SELECT concept_code, concept_name, vocabulary_id FROM <fqn>('retinopathy')
        # so we pass the clinical text as a single positional argument.
        sql = f"SELECT * FROM {self.function_fqn}('{escaped}')"
        logger.info(f"Executing vector search SQL via warehouse: {sql}")

        codes: List[Dict] = []

        with connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=config.token,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

                cols = [d[0] for d in cursor.description]
                for row in cursor.fetchall():
                    rec = dict(zip(cols, row))
                    # Map expected columns from the function result. Different implementations
                    # might use slightly different column names (e.g., concept_code vs source_code,
                    # search_score vs similarity_score), so we handle the common variants.
                    code_val = rec.get("concept_code") or rec.get("source_code")
                    desc_val = rec.get("concept_name") or rec.get("description")
                    vocab_val = rec.get("vocabulary_id") or rec.get("vocabulary")
                    # If your function already filters to the best matches and does
                    # not expose a score, we can treat them as 100%-confidence hits.
                    raw_score = rec.get("search_score")
                    if raw_score is None:
                        raw_score = rec.get("similarity_score")
                    confidence = 100 if raw_score is None else raw_score * 100

                    codes.append(
                        {
                            "code": code_val,
                            "description": desc_val,
                            "vocabulary": vocab_val,
                            "confidence": confidence,
                            "reason": f"Match based on semantic similarity to '{clinical_text}'",
                        }
                    )

        logger.info(f"Vector search returned {len(codes)} codes")
        return codes


# Example usage
if __name__ == "__main__":
    vector_service = VectorSearchService()
    codes = vector_service.search_codes("acute myocardial infarction LAD")

    print(f"Found {len(codes)} codes:")
    for code in codes:
        print(f"  {code['code']} - {code['description']} ({code['confidence']:.1f}%)")


