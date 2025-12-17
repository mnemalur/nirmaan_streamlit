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
        if not config.token:
            raise ValueError("DATABRICKS_TOKEN is required")
        if not config.warehouse_id:
            raise ValueError("SQL_WAREHOUSE_ID is required for vector search")

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
        sql = f"SELECT * FROM {self.function_fqn}(query_text => '{escaped}')"
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
                    # Map expected columns from the function result
                    codes.append(
                        {
                            "code": rec.get("source_code"),
                            "description": rec.get("concept_name"),
                            "vocabulary": rec.get("vocabulary_id"),
                            "confidence": (rec.get("similarity_score") or 0) * 100,
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


