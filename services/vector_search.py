"""
Vector Search Service
Integrates with Databricks UC Function: standard_code_lookup
"""

from databricks.sdk import WorkspaceClient
from typing import List, Dict
from config import config
import logging

logger = logging.getLogger(__name__)


class VectorSearchService:
    def __init__(self):
        self.w = WorkspaceClient(
            host=config.host,
            token=config.token
        )
    
    def search_codes(self, clinical_text: str, limit: int = 10) -> List[Dict]:
        """
        Call UC Function: standard_code_lookup
        
        Args:
            clinical_text: Natural language description of condition
            limit: Maximum number of codes to return
        
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
        
        try:
            # Call UC Function
            result = self.w.functions.execute(
                name=config.vector_function_fqn,
                parameters=[
                    {"name": "query_text", "value": clinical_text},
                    {"name": "limit", "value": str(limit)}
                ]
            )
            
            # Parse results
            codes = []
            if result.value:
                # Assuming function returns JSON array
                import json
                results = json.loads(result.value) if isinstance(result.value, str) else result.value
                
                for item in results:
                    codes.append({
                        'code': item['source_code'],
                        'description': item['concept_name'],
                        'vocabulary': item['vocabulary_id'],
                        'confidence': item.get('similarity_score', 0) * 100,  # Convert to percentage
                        'reason': f"Match based on semantic similarity to '{clinical_text}'"
                    })
            
            logger.info(f"Vector search returned {len(codes)} codes")
            return codes
            
        except Exception as e:
            logger.error(f"Vector search failed: {str(e)}")
            # Return empty list on error rather than failing
            return []


# Example usage
if __name__ == "__main__":
    vector_service = VectorSearchService()
    codes = vector_service.search_codes("acute myocardial infarction LAD")
    
    print(f"Found {len(codes)} codes:")
    for code in codes:
        print(f"  {code['code']} - {code['description']} ({code['confidence']:.1f}%)")


