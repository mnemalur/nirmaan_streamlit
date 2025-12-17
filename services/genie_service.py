"""
Databricks Genie Service with Polling
Handles Text-to-SQL generation using Genie API
"""

from databricks.sdk import WorkspaceClient
from typing import Dict, Optional
from config import config
import time
import logging

from enum import Enum

# MessageStatus enum - use string values for compatibility
class MessageStatus:
    """Message status constants for Genie API"""
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    EXECUTING_QUERY = "EXECUTING_QUERY"
    FETCHING_METADATA = "FETCHING_METADATA"
    QUERYING = "QUERYING"
    RUNNING = "RUNNING"

logger = logging.getLogger(__name__)


class GenieService:
    def __init__(self):
        # Validate config before creating client
        if not config.host:
            raise ValueError("DATABRICKS_HOST is required")
        if not config.token:
            raise ValueError("DATABRICKS_TOKEN is required")
        
        # Clear any OAuth env vars that might interfere
        import os
        oauth_vars = ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET', 
                     'DATABRICKS_OAUTH_CLIENT_ID', 'DATABRICKS_OAUTH_CLIENT_SECRET']
        for var in oauth_vars:
            if var in os.environ:
                logger.warning(f"Removing OAuth env var {var} to use token auth only")
                del os.environ[var]
        
        # Explicitly use only token authentication
        # WorkspaceClient will prefer OAuth if client_id is in env, so we cleared it above
        self.w = WorkspaceClient(
            host=config.host,
            token=config.token
        )
        self.space_id = config.space_id
        self.max_poll_attempts = 60  # 60 attempts * 2 seconds = 2 minutes max
        self.poll_interval = 2  # seconds
    
    def create_cohort_query(self, criteria: Dict) -> Dict:
        """Build the Genie prompt and (in preview mode) return it without calling Genie.

        This lets us show the enriched, code-aware request to the user first,
        before we actually hit the Genie API or materialize anything.
        """
        
        # Build natural language query / prompt for Genie
        nl_query = self._build_nl_query(criteria)
        
        logger.info(f"[PREVIEW] Genie prompt built (not sent): {nl_query}")
        
        # In preview mode, don't call the remote Genie service
        return {
            'sql': None,
            'conversation_id': None,
            'execution_time': None,
            'row_count': 0,
            'prompt': nl_query,
        }
    
    def follow_up_question(self, conversation_id: str, question: str) -> Dict:
        """
        Continue existing Genie conversation with polling
        
        Args:
            conversation_id: Existing conversation ID
            question: Follow-up question
        
        Returns:
            {
                'sql': 'SELECT ...',
                'data': [...],
                'execution_time': 1.2
            }
        """
        
        logger.info(f"Sending follow-up to Genie: {question}")
        
        # Create follow-up message
        message = self.w.genie.create_message(
            space_id=self.space_id,
            content=question,
            conversation_id=conversation_id
        )
        
        # Poll for completion
        result = self._poll_for_completion(conversation_id, message.id)
        
        return {
            'sql': result['sql'],
            'data': result.get('data', []),
            'execution_time': result.get('execution_time')
        }
    
    def _poll_for_completion(self, conversation_id: str, message_id: str) -> Dict:
        """
        Poll Genie message status until complete
        
        Returns:
            {
                'sql': '...',
                'data': [...],
                'execution_time': 2.5,
                'row_count': 237
            }
        """
        
        for attempt in range(self.max_poll_attempts):
            try:
                # Get message status
                message = self.w.genie.get_message(
                    space_id=self.space_id,
                    conversation_id=conversation_id,
                    message_id=message_id
                )
                
                status = message.status
                logger.info(f"Genie status (attempt {attempt + 1}): {status}")
                
                # Check if completed
                if status == MessageStatus.COMPLETED:
                    logger.info("Genie query completed successfully")
                    return self._extract_result(message)
                
                # Check if failed
                elif status == MessageStatus.FAILED:
                    error_msg = getattr(message, 'error', 'Unknown error')
                    logger.error(f"Genie query failed: {error_msg}")
                    raise Exception(f"Genie query failed: {error_msg}")
                
                # Check if cancelled
                elif status == MessageStatus.CANCELLED:
                    logger.error("Genie query was cancelled")
                    raise Exception("Genie query was cancelled")
                
                # Still running - wait and retry
                elif status in [MessageStatus.EXECUTING_QUERY, MessageStatus.FETCHING_METADATA, 
                               MessageStatus.QUERYING, MessageStatus.RUNNING]:
                    logger.info(f"Genie still processing, waiting {self.poll_interval}s...")
                    time.sleep(self.poll_interval)
                    continue
                
                else:
                    # Unknown status, keep waiting
                    logger.warning(f"Unknown Genie status: {status}, continuing to poll...")
                    time.sleep(self.poll_interval)
                    continue
                    
            except Exception as e:
                logger.error(f"Error polling Genie: {str(e)}")
                if attempt == self.max_poll_attempts - 1:
                    raise Exception(f"Genie query timeout after {self.max_poll_attempts * self.poll_interval}s")
                time.sleep(self.poll_interval)
        
        # Timeout
        raise Exception(f"Genie query timeout after {self.max_poll_attempts * self.poll_interval} seconds")
    
    def _extract_result(self, message) -> Dict:
        """Extract SQL and results from completed Genie message"""
        
        result = {
            'sql': None,
            'data': [],
            'execution_time': None,
            'row_count': 0
        }
        
        # Extract SQL from attachments
        if hasattr(message, 'attachments') and message.attachments:
            for attachment in message.attachments:
                if hasattr(attachment, 'query') and attachment.query:
                    query = attachment.query
                    
                    # Get SQL text
                    if hasattr(query, 'query') and query.query:
                        result['sql'] = query.query
                    
                    # Get execution time
                    if hasattr(query, 'duration'):
                        result['execution_time'] = query.duration
                    
                    # Get result data
                    if hasattr(query, 'result') and query.result:
                        if hasattr(query.result, 'data_array'):
                            result['data'] = query.result.data_array or []
                            result['row_count'] = len(result['data'])
        
        if not result['sql']:
            # Fallback: check if SQL is in message text
            if hasattr(message, 'text') and message.text:
                result['sql'] = self._extract_sql_from_text(message.text)
        
        return result
    
    def _extract_sql_from_text(self, text: str) -> Optional[str]:
        """Extract SQL from markdown code blocks in message text"""
        
        import re
        
        # Look for SQL in code blocks
        sql_pattern = r'```sql\n(.*?)\n```'
        matches = re.findall(sql_pattern, text, re.DOTALL | re.IGNORECASE)
        
        if matches:
            return matches[0].strip()
        
        # Look for any code block
        code_pattern = r'```\n(.*?)\n```'
        matches = re.findall(code_pattern, text, re.DOTALL)
        
        if matches:
            return matches[0].strip()
        
        return None
    
    def _build_nl_query(self, criteria: Dict) -> str:
        """Build a *structured* request for Genie using vector-search codes.

        We assume Genie already knows your data model and joins, so we do NOT
        tell it which tables or columns to use. Instead we:
        - Restate the user's original intent
        - Provide the exact standard codes (with descriptions) that should
          represent that intent
        - Provide simple, high-level filters (timeframe, age, etc.)
        """

        codes = criteria.get("codes", []) or []
        # Either a single vocabulary or a list of vocabularies / coding systems
        vocabularies = criteria.get("vocabularies") or []
        vocabulary = criteria.get("vocabulary", "ICD10CM")
        original_query = criteria.get("original_query") or ""
        code_details = criteria.get("code_details", []) or []
        timeframe = criteria.get("timeframe")
        age_filter = criteria.get("age")

        # Build a structured, Genie-friendly prompt
        lines: list[str] = []

        lines.append(
            "You are a SQL generator for our clinical data warehouse. "
            "You already know the schema, table relationships, and best practices "
            "for querying it. Generate a single SQL query that returns the patient "
            "cohort described below."
        )

        if original_query:
            lines.append("")
            lines.append("User's original clinical intent:")
            lines.append(f"- {original_query}")

        if code_details or codes:
            lines.append("")
            if vocabularies:
                lines.append(
                    "Diagnosis/code signal from semantic vector search over the "
                    f"following vocabularies/coding systems: {', '.join(vocabularies)}."
                )
            else:
                lines.append(
                    f"Diagnosis code signal (semantic vector search, vocabulary={vocabulary}):"
                )
            if code_details:
                for c in code_details:
                    code = c.get("code")
                    desc = c.get("description") or ""
                    vocab = c.get("vocabulary") or vocabulary
                    if code:
                        lines.append(f"- code={code}, description={desc}, vocabulary={vocab}")
            elif codes:
                # Fallback: we only have raw codes
                lines.append(f"- codes: {', '.join(codes)}")

        # High-level filters
        lines.append("")
        lines.append("Additional cohort filters (high-level, you choose columns/tables):")
        if timeframe:
            lines.append(f"- timeframe: within the last {timeframe}")
        else:
            lines.append("- timeframe: not specified")

        if age_filter:
            lines.append(f"- age filter: {age_filter}")
        else:
            lines.append("- age filter: not specified")

        # Final instructions
        lines.append("")
        lines.append("Requirements for the SQL you generate:")
        lines.append("- Use the appropriate tables and joins from our existing data model.")
        lines.append("- Use EXACTLY the diagnosis codes listed above for the primary condition filter.")
        lines.append("- Apply the additional filters where appropriate.")
        lines.append("- Return a coherent patient cohort result set (one row per patient/encounter as appropriate).")

        return "\n".join(lines)


# Example usage
if __name__ == "__main__":
    # Test Genie service
    genie = GenieService()
    
    criteria = {
        'codes': ['I21.09', 'I21.01'],
        'timeframe': '30 days',
        'age': '> 50',
        'labs': []
    }
    
    try:
        result = genie.create_cohort_query(criteria)
        print("SQL Generated:")
        print(result['sql'])
        print(f"\nExecution time: {result['execution_time']}s")
        print(f"Row count: {result['row_count']}")
    except Exception as e:
        print(f"Error: {e}")


