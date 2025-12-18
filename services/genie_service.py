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
    
    def check_genie_health(self) -> tuple[bool, str]:
        """
        Check if Genie service is active and accessible.
        
        Returns:
            (success: bool, message: str)
        """
        try:
            # 1. Verify authentication
            try:
                current_user = self.w.current_user.me()
                user_name = getattr(current_user, 'user_name', 'unknown')
                logger.info(f"Authentication verified for user: {user_name}")
            except Exception as e:
                return False, f"Authentication failed: {str(e)}. Please check your DATABRICKS_TOKEN."
            
            # 2. Verify Genie API is available
            if not hasattr(self.w, 'genie'):
                return False, "Genie API is not available in this Databricks SDK version."
            
            if not hasattr(self.w.genie, 'start_conversation'):
                return False, "Genie 2.0 API (start_conversation) is not available. Please update your Databricks SDK."
            
            # 3. Verify space_id is configured
            if not self.space_id:
                return False, "GENIE_SPACE_ID is not configured. Please set it in your environment or configuration."
            
            # 4. Try to verify access to the space (by attempting to list conversations or checking space)
            # Note: Some SDKs may have a list_spaces or get_space method
            try:
                # Try to access space info if available
                if hasattr(self.w.genie, 'get_space'):
                    try:
                        space = self.w.genie.get_space(space_id=self.space_id)
                        logger.info(f"Verified access to Genie space: {self.space_id}")
                    except Exception as e:
                        logger.warning(f"Could not verify space access directly: {e}. Continuing anyway...")
                elif hasattr(self.w.genie, 'list_spaces'):
                    # Try to list spaces to verify API access
                    try:
                        spaces = self.w.genie.list_spaces()
                        logger.info(f"Genie API is accessible. Found {len(list(spaces)) if spaces else 0} space(s).")
                    except Exception as e:
                        logger.warning(f"Could not list spaces: {e}. Continuing anyway...")
            except Exception as e:
                logger.warning(f"Space verification check failed: {e}. Continuing anyway...")
            
            # 5. All checks passed
            return True, f"Genie service is active and accessible. Space ID: {self.space_id}"
            
        except Exception as e:
            error_msg = f"Genie health check failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def create_cohort_query(self, criteria: Dict) -> Dict:
        """Create a new Genie conversation for a cohort query and poll until complete.

        This uses the refined, code-aware natural language criteria to ask Genie
        to generate SQL, execute it against the warehouse, and return results.
        Because this can take time, we poll the message status until it is
        COMPLETED or times out.
        """
        
        # Health check: Ensure Genie is active before attempting to use it
        logger.info("Checking Genie service health before starting conversation...")
        is_healthy, health_message = self.check_genie_health()
        if not is_healthy:
            raise ValueError(f"Genie service is not available: {health_message}")
        logger.info(f"Genie health check passed: {health_message}")
        
        # Build natural language query / prompt for Genie
        nl_query = self._build_nl_query(criteria)
        logger.info(f"Sending cohort request to Genie (space_id={self.space_id}): {nl_query}")

        # Use start_conversation for Genie 2.0 API (required method for new conversations)
        if not hasattr(self.w.genie, 'start_conversation'):
            raise ValueError(
                "start_conversation method not available. This code requires Genie 2.0 API. "
                "Please ensure you're using a compatible version of the Databricks SDK."
            )
        
        try:
            logger.info("Using start_conversation (Genie 2.0 API) to create new conversation")
            response = self.w.genie.start_conversation(
                space_id=self.space_id,
                content=nl_query,
            )
            
            # Extract conversation_id and message_id from response
            # The response structure may vary, so try multiple attribute paths
            conversation_id = getattr(response, "conversation_id", None) or getattr(response, "id", None)
            message_id = getattr(response, "message_id", None)
            
            # Try alternative response structures
            if not conversation_id:
                if hasattr(response, "conversation"):
                    conv_obj = response.conversation
                    conversation_id = getattr(conv_obj, "id", None) or getattr(conv_obj, "conversation_id", None)
            
            if not message_id:
                if hasattr(response, "message"):
                    msg_obj = response.message
                    message_id = getattr(msg_obj, "id", None)
                    if not conversation_id:
                        conversation_id = getattr(msg_obj, "conversation_id", None)
                elif hasattr(response, "id"):
                    message_id = response.id
            
            logger.info(f"Started new Genie conversation: {conversation_id}, message: {message_id}")
            
            # Get the message object for polling
            # The response might already contain the message, or we need to fetch it
            if hasattr(response, 'message') and response.message:
                message = response.message
            elif conversation_id and message_id:
                # Fetch the message separately for polling
                message = self.w.genie.get_message(
                    space_id=self.space_id,
                    conversation_id=conversation_id,
                    message_id=message_id
                )
            else:
                # Fallback: use response as message if it has the necessary attributes
                message = response
                
        except Exception as e:
            error_msg = f"Failed to start Genie conversation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # Check if it's an authentication/permission error
            if "does not own" in str(e).lower() or "permission" in str(e).lower() or "unauthorized" in str(e).lower():
                raise ValueError(
                    f"Authentication or permission error: {error_msg}\n"
                    f"Please verify:\n"
                    f"1. Your DATABRICKS_TOKEN has access to the Genie space (space_id={self.space_id})\n"
                    f"2. The GENIE_SPACE_ID is correct\n"
                    f"3. Your user has permissions to create conversations in this space"
                )
            raise ValueError(f"Cannot start Genie conversation. {error_msg}")

        if not conversation_id:
            raise ValueError("Could not determine conversation_id from Genie start_conversation response")
        
        if not message:
            raise ValueError("Could not get message object from Genie start_conversation response")
        
        message_id = getattr(message, "id", None)
        if not message_id:
            raise ValueError("Could not determine message_id from Genie response")
        
        logger.info(f"Successfully started Genie conversation: {conversation_id}, message: {message_id}")

        # Poll until the message completes
        result = self._poll_for_completion(conversation_id, message.id)

        return {
            'sql': result.get('sql'),
            'data': result.get('data', []),
            'execution_time': result.get('execution_time'),
            'row_count': result.get('row_count', 0),
            'conversation_id': conversation_id,
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


