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
        self.max_poll_attempts = 120  # 120 attempts * 2 seconds = 4 minutes max (Genie can take time)
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
            
            # Debug: Log the response structure to understand what we're getting
            logger.info(f"Response type: {type(response)}")
            logger.info(f"Response attributes: {[a for a in dir(response) if not a.startswith('_')]}")
            try:
                if hasattr(response, '__dict__'):
                    logger.info(f"Response __dict__ keys: {list(response.__dict__.keys())}")
                if hasattr(response, 'as_dict'):
                    resp_dict = response.as_dict()
                    logger.info(f"Response as_dict keys: {list(resp_dict.keys())}")
            except Exception as e:
                logger.debug(f"Could not inspect response structure: {e}")
            
            # Extract conversation_id and message_id from response
            # The response structure may vary, so try multiple attribute paths
            conversation_id = getattr(response, "conversation_id", None) or getattr(response, "id", None)
            message_id = getattr(response, "message_id", None)
            
            logger.info(f"Initial extraction - conversation_id: {conversation_id}, message_id: {message_id}")
            
            # Try alternative response structures
            if not conversation_id:
                if hasattr(response, "conversation"):
                    conv_obj = response.conversation
                    conversation_id = getattr(conv_obj, "id", None) or getattr(conv_obj, "conversation_id", None)
                    logger.info(f"Found conversation_id from response.conversation: {conversation_id}")
            
            if not message_id:
                if hasattr(response, "message"):
                    msg_obj = response.message
                    message_id = getattr(msg_obj, "id", None)
                    if not conversation_id:
                        conversation_id = getattr(msg_obj, "conversation_id", None)
                    logger.info(f"Found message_id from response.message: {message_id}")
                elif hasattr(response, "id") and not conversation_id:
                    # If we don't have conversation_id yet, response.id might be it
                    conversation_id = response.id
                    logger.info(f"Using response.id as conversation_id: {conversation_id}")
            
            # Try to get values from response as dict if available
            try:
                if hasattr(response, 'as_dict'):
                    resp_dict = response.as_dict()
                    logger.info(f"Response as_dict: {resp_dict}")
                    if not conversation_id and 'conversation_id' in resp_dict:
                        conversation_id = resp_dict['conversation_id']
                    if not message_id and 'message_id' in resp_dict:
                        message_id = resp_dict['message_id']
                    if not conversation_id and 'id' in resp_dict:
                        conversation_id = resp_dict['id']
            except Exception as e:
                logger.debug(f"Could not convert response to dict: {e}")
            
            logger.info(f"Final extraction - conversation_id: {conversation_id}, message_id: {message_id}")
            
            # Get the message object for polling
            # The response might already contain the message, or we need to fetch it
            message = None
            if hasattr(response, 'message') and response.message:
                message = response.message
                logger.info("Using response.message for polling")
            elif conversation_id and message_id:
                # Fetch the message separately for polling
                try:
                    message = self.w.genie.get_message(
                        space_id=self.space_id,
                        conversation_id=conversation_id,
                        message_id=message_id
                    )
                    logger.info("Fetched message separately for polling")
                except Exception as e:
                    logger.warning(f"Could not fetch message separately: {e}. Will try using response directly.")
                    message = response
            else:
                # Fallback: use response as message if it has the necessary attributes
                message = response
                logger.info("Using response directly as message object")
                
        except Exception as e:
            error_msg = f"Failed to start Genie conversation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # Include full traceback for debugging
            import traceback
            full_trace = traceback.format_exc()
            logger.error(f"Full traceback:\n{full_trace}")
            
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
            # Provide detailed error with response structure info
            response_info = f"Response type: {type(response)}, attributes: {[a for a in dir(response) if not a.startswith('_')]}"
            try:
                if hasattr(response, '__dict__'):
                    response_info += f", __dict__: {response.__dict__}"
                if hasattr(response, 'as_dict'):
                    response_info += f", as_dict: {response.as_dict()}"
            except:
                pass
            raise ValueError(
                f"Could not determine conversation_id from Genie start_conversation response. "
                f"{response_info}"
            )
        
        if not message:
            raise ValueError(
                f"Could not get message object from Genie start_conversation response. "
                f"conversation_id={conversation_id}, message_id={message_id}"
            )
        
        message_id = getattr(message, "id", None)
        if not message_id:
            # Try alternative ways to get message_id
            if hasattr(message, "message_id"):
                message_id = message.message_id
            elif hasattr(message, "__dict__") and "id" in message.__dict__:
                message_id = message.__dict__["id"]
            
            if not message_id:
                raise ValueError(
                    f"Could not determine message_id from Genie response. "
                    f"Message type: {type(message)}, attributes: {[a for a in dir(message) if not a.startswith('_')]}"
                )
        
        logger.info(f"Successfully started Genie conversation: {conversation_id}, message: {message_id}")

        # Poll until the message completes
        # Use the message_id we extracted, not message.id (which might not exist)
        result = self._poll_for_completion(conversation_id, message_id)

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
        
        logger.info(f"Starting to poll for completion: conversation_id={conversation_id}, message_id={message_id}")
        
        for attempt in range(self.max_poll_attempts):
            try:
                # Get message status
                message = self.w.genie.get_message(
                    space_id=self.space_id,
                    conversation_id=conversation_id,
                    message_id=message_id
                )
                
                # Extract status - try multiple ways
                status = None
                if hasattr(message, 'status'):
                    status = message.status
                elif hasattr(message, '__dict__') and 'status' in message.__dict__:
                    status = message.__dict__['status']
                
                if not status:
                    logger.warning(f"Could not determine status from message. Message attributes: {[a for a in dir(message) if not a.startswith('_')]}")
                    # If we can't get status, try to extract result anyway (maybe it's already done)
                    try:
                        result = self._extract_result(message)
                        if result.get('sql'):
                            logger.info("Found SQL in message even though status is unknown - assuming completed")
                            return result
                    except Exception as e:
                        logger.debug(f"Could not extract result: {e}")
                
                logger.info(f"Genie status (attempt {attempt + 1}/{self.max_poll_attempts}): {status}")
                
                # Check if completed (try multiple status string formats)
                status_str = str(status).upper() if status else ""
                if status == MessageStatus.COMPLETED or status_str == "COMPLETED":
                    logger.info("Genie query completed successfully")
                    return self._extract_result(message)
                
                # Check if failed
                elif status == MessageStatus.FAILED or status_str == "FAILED":
                    error_msg = getattr(message, 'error', None) or getattr(message, 'error_message', 'Unknown error')
                    logger.error(f"Genie query failed: {error_msg}")
                    raise Exception(f"Genie query failed: {error_msg}")
                
                # Check if cancelled
                elif status == MessageStatus.CANCELLED or status_str == "CANCELLED":
                    logger.error("Genie query was cancelled")
                    raise Exception("Genie query was cancelled")
                
                # Still running - wait and retry
                elif status in [MessageStatus.EXECUTING_QUERY, MessageStatus.FETCHING_METADATA, 
                               MessageStatus.QUERYING, MessageStatus.RUNNING] or \
                     status_str in ["EXECUTING_QUERY", "FETCHING_METADATA", "QUERYING", "RUNNING", "PROCESSING"]:
                    if attempt % 5 == 0:  # Log every 5th attempt to reduce log noise
                        logger.info(f"Genie still processing (status: {status}), waiting {self.poll_interval}s... ({attempt + 1}/{self.max_poll_attempts})")
                    time.sleep(self.poll_interval)
                    continue
                
                else:
                    # Unknown status - try to extract result anyway (maybe it's done but status is unexpected)
                    logger.warning(f"Unknown Genie status: {status}, attempting to extract result anyway...")
                    try:
                        result = self._extract_result(message)
                        if result.get('sql'):
                            logger.info(f"Successfully extracted result despite unknown status: {status}")
                            return result
                    except Exception as e:
                        logger.debug(f"Could not extract result with unknown status: {e}")
                    
                    # If we can't extract result, keep waiting
                    logger.info(f"Continuing to poll with unknown status: {status}")
                    time.sleep(self.poll_interval)
                    continue
                    
            except Exception as e:
                error_str = str(e)
                # Don't retry on certain errors (like failed/cancelled)
                if "failed" in error_str.lower() or "cancelled" in error_str.lower():
                    raise
                    
                logger.error(f"Error polling Genie (attempt {attempt + 1}): {error_str}")
                if attempt == self.max_poll_attempts - 1:
                    raise Exception(f"Genie query timeout after {self.max_poll_attempts * self.poll_interval}s. Last error: {error_str}")
                time.sleep(self.poll_interval)
        
        # Timeout
        raise Exception(f"Genie query timeout after {self.max_poll_attempts * self.poll_interval} seconds ({self.max_poll_attempts} attempts)")
    
    def _extract_result(self, message) -> Dict:
        """Extract SQL and results from completed Genie message"""
        
        logger.info("Extracting result from Genie message...")
        logger.info(f"Message type: {type(message)}")
        logger.info(f"Message attributes: {[a for a in dir(message) if not a.startswith('_')]}")
        
        result = {
            'sql': None,
            'data': [],
            'execution_time': None,
            'row_count': 0
        }
        
        # Try to get message as dict for easier inspection
        message_dict = None
        try:
            if hasattr(message, 'as_dict'):
                message_dict = message.as_dict()
                logger.info(f"Message as_dict keys: {list(message_dict.keys())}")
            elif hasattr(message, '__dict__'):
                message_dict = message.__dict__
                logger.info(f"Message __dict__ keys: {list(message_dict.keys())}")
        except Exception as e:
            logger.debug(f"Could not convert message to dict: {e}")
        
        # Extract SQL from attachments (primary method)
        if hasattr(message, 'attachments') and message.attachments:
            logger.info(f"Found {len(message.attachments)} attachment(s)")
            for idx, attachment in enumerate(message.attachments):
                logger.info(f"Attachment {idx} type: {type(attachment)}, attributes: {[a for a in dir(attachment) if not a.startswith('_')]}")
                
                if hasattr(attachment, 'query') and attachment.query:
                    query = attachment.query
                    logger.info(f"Found query in attachment {idx}")
                    
                    # Get SQL text
                    if hasattr(query, 'query') and query.query:
                        result['sql'] = query.query
                        logger.info(f"Extracted SQL from query.query: {len(result['sql'])} chars")
                    
                    # Try alternative SQL locations
                    if not result['sql']:
                        if hasattr(query, 'sql'):
                            result['sql'] = query.sql
                            logger.info(f"Extracted SQL from query.sql")
                        elif hasattr(query, 'text'):
                            result['sql'] = query.text
                            logger.info(f"Extracted SQL from query.text")
                    
                    # Get execution time
                    if hasattr(query, 'duration'):
                        result['execution_time'] = query.duration
                        logger.info(f"Extracted execution_time: {result['execution_time']}")
                    elif hasattr(query, 'execution_time'):
                        result['execution_time'] = query.execution_time
                    
                    # Get result data
                    if hasattr(query, 'result') and query.result:
                        result_obj = query.result
                        logger.info(f"Found result object, type: {type(result_obj)}")
                        
                        if hasattr(result_obj, 'data_array'):
                            result['data'] = result_obj.data_array or []
                            result['row_count'] = len(result['data'])
                            logger.info(f"Extracted {result['row_count']} rows from data_array")
                        elif hasattr(result_obj, 'data'):
                            result['data'] = result_obj.data or []
                            result['row_count'] = len(result['data'])
                            logger.info(f"Extracted {result['row_count']} rows from data")
                        elif hasattr(result_obj, 'rows'):
                            result['data'] = result_obj.rows or []
                            result['row_count'] = len(result['data'])
                            logger.info(f"Extracted {result['row_count']} rows from rows")
        
        # Fallback: check if SQL is in message text/content
        if not result['sql']:
            logger.info("SQL not found in attachments, checking message text/content...")
            if hasattr(message, 'text') and message.text:
                result['sql'] = self._extract_sql_from_text(message.text)
                if result['sql']:
                    logger.info(f"Extracted SQL from message.text: {len(result['sql'])} chars")
            elif hasattr(message, 'content') and message.content:
                result['sql'] = self._extract_sql_from_text(message.content)
                if result['sql']:
                    logger.info(f"Extracted SQL from message.content")
        
        # Fallback: check message_dict directly
        if not result['sql'] and message_dict:
            logger.info("Checking message_dict for SQL...")
            if 'sql' in message_dict:
                result['sql'] = message_dict['sql']
            elif 'query' in message_dict:
                query_val = message_dict['query']
                if isinstance(query_val, str):
                    result['sql'] = query_val
                elif isinstance(query_val, dict) and 'query' in query_val:
                    result['sql'] = query_val['query']
        
        # Log final result
        logger.info(f"Extraction complete - SQL: {'Found' if result['sql'] else 'Not found'}, "
                   f"Rows: {result['row_count']}, Execution time: {result['execution_time']}")
        
        if not result['sql']:
            logger.warning("Could not extract SQL from Genie message. Message structure may be unexpected.")
        
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


