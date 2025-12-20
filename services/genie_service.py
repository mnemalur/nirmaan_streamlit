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
        
        # In Databricks runtime, token is optional (uses workspace context)
        # For local development, token is required
        if not config.is_databricks_runtime and not config.token:
            raise ValueError("DATABRICKS_TOKEN is required when running outside Databricks")
        
        # Clear any OAuth env vars that might interfere
        import os
        oauth_vars = ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET', 
                     'DATABRICKS_OAUTH_CLIENT_ID', 'DATABRICKS_OAUTH_CLIENT_SECRET']
        for var in oauth_vars:
            if var in os.environ:
                logger.warning(f"Removing OAuth env var {var} to use token/runtime auth only")
                del os.environ[var]
        
        # Initialize WorkspaceClient
        # In Databricks runtime, ALWAYS use WorkspaceClient() without params to use workspace context
        # This avoids "public access not allowed" errors when running inside Databricks
        # Outside Databricks, we need explicit host and token from .env
        if config.is_databricks_runtime:
            # Running in Databricks - ALWAYS use workspace context authentication
            # Even if .env has token, we use workspace context to avoid public access errors
            self.w = WorkspaceClient()
            logger.info("Using Databricks workspace context authentication (ignoring .env token when in Databricks)")
        else:
            # Running locally - require explicit token from .env
            self.w = WorkspaceClient(
                host=config.host,
                token=config.token
            )
            logger.info("Using explicit token authentication from .env (local development)")
        
        self.space_id = config.space_id
        self.max_poll_attempts = 150  # 150 attempts * 2 seconds = 5 minutes max (Genie can take time)
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
        
        # Call start_conversation - catch only API call errors, not parsing errors
        try:
            logger.info("Using start_conversation (Genie 2.0 API) to create new conversation")
            response = self.w.genie.start_conversation(
                space_id=self.space_id,
                content=nl_query,
            )
            logger.info(f"start_conversation succeeded. Response type: {type(response)}")
        except Exception as e:
            # Only catch errors from the actual API call
            error_msg = f"Failed to call Genie start_conversation: {str(e)}"
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
        
        # Parse response - handle errors gracefully, don't fail the whole operation
        conversation_id = None
        message_id = None
        message = None
        
        try:
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
                try:
                    if hasattr(response, "conversation"):
                        conv_obj = response.conversation
                        conversation_id = getattr(conv_obj, "id", None) or getattr(conv_obj, "conversation_id", None)
                        logger.info(f"Found conversation_id from response.conversation: {conversation_id}")
                except Exception as e:
                    logger.debug(f"Error accessing response.conversation: {e}")
            
            if not message_id:
                try:
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
                except Exception as e:
                    logger.debug(f"Error accessing response.message or response.id: {e}")
            
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
            # Check if response has a message attribute that's not None/empty
            try:
                if hasattr(response, 'message'):
                    message_from_response = getattr(response, 'message', None)
                    # Check if it's not None and has some content
                    if message_from_response is not None:
                        # Check if it's not an empty object
                        if hasattr(message_from_response, '__dict__') and message_from_response.__dict__:
                            message = message_from_response
                            logger.info("Using response.message for polling")
                        elif hasattr(message_from_response, 'id') or hasattr(message_from_response, 'status'):
                            message = message_from_response
                            logger.info("Using response.message for polling (has id or status)")
            except Exception as e:
                logger.debug(f"Error accessing response.message: {e}")
            
            # If we don't have a message yet, try fetching it
            if not message and conversation_id and message_id:
                try:
                    logger.info(f"Fetching message separately: conversation_id={conversation_id}, message_id={message_id}")
                    message = self.w.genie.get_message(
                        space_id=self.space_id,
                        conversation_id=conversation_id,
                        message_id=message_id
                    )
                    logger.info("Successfully fetched message separately for polling")
                except Exception as e:
                    logger.warning(f"Could not fetch message separately: {e}. Will try using response directly.")
                    # Fallback: use response as message
                    message = response
                    logger.info("Using response directly as message object (fallback)")
            elif not message:
                # Last resort: check if response itself has message-like attributes
                # (start_conversation might return the message directly)
                if hasattr(response, 'status') or hasattr(response, 'id'):
                    message = response
                    logger.info("Using response directly as message object (has status or id)")
                else:
                    # Try to use response anyway - polling will handle it
                    message = response
                    logger.warning("Using response directly as message object (fallback - may not work)")
        except Exception as e:
            # Parsing errors shouldn't fail the whole operation - log and continue
            logger.warning(f"Error parsing Genie response structure: {e}. Will attempt to proceed with available data.")
            # Use response as message if we don't have one
            if not message:
                message = response

        # Validate we have what we need - but be lenient, we can try to get IDs during polling
        if not conversation_id:
            logger.warning(
                f"Could not determine conversation_id from Genie start_conversation response. "
                f"Response type: {type(response)}, attributes: {[a for a in dir(response) if not a.startswith('_')]}. "
                f"Will attempt to extract during polling."
            )
            # Try one more time to get conversation_id from response
            try:
                if hasattr(response, '__dict__'):
                    response_info = response.__dict__
                    if 'conversation_id' in response_info:
                        conversation_id = response_info['conversation_id']
                    elif 'id' in response_info:
                        conversation_id = response_info['id']
            except:
                pass
        
        if not message:
            logger.warning(
                f"Could not get message object from Genie start_conversation response. "
                f"Using response directly. conversation_id={conversation_id}, message_id={message_id}"
            )
            message = response  # Use response as message - polling will handle it
        
        # Validate message and extract message_id
        logger.info(f"Message object obtained: type={type(message)}")
        
        # Try to get message_id from the message object
        message_id_from_message = getattr(message, "id", None)
        if not message_id_from_message:
            # Try alternative ways to get message_id
            if hasattr(message, "message_id"):
                message_id_from_message = message.message_id
            elif hasattr(message, "__dict__") and "id" in message.__dict__:
                message_id_from_message = message.__dict__["id"]
        
        # Use message_id from message if we got it, otherwise use the one we extracted earlier
        if message_id_from_message:
            message_id = message_id_from_message
            logger.info(f"Using message_id from message object: {message_id}")
        elif message_id:
            logger.info(f"Using previously extracted message_id: {message_id}")
        else:
            # If we still don't have message_id, log warning but continue - polling might work without it
            logger.warning(
                f"Could not determine message_id from Genie message. "
                f"Message type: {type(message)}, attributes: {[a for a in dir(message) if not a.startswith('_')]}. "
                f"Will attempt polling with conversation_id only."
            )
            # Set a placeholder - polling will need to handle this
            message_id = "unknown"
        
        # Final validation - we need conversation_id to poll
        if not conversation_id:
            # Try one last desperate attempt - maybe response itself has it nested somewhere
            response_info = f"Response type: {type(response)}"
            try:
                if hasattr(response, '__dict__'):
                    response_info += f", __dict__ keys: {list(response.__dict__.keys())}"
                    # Check if any value in __dict__ might be a conversation_id
                    for key, value in response.__dict__.items():
                        if 'conversation' in key.lower() and value:
                            try:
                                if hasattr(value, 'id'):
                                    conversation_id = value.id
                                    logger.info(f"Found conversation_id from {key}.id: {conversation_id}")
                                    break
                            except:
                                pass
                if hasattr(response, 'as_dict'):
                    resp_dict = response.as_dict()
                    response_info += f", as_dict keys: {list(resp_dict.keys())}"
            except Exception as e:
                logger.debug(f"Final attempt to find conversation_id failed: {e}")
            
            if not conversation_id:
                # This is a real problem - we can't poll without conversation_id
                raise ValueError(
                    f"Could not determine conversation_id from Genie start_conversation response. "
                    f"This is required for polling. {response_info}. "
                    f"Please check the logs for the full response structure, or contact support."
                )
        
        logger.info(f"Successfully started Genie conversation: {conversation_id}, message_id: {message_id}")

        # Poll until the message completes
        # Use the message_id we extracted, not message.id (which might not exist)
        result = self._poll_for_completion(conversation_id, message_id)

        return {
            'sql': result.get('sql'),
            'data': result.get('data', []),
            'execution_time': result.get('execution_time'),
            'row_count': result.get('row_count', 0),
            'columns': result.get('columns'),  # Include column names for DataFrame display
            'conversation_id': conversation_id,
            'prompt': nl_query,
        }
    
    def start_cohort_query(self, criteria: Dict) -> Dict:
        """Start a Genie conversation for a cohort query WITHOUT polling.
        
        This is for conversational interfaces where we want to return quickly
        and let the user see progress. Returns immediately with conversation_id
        and prompt, but no SQL/data yet.
        
        Use poll_conversation_status() later to check for completion.
        """
        # Health check
        logger.info("Checking Genie service health before starting conversation...")
        is_healthy, health_message = self.check_genie_health()
        if not is_healthy:
            raise ValueError(f"Genie service is not available: {health_message}")
        logger.info(f"Genie health check passed: {health_message}")
        
        # Build natural language query
        nl_query = self._build_nl_query(criteria)
        logger.info(f"Starting Genie conversation (space_id={self.space_id}): {nl_query[:100]}...")
        
        # Start conversation
        if not hasattr(self.w.genie, 'start_conversation'):
            raise ValueError(
                "start_conversation method not available. This code requires Genie 2.0 API."
            )
        
        try:
            response = self.w.genie.start_conversation(
                space_id=self.space_id,
                content=nl_query,
            )
            
            # Extract conversation_id and message_id (same logic as create_cohort_query)
            conversation_id = None
            message_id = None
            
            if hasattr(response, 'conversation_id'):
                conversation_id = response.conversation_id
            elif hasattr(response, 'id'):
                conversation_id = response.id
            elif hasattr(response, '__dict__'):
                conversation_id = response.__dict__.get('conversation_id') or response.__dict__.get('id')
            
            if hasattr(response, 'message'):
                message = response.message
                if hasattr(message, 'id'):
                    message_id = message.id
                elif hasattr(message, 'message_id'):
                    message_id = message.message_id
            
            if not conversation_id:
                raise ValueError(f"Could not extract conversation_id from Genie response: {type(response)}")
            
            logger.info(f"Started Genie conversation: {conversation_id}, message_id: {message_id}")
            
            return {
                'conversation_id': conversation_id,
                'message_id': message_id,
                'prompt': nl_query,
                'sql': None,  # Will be available after polling
                'data': None,
                'status': 'RUNNING'
            }
        except Exception as e:
            error_msg = f"Failed to start Genie conversation: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg)
    
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
        
        # Track when we first see COMPLETED status (to avoid infinite polling if data never arrives)
        completed_first_seen_at = None
        max_wait_after_completed = 30  # Wait max 30 seconds after COMPLETED status for data
        
        # If message_id is unknown, try to get it from the conversation
        if message_id == "unknown" or not message_id:
            logger.info("message_id not available, attempting to list messages in conversation...")
            try:
                if hasattr(self.w.genie, 'list_messages'):
                    messages = self.w.genie.list_messages(
                        space_id=self.space_id,
                        conversation_id=conversation_id
                    )
                    # Get the most recent message
                    if messages:
                        message_list = list(messages) if hasattr(messages, '__iter__') else [messages]
                        if message_list:
                            latest_message = message_list[-1]  # Most recent
                            message_id = getattr(latest_message, 'id', None) or getattr(latest_message, 'message_id', None)
                            logger.info(f"Found message_id from conversation messages: {message_id}")
            except Exception as e:
                logger.warning(f"Could not list messages: {e}")
        
        if not message_id or message_id == "unknown":
            raise ValueError(
                f"Cannot poll Genie conversation: message_id is required but not available. "
                f"conversation_id={conversation_id}"
            )
        
        for attempt in range(self.max_poll_attempts):
            try:
                # First, check all messages in conversation for data (data might be in a different message)
                if attempt > 0 and attempt % 5 == 0:  # Check every 5th attempt to avoid too many API calls
                    try:
                        if hasattr(self.w.genie, 'list_messages'):
                            messages = self.w.genie.list_messages(
                                space_id=self.space_id,
                                conversation_id=conversation_id
                            )
                            if messages:
                                message_list = list(messages) if hasattr(messages, '__iter__') else [messages]
                                logger.info(f"Checking all {len(message_list)} message(s) in conversation for data...")
                                for msg in message_list:
                                    try:
                                        msg_id = getattr(msg, 'id', None) or getattr(msg, 'message_id', None)
                                        msg_result = self._extract_result(msg, conversation_id, msg_id)
                                        if msg_result.get('data') and len(msg_result.get('data', [])) > 0:
                                            logger.info(f"Found data in message {msg_id}: {len(msg_result['data'])} rows")
                                            # Return immediately if we find data
                                            return msg_result
                                    except Exception as e:
                                        logger.debug(f"Could not extract from message: {e}")
                    except Exception as e:
                        logger.debug(f"Could not list messages: {e}")
                
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
                        result = self._extract_result(message, conversation_id, message_id)
                        if result.get('sql'):
                            logger.info("Found SQL in message even though status is unknown - assuming completed")
                            return result
                    except Exception as e:
                        logger.debug(f"Could not extract result: {e}")
                
                logger.info(f"Genie status (attempt {attempt + 1}/{self.max_poll_attempts}): {status}")
                
                # Extract result to check what we have
                result = self._extract_result(message, conversation_id, message_id)
                has_sql = bool(result.get('sql'))
                has_data = bool(result.get('data')) and len(result.get('data', [])) > 0
                row_count = result.get('row_count', 0) or 0
                has_row_count = row_count > 0
                
                logger.info(
                    f"Extracted result - SQL: {has_sql}, Data array: {has_data} ({len(result.get('data', []))} rows), "
                    f"Row count: {row_count}"
                )
                
                # Check if completed (try multiple status string formats)
                status_str = str(status).upper() if status else ""
                is_completed = (status == MessageStatus.COMPLETED or status_str == "COMPLETED")
                
                if is_completed:
                    # Track when we first see COMPLETED
                    if completed_first_seen_at is None:
                        completed_first_seen_at = attempt
                        logger.info("First time seeing COMPLETED status")
                    
                    elapsed_since_completed = (attempt - completed_first_seen_at) * self.poll_interval
                    
                    logger.info(
                        f"Genie query completed. SQL: {has_sql}, Data array: {has_data} ({len(result.get('data', []))} rows), "
                        f"Row count: {row_count}, Elapsed since COMPLETED: {elapsed_since_completed}s"
                    )
                    
                    # If we have SQL and row_count > 0, that means Genie has results (even if data array is truncated/empty)
                    # Return it - we can show the count and whatever data we have
                    if has_sql and has_row_count:
                        logger.info(
                            f"Status COMPLETED with SQL and row_count={row_count}. "
                            f"Returning result (data array has {len(result.get('data', []))} rows, may be truncated)"
                        )
                        return result
                    
                    # If we've been waiting too long after COMPLETED status, return what we have
                    if elapsed_since_completed >= max_wait_after_completed:
                        logger.warning(
                            f"COMPLETED status seen {elapsed_since_completed}s ago but no data/row_count found. "
                            f"Returning what we have (SQL: {has_sql}) to avoid infinite polling."
                        )
                        return result
                    
                    # CRITICAL: If we have SQL but no data AND no row_count, keep polling - data is still loading!
                    if has_sql and not has_data and not has_row_count:
                        logger.info(
                            f"SQL found but no data yet (attempt {attempt + 1}/{self.max_poll_attempts}). "
                            f"Genie is still processing data. Continuing to poll..."
                        )
                        # Check for follow-up messages that might have data
                        try:
                            if hasattr(self.w.genie, 'list_messages'):
                                messages = self.w.genie.list_messages(
                                    space_id=self.space_id,
                                    conversation_id=conversation_id
                                )
                                if messages:
                                    message_list = list(messages) if hasattr(messages, '__iter__') else [messages]
                                    # Check all messages for data (not just after current one)
                                    for msg in message_list:
                                        msg_id = getattr(msg, 'id', None)
                                        if msg_id:  # Check all messages, including current
                                            try:
                                                msg_result = self._extract_result(msg, conversation_id, msg_id)
                                                if msg_result.get('data') and len(msg_result.get('data', [])) > 0:
                                                    logger.info(f"Found data in message {msg_id}: {len(msg_result['data'])} rows")
                                                    # Merge: keep SQL from first, data from this message
                                                    if not result.get('sql') and msg_result.get('sql'):
                                                        result['sql'] = msg_result['sql']
                                                    result['data'] = msg_result['data']
                                                    result['row_count'] = len(result['data'])
                                                    has_data = True
                                                    break
                                            except Exception as e:
                                                logger.debug(f"Could not extract from message {msg_id}: {e}")
                        except Exception as e:
                            logger.debug(f"Could not list messages: {e}")
                        
                        # If we found data in follow-up messages, return it
                        if has_data:
                            logger.info(f"Returning result with data from follow-up message: {len(result['data'])} rows")
                            return result
                        
                        # Otherwise, continue polling - don't return yet!
                        logger.info(f"No data or row_count found yet. Waiting {self.poll_interval}s and continuing to poll...")
                        time.sleep(self.poll_interval)
                        continue
                    
                    # If we have data array, return it
                    if has_data:
                        logger.info(f"Returning completed result with data array: {len(result.get('data', []))} rows")
                        return result
                    
                    # If completed but no SQL and no data/row_count, something's wrong but return what we have
                    logger.warning("Status is COMPLETED but no SQL, data, or row_count found. Returning what we have.")
                    return result
                
                # Check if failed
                elif status == MessageStatus.FAILED or status_str == "FAILED":
                    error_msg = getattr(message, 'error', None) or getattr(message, 'error_message', 'Unknown error')
                    logger.error(f"Genie query failed: {error_msg}")
                    raise Exception(f"Genie query failed: {error_msg}")
                
                # Check if cancelled
                elif status == MessageStatus.CANCELLED or status_str == "CANCELLED":
                    logger.error("Genie query was cancelled")
                    raise Exception("Genie query was cancelled")
                
                # Still running - wait and retry, but check for data anyway (sometimes data arrives before status changes)
                elif status in [MessageStatus.EXECUTING_QUERY, MessageStatus.FETCHING_METADATA, 
                               MessageStatus.QUERYING, MessageStatus.RUNNING] or \
                     status_str in ["EXECUTING_QUERY", "FETCHING_METADATA", "QUERYING", "RUNNING", "PROCESSING"]:
                    # Even if status says running, check if we have data or row_count - sometimes results arrive early
                    has_sql = bool(result.get('sql'))
                    has_data = bool(result.get('data')) and len(result.get('data', [])) > 0
                    row_count = result.get('row_count', 0) or 0
                    
                    # If we have SQL and row_count, return it (data might be truncated)
                    if has_sql and row_count > 0:
                        logger.info(
                            f"Found SQL and row_count={row_count} even though status is {status}! "
                            f"Returning result (data array has {len(result.get('data', []))} rows)."
                        )
                        return result
                    
                    if has_data:
                        logger.info(
                            f"Found data even though status is {status}! "
                            f"Returning result with {len(result['data'])} rows."
                        )
                        return result
                    
                    # Log progress more frequently as we approach timeout
                    elapsed_time = (attempt + 1) * self.poll_interval
                    remaining_time = (self.max_poll_attempts - attempt - 1) * self.poll_interval
                    
                    if attempt % 10 == 0 or remaining_time <= 60:  # Log every 10 attempts, or every attempt in last minute
                        logger.info(
                            f"Genie still processing (status: {status}). "
                            f"SQL: {has_sql}, Data: {has_data}. "
                            f"Elapsed: {elapsed_time}s, Remaining: ~{remaining_time}s "
                            f"({attempt + 1}/{self.max_poll_attempts} attempts)"
                        )
                    time.sleep(self.poll_interval)
                    continue
                
                else:
                    # Unknown status - check if we have data or row_count, but keep polling if we only have SQL
                    logger.warning(f"Unknown Genie status: {status}, checking for results...")
                    has_sql = bool(result.get('sql'))
                    has_data = bool(result.get('data')) and len(result.get('data', [])) > 0
                    row_count = result.get('row_count', 0) or 0
                    
                    # If we have SQL and row_count, return it (data might be truncated)
                    if has_sql and row_count > 0:
                        logger.info(f"Found SQL and row_count={row_count} despite unknown status. Returning result.")
                        return result
                    
                    # If we have data (with or without SQL), return it
                    if has_data:
                        logger.info(f"Found data despite unknown status. Returning result: {len(result['data'])} rows")
                        return result
                    
                    # If we have SQL but no data/row_count, keep polling - data might still be loading
                    if has_sql and not has_data and not row_count:
                        logger.info(
                            f"Have SQL but no data/row_count yet (status: {status}). "
                            f"Continuing to poll for data... ({attempt + 1}/{self.max_poll_attempts})"
                        )
                        time.sleep(self.poll_interval)
                        continue
                    
                    # If we have neither, keep waiting
                    logger.info(f"Continuing to poll with unknown status: {status} (no SQL, data, or row_count yet)")
                    time.sleep(self.poll_interval)
                    continue
                    
            except Exception as e:
                error_str = str(e)
                # Don't retry on certain errors (like failed/cancelled)
                if "failed" in error_str.lower() or "cancelled" in error_str.lower():
                    raise
                    
                logger.error(f"Error polling Genie (attempt {attempt + 1}/{self.max_poll_attempts}): {error_str}")
                if attempt == self.max_poll_attempts - 1:
                    total_time = self.max_poll_attempts * self.poll_interval
                    raise Exception(
                        f"Genie query timeout after {total_time} seconds ({self.max_poll_attempts} attempts). "
                        f"Genie may still be processing - please check Genie logs or try again. Last error: {error_str}"
                    )
                time.sleep(self.poll_interval)
        
        # Timeout - this should rarely be reached as we check attempt count above
        total_time = self.max_poll_attempts * self.poll_interval
        raise Exception(
            f"Genie query timeout after {total_time} seconds ({self.max_poll_attempts} attempts). "
            f"Genie may still be processing - please check Genie logs or try again."
        )
    
    def _extract_result(self, message, conversation_id: str = None, message_id: str = None) -> Dict:
        """Extract SQL and results from completed Genie message
        
        Args:
            message: The Genie message object
            conversation_id: Conversation ID (for fetching query results via API)
            message_id: Message ID (for fetching query results via API)
        """
        
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
        
        # Extract conversation_id and message_id from message if not provided
        if not conversation_id:
            if hasattr(message, 'conversation_id'):
                conversation_id = message.conversation_id
            elif hasattr(message, '__dict__') and 'conversation_id' in message.__dict__:
                conversation_id = message.__dict__['conversation_id']
        
        if not message_id:
            if hasattr(message, 'id'):
                message_id = message.id
            elif hasattr(message, 'message_id'):
                message_id = message.message_id
            elif hasattr(message, '__dict__') and 'id' in message.__dict__:
                message_id = message.__dict__['id']
        
        if hasattr(message, 'attachments') and message.attachments:
            logger.info(f"Found {len(message.attachments)} attachment(s)")
            for idx, attachment in enumerate(message.attachments):
                logger.info(f"Attachment {idx} type: {type(attachment)}, attributes: {[a for a in dir(attachment) if not a.startswith('_')]}")
                
                # Check if attachment itself has data
                if not result.get('data'):
                    logger.info(f"Checking attachment {idx} directly for data...")
                    for key in ['data', 'data_array', 'rows', 'result']:
                        if hasattr(attachment, key):
                            data_val = getattr(attachment, key)
                            if data_val:
                                if isinstance(data_val, list) or (hasattr(data_val, '__iter__') and not isinstance(data_val, str)):
                                    result['data'] = list(data_val) if hasattr(data_val, '__iter__') else [data_val]
                                    result['row_count'] = len(result['data'])
                                    logger.info(f"Extracted {result['row_count']} rows from attachment.{key}")
                                    break
                
                if hasattr(attachment, 'query') and attachment.query:
                    query = attachment.query
                    logger.info(f"Found query in attachment {idx}, type: {type(query)}, attributes: {[a for a in dir(query) if not a.startswith('_')]}")
                    
                    # Try to get query as dict for easier inspection
                    query_dict = None
                    try:
                        if hasattr(query, 'as_dict'):
                            query_dict = query.as_dict()
                            logger.info(f"Query as_dict keys: {list(query_dict.keys())}")
                        elif hasattr(query, '__dict__'):
                            query_dict = query.__dict__
                            logger.info(f"Query __dict__ keys: {list(query_dict.keys())}")
                    except Exception as e:
                        logger.debug(f"Could not convert query to dict: {e}")
                    
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
                        elif query_dict and 'query' in query_dict:
                            result['sql'] = query_dict['query']
                            logger.info(f"Extracted SQL from query_dict['query']")
                    
                    # Get execution time
                    if hasattr(query, 'duration'):
                        result['execution_time'] = query.duration
                        logger.info(f"Extracted execution_time: {result['execution_time']}")
                    elif hasattr(query, 'execution_time'):
                        result['execution_time'] = query.execution_time
                    elif query_dict and 'duration' in query_dict:
                        result['execution_time'] = query_dict['duration']
                    
                    # Check if query itself has data (not just query.result)
                    if not result.get('data'):
                        logger.info("Checking query object directly for data...")
                        for key in ['data', 'data_array', 'rows', 'result_data']:
                            if hasattr(query, key):
                                data_val = getattr(query, key)
                                if data_val:
                                    if isinstance(data_val, list) or (hasattr(data_val, '__iter__') and not isinstance(data_val, str)):
                                        result['data'] = list(data_val) if hasattr(data_val, '__iter__') else [data_val]
                                        result['row_count'] = len(result['data'])
                                        logger.info(f"Extracted {result['row_count']} rows from query.{key}")
                                        break
                        # Also check query_dict
                        if not result.get('data') and query_dict:
                            for key in ['data', 'data_array', 'rows', 'result_data']:
                                if key in query_dict and query_dict[key]:
                                    data_val = query_dict[key]
                                    if isinstance(data_val, list):
                                        result['data'] = data_val
                                        result['row_count'] = len(data_val)
                                        logger.info(f"Extracted {result['row_count']} rows from query_dict['{key}']")
                                        break
                    
                    # Get row_count from query_result_metadata if available
                    if hasattr(query, 'query_result_metadata') and query.query_result_metadata:
                        metadata = query.query_result_metadata
                        if hasattr(metadata, 'row_count'):
                            result['row_count'] = metadata.row_count or 0
                            logger.info(f"Extracted row_count from query_result_metadata: {result['row_count']}")
                        elif hasattr(metadata, '__dict__') and 'row_count' in metadata.__dict__:
                            result['row_count'] = metadata.__dict__.get('row_count', 0)
                            logger.info(f"Extracted row_count from query_result_metadata.__dict__: {result['row_count']}")
                    
                    # CRITICAL: Genie 2.0 requires a separate API call to get query results
                    # First, try to get statement_id directly from query (faster path)
                    statement_id = None
                    if hasattr(query, 'statement_id'):
                        statement_id = query.statement_id
                        logger.info(f"Found statement_id directly in query: {statement_id}")
                    elif query_dict and 'statement_id' in query_dict:
                        statement_id = query_dict['statement_id']
                        logger.info(f"Found statement_id in query_dict: {statement_id}")
                    
                    # If we have statement_id, use it directly to get data
                    if statement_id and not result.get('data') and conversation_id and message_id:
                        logger.info(f"Using statement_id directly to fetch query results...")
                        try:
                            statement_result = self.w.statement_execution.get_statement(statement_id)
                            logger.info(f"Statement result type: {type(statement_result)}")
                            logger.info(f"Statement result attributes: {[a for a in dir(statement_result) if not a.startswith('_')]}")
                            
                            # Try to get as_dict() for inspection if available
                            try:
                                if hasattr(statement_result, 'as_dict'):
                                    result_dict = statement_result.as_dict()
                                    logger.info(f"Statement result keys (as_dict): {list(result_dict.keys()) if isinstance(result_dict, dict) else 'Not a dict'}")
                                    if isinstance(result_dict, dict) and 'manifest' in result_dict:
                                        manifest_dict = result_dict['manifest']
                                        logger.info(f"Manifest keys: {list(manifest_dict.keys()) if isinstance(manifest_dict, dict) else 'Not a dict'}")
                                        if isinstance(manifest_dict, dict) and 'schema' in manifest_dict:
                                            schema_dict = manifest_dict['schema']
                                            logger.info(f"Schema keys: {list(schema_dict.keys()) if isinstance(schema_dict, dict) else 'Not a dict'}")
                                            if isinstance(schema_dict, dict) and 'columns' in schema_dict:
                                                logger.info(f"Found columns in dict! Count: {len(schema_dict['columns']) if isinstance(schema_dict['columns'], list) else 'N/A'}")
                            except Exception as e:
                                logger.debug(f"Could not inspect as_dict: {e}")
                            
                            # Extract data from statement result
                            if hasattr(statement_result, 'result') and statement_result.result:
                                stmt_result = statement_result.result
                                if hasattr(stmt_result, 'data_array'):
                                    data_array = stmt_result.data_array
                                    if data_array:
                                        result['data'] = list(data_array) if hasattr(data_array, '__iter__') else [data_array]
                                        result['row_count'] = len(result['data'])
                                        logger.info(f"Extracted {result['row_count']} rows from statement result data_array (via statement_id)")
                            
                            # Also check for row_count in statement result
                            if hasattr(statement_result, 'result') and statement_result.result:
                                stmt_result = statement_result.result
                                if hasattr(stmt_result, 'row_count'):
                                    stmt_row_count = stmt_result.row_count
                                    if stmt_row_count and stmt_row_count > result.get('row_count', 0):
                                        result['row_count'] = stmt_row_count
                                        logger.info(f"Updated row_count from statement result: {result['row_count']}")
                            
                            # Get column names if available - try multiple paths
                            logger.info("Attempting to extract column names from statement_result...")
                            columns_extracted = False
                            
                            # Path 1: manifest.schema.columns
                            if hasattr(statement_result, 'manifest') and statement_result.manifest:
                                logger.info(f"Found manifest, type: {type(statement_result.manifest)}")
                                logger.info(f"Manifest attributes: {[a for a in dir(statement_result.manifest) if not a.startswith('_')]}")
                                if hasattr(statement_result.manifest, 'schema') and statement_result.manifest.schema:
                                    logger.info(f"Found schema, type: {type(statement_result.manifest.schema)}")
                                    logger.info(f"Schema attributes: {[a for a in dir(statement_result.manifest.schema) if not a.startswith('_')]}")
                                    if hasattr(statement_result.manifest.schema, 'columns'):
                                        columns = statement_result.manifest.schema.columns
                                        logger.info(f"Found columns, type: {type(columns)}, length: {len(columns) if hasattr(columns, '__len__') else 'N/A'}")
                                        if columns:
                                            try:
                                                result['columns'] = [col.name if hasattr(col, 'name') else str(col) for col in columns]
                                                logger.info(f"✅ Extracted {len(result['columns'])} column names from manifest.schema.columns: {result['columns']}")
                                                columns_extracted = True
                                            except Exception as e:
                                                logger.warning(f"Error extracting column names from columns list: {e}")
                            
                            # Path 2: Check if columns are in result directly
                            if not columns_extracted and hasattr(statement_result, 'result') and statement_result.result:
                                stmt_result = statement_result.result
                                if hasattr(stmt_result, 'columns'):
                                    columns = stmt_result.columns
                                    if columns:
                                        try:
                                            result['columns'] = [col.name if hasattr(col, 'name') else str(col) for col in columns]
                                            logger.info(f"✅ Extracted {len(result['columns'])} column names from result.columns: {result['columns']}")
                                            columns_extracted = True
                                        except Exception as e:
                                            logger.warning(f"Error extracting from result.columns: {e}")
                            
                            # Path 3: Check manifest.columns directly
                            if not columns_extracted and hasattr(statement_result, 'manifest') and statement_result.manifest:
                                if hasattr(statement_result.manifest, 'columns'):
                                    columns = statement_result.manifest.columns
                                    if columns:
                                        try:
                                            result['columns'] = [col.name if hasattr(col, 'name') else str(col) for col in columns]
                                            logger.info(f"✅ Extracted {len(result['columns'])} column names from manifest.columns: {result['columns']}")
                                            columns_extracted = True
                                        except Exception as e:
                                            logger.warning(f"Error extracting from manifest.columns: {e}")
                            
                            # Path 4: Try extracting from as_dict() if available
                            if not columns_extracted:
                                try:
                                    if hasattr(statement_result, 'as_dict'):
                                        result_dict = statement_result.as_dict()
                                        if isinstance(result_dict, dict):
                                            manifest = result_dict.get('manifest', {})
                                            if isinstance(manifest, dict):
                                                schema = manifest.get('schema', {})
                                                if isinstance(schema, dict):
                                                    columns_list = schema.get('columns', [])
                                                    if columns_list and isinstance(columns_list, list):
                                                        # Extract column names from dict structure
                                                        column_names = []
                                                        for col in columns_list:
                                                            if isinstance(col, dict):
                                                                col_name = col.get('name') or col.get('column_name')
                                                                if col_name:
                                                                    column_names.append(col_name)
                                                            elif hasattr(col, 'name'):
                                                                column_names.append(col.name)
                                                            else:
                                                                column_names.append(str(col))
                                                        if column_names:
                                                            result['columns'] = column_names
                                                            logger.info(f"✅ Extracted {len(result['columns'])} column names from as_dict() structure: {result['columns']}")
                                                            columns_extracted = True
                                except Exception as e:
                                    logger.debug(f"Error extracting from as_dict(): {e}")
                            
                            if not columns_extracted:
                                logger.warning("⚠️ Could not extract column names from statement_result. Available attributes:")
                                logger.warning(f"  statement_result attributes: {[a for a in dir(statement_result) if not a.startswith('_')]}")
                                if hasattr(statement_result, 'manifest'):
                                    logger.warning(f"  manifest attributes: {[a for a in dir(statement_result.manifest) if not a.startswith('_')]}")
                                    if hasattr(statement_result.manifest, 'schema'):
                                        logger.warning(f"  schema attributes: {[a for a in dir(statement_result.manifest.schema) if not a.startswith('_')]}")
                        except Exception as e:
                            logger.warning(f"Could not fetch statement result directly via statement_id: {e}. Will try attachment_id method.")
                    
                    # If we don't have statement_id or direct method failed, try via attachment_id
                    if not result.get('data'):
                        attachment_id = None
                        if hasattr(attachment, 'attachment_id'):
                            attachment_id = attachment.attachment_id
                        elif hasattr(attachment, 'id'):
                            attachment_id = attachment.id
                        elif hasattr(attachment, '__dict__') and 'attachment_id' in attachment.__dict__:
                            attachment_id = attachment.__dict__['attachment_id']
                        
                        if attachment_id and conversation_id and message_id:
                            logger.info(f"Found attachment_id: {attachment_id}. Fetching query results via separate API call...")
                            try:
                                # Get the query result response (contains statement_id)
                                query_result_response = self.w.genie.get_message_attachment_query_result(
                                    space_id=self.space_id,
                                    conversation_id=conversation_id,
                                    message_id=message_id,
                                    attachment_id=attachment_id
                                )
                                
                                logger.info(f"Query result response type: {type(query_result_response)}")
                                logger.info(f"Query result response attributes: {[a for a in dir(query_result_response) if not a.startswith('_')]}")
                                
                                # Extract statement_id from response
                                statement_id = None
                                if hasattr(query_result_response, 'statement_id'):
                                    statement_id = query_result_response.statement_id
                                elif hasattr(query_result_response, '__dict__') and 'statement_id' in query_result_response.__dict__:
                                    statement_id = query_result_response.__dict__['statement_id']
                                
                                if statement_id:
                                    logger.info(f"Found statement_id: {statement_id}. Fetching statement execution result...")
                                    # Get the actual statement execution result with data
                                    statement_result = self.w.statement_execution.get_statement(statement_id)
                                    
                                    logger.info(f"Statement result type: {type(statement_result)}")
                                    logger.info(f"Statement result attributes: {[a for a in dir(statement_result) if not a.startswith('_')]}")
                                    
                                    # Try to get as_dict() for inspection if available
                                    try:
                                        if hasattr(statement_result, 'as_dict'):
                                            result_dict = statement_result.as_dict()
                                            logger.info(f"Statement result keys (as_dict): {list(result_dict.keys()) if isinstance(result_dict, dict) else 'Not a dict'}")
                                            if isinstance(result_dict, dict) and 'manifest' in result_dict:
                                                manifest_dict = result_dict['manifest']
                                                logger.info(f"Manifest keys: {list(manifest_dict.keys()) if isinstance(manifest_dict, dict) else 'Not a dict'}")
                                                if isinstance(manifest_dict, dict) and 'schema' in manifest_dict:
                                                    schema_dict = manifest_dict['schema']
                                                    logger.info(f"Schema keys: {list(schema_dict.keys()) if isinstance(schema_dict, dict) else 'Not a dict'}")
                                                    if isinstance(schema_dict, dict) and 'columns' in schema_dict:
                                                        logger.info(f"Found columns in dict! Count: {len(schema_dict['columns']) if isinstance(schema_dict['columns'], list) else 'N/A'}")
                                                        # Try to extract column names from dict structure
                                                        if isinstance(schema_dict['columns'], list) and len(schema_dict['columns']) > 0:
                                                            first_col = schema_dict['columns'][0]
                                                            logger.info(f"First column structure: {type(first_col)}, keys: {list(first_col.keys()) if isinstance(first_col, dict) else 'N/A'}")
                                    except Exception as e:
                                        logger.debug(f"Could not inspect as_dict: {e}")
                                    
                                    # Extract data from statement result
                                    if hasattr(statement_result, 'result') and statement_result.result:
                                        stmt_result = statement_result.result
                                        if hasattr(stmt_result, 'data_array'):
                                            data_array = stmt_result.data_array
                                            if data_array:
                                                result['data'] = list(data_array) if hasattr(data_array, '__iter__') else [data_array]
                                                result['row_count'] = len(result['data'])
                                                logger.info(f"Extracted {result['row_count']} rows from statement result data_array (via attachment_id)")
                                        
                                        # Also check for row_count in statement result
                                        if hasattr(stmt_result, 'row_count'):
                                            stmt_row_count = stmt_result.row_count
                                            if stmt_row_count and stmt_row_count > result.get('row_count', 0):
                                                result['row_count'] = stmt_row_count
                                                logger.info(f"Updated row_count from statement result: {result['row_count']}")
                                    
                                    # If we still don't have data, check statement_result directly
                                    if not result.get('data'):
                                        if hasattr(statement_result, 'data_array'):
                                            data_array = statement_result.data_array
                                            if data_array:
                                                result['data'] = list(data_array) if hasattr(data_array, '__iter__') else [data_array]
                                                result['row_count'] = len(result['data'])
                                                logger.info(f"Extracted {result['row_count']} rows from statement_result.data_array")
                                    
                                    # Get column names if available (for better DataFrame display) - try multiple paths
                                    logger.info("Attempting to extract column names from statement_result (via attachment_id)...")
                                    columns_extracted = False
                                    
                                    # Path 1: manifest.schema.columns
                                    if hasattr(statement_result, 'manifest') and statement_result.manifest:
                                        logger.info(f"Found manifest, type: {type(statement_result.manifest)}")
                                        if hasattr(statement_result.manifest, 'schema') and statement_result.manifest.schema:
                                            logger.info(f"Found schema, type: {type(statement_result.manifest.schema)}")
                                            if hasattr(statement_result.manifest.schema, 'columns'):
                                                columns = statement_result.manifest.schema.columns
                                                logger.info(f"Found columns, type: {type(columns)}, length: {len(columns) if hasattr(columns, '__len__') else 'N/A'}")
                                                if columns:
                                                    try:
                                                        result['columns'] = [col.name if hasattr(col, 'name') else str(col) for col in columns]
                                                        logger.info(f"✅ Extracted {len(result['columns'])} column names from manifest.schema.columns: {result['columns']}")
                                                        columns_extracted = True
                                                    except Exception as e:
                                                        logger.warning(f"Error extracting column names: {e}")
                                    
                                    # Path 2: Check result.columns
                                    if not columns_extracted and hasattr(statement_result, 'result') and statement_result.result:
                                        stmt_result = statement_result.result
                                        if hasattr(stmt_result, 'columns'):
                                            columns = stmt_result.columns
                                            if columns:
                                                try:
                                                    result['columns'] = [col.name if hasattr(col, 'name') else str(col) for col in columns]
                                                    logger.info(f"✅ Extracted {len(result['columns'])} column names from result.columns: {result['columns']}")
                                                    columns_extracted = True
                                                except Exception as e:
                                                    logger.warning(f"Error extracting from result.columns: {e}")
                                    
                                    # Path 3: Check manifest.columns directly
                                    if not columns_extracted and hasattr(statement_result, 'manifest') and statement_result.manifest:
                                        if hasattr(statement_result.manifest, 'columns'):
                                            columns = statement_result.manifest.columns
                                            if columns:
                                                try:
                                                    result['columns'] = [col.name if hasattr(col, 'name') else str(col) for col in columns]
                                                    logger.info(f"✅ Extracted {len(result['columns'])} column names from manifest.columns: {result['columns']}")
                                                    columns_extracted = True
                                                except Exception as e:
                                                    logger.warning(f"Error extracting from manifest.columns: {e}")
                                    
                                    # Path 4: Try extracting from as_dict() if available
                                    if not columns_extracted:
                                        try:
                                            if hasattr(statement_result, 'as_dict'):
                                                result_dict = statement_result.as_dict()
                                                if isinstance(result_dict, dict):
                                                    manifest = result_dict.get('manifest', {})
                                                    if isinstance(manifest, dict):
                                                        schema = manifest.get('schema', {})
                                                        if isinstance(schema, dict):
                                                            columns_list = schema.get('columns', [])
                                                            if columns_list and isinstance(columns_list, list):
                                                                # Extract column names from dict structure
                                                                column_names = []
                                                                for col in columns_list:
                                                                    if isinstance(col, dict):
                                                                        col_name = col.get('name') or col.get('column_name')
                                                                        if col_name:
                                                                            column_names.append(col_name)
                                                                    elif hasattr(col, 'name'):
                                                                        column_names.append(col.name)
                                                                    else:
                                                                        column_names.append(str(col))
                                                                if column_names:
                                                                    result['columns'] = column_names
                                                                    logger.info(f"✅ Extracted {len(result['columns'])} column names from as_dict() structure (via attachment_id): {result['columns']}")
                                                                    columns_extracted = True
                                        except Exception as e:
                                            logger.debug(f"Error extracting from as_dict() (via attachment_id): {e}")
                                    
                                    if not columns_extracted:
                                        logger.warning("⚠️ Could not extract column names from statement_result (via attachment_id)")
                                else:
                                    logger.warning(f"Could not extract statement_id from query_result_response")
                            except Exception as e:
                                logger.warning(f"Could not fetch query results via get_message_attachment_query_result: {e}. Will try other methods.")
                                # Continue to try other extraction methods below
                    
                    # Fallback: Try to get result data from query.result (older API format)
                    if not result.get('data') and hasattr(query, 'result') and query.result:
                        result_obj = query.result
                        logger.info(f"Found result object, type: {type(result_obj)}, attributes: {[a for a in dir(result_obj) if not a.startswith('_')]}")
                        
                        # Try to get as dict for easier inspection
                        result_dict = None
                        try:
                            if hasattr(result_obj, 'as_dict'):
                                result_dict = result_obj.as_dict()
                                logger.info(f"Result as_dict keys: {list(result_dict.keys())}")
                            elif hasattr(result_obj, '__dict__'):
                                result_dict = result_obj.__dict__
                                logger.info(f"Result __dict__ keys: {list(result_dict.keys())}")
                        except Exception as e:
                            logger.debug(f"Could not convert result to dict: {e}")
                        
                        # Try multiple ways to get data
                        data_found = False
                        if hasattr(result_obj, 'data_array'):
                            data_val = result_obj.data_array
                            if data_val:
                                result['data'] = list(data_val) if hasattr(data_val, '__iter__') else [data_val]
                                result['row_count'] = len(result['data'])
                                logger.info(f"Extracted {result['row_count']} rows from data_array")
                                data_found = True
                        elif hasattr(result_obj, 'data'):
                            data_val = result_obj.data
                            if data_val:
                                result['data'] = list(data_val) if hasattr(data_val, '__iter__') else [data_val]
                                result['row_count'] = len(result['data'])
                                logger.info(f"Extracted {result['row_count']} rows from data")
                                data_found = True
                        elif hasattr(result_obj, 'rows'):
                            data_val = result_obj.rows
                            if data_val:
                                result['data'] = list(data_val) if hasattr(data_val, '__iter__') else [data_val]
                                result['row_count'] = len(result['data'])
                                logger.info(f"Extracted {result['row_count']} rows from rows")
                                data_found = True
                        
                        # Check result_dict for data
                        if not data_found and result_dict:
                            logger.info("Checking result_dict for data...")
                            for key in ['data', 'data_array', 'rows', 'values', 'records']:
                                if key in result_dict and result_dict[key]:
                                    data_val = result_dict[key]
                                    if isinstance(data_val, list):
                                        result['data'] = data_val
                                        result['row_count'] = len(data_val)
                                        logger.info(f"Extracted {result['row_count']} rows from result_dict['{key}']")
                                        data_found = True
                                        break
                                    elif hasattr(data_val, '__iter__') and not isinstance(data_val, str):
                                        result['data'] = list(data_val)
                                        result['row_count'] = len(result['data'])
                                        logger.info(f"Extracted {result['row_count']} rows from result_dict['{key}'] (converted from iterable)")
                                        data_found = True
                                        break
                        
                        if not data_found:
                            logger.warning(f"No data found in result object. Result object structure: {result_dict if result_dict else 'could not inspect'}")
        
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
        
        # Fallback: check message_dict directly for SQL and data
        if message_dict:
            if not result['sql']:
                logger.info("Checking message_dict for SQL...")
                if 'sql' in message_dict:
                    result['sql'] = message_dict['sql']
                elif 'query' in message_dict:
                    query_val = message_dict['query']
                    if isinstance(query_val, str):
                        result['sql'] = query_val
                    elif isinstance(query_val, dict) and 'query' in query_val:
                        result['sql'] = query_val['query']
            
            if not result['data']:
                logger.info("Checking message_dict for data...")
                # Try various keys where data might be stored
                for key in ['data', 'data_array', 'rows', 'result', 'results']:
                    if key in message_dict and message_dict[key]:
                        data_val = message_dict[key]
                        if isinstance(data_val, list):
                            result['data'] = data_val
                            result['row_count'] = len(data_val)
                            logger.info(f"Extracted {result['row_count']} rows from message_dict['{key}']")
                            break
                        elif isinstance(data_val, dict):
                            # Check nested structures
                            for nested_key in ['data', 'data_array', 'rows']:
                                if nested_key in data_val and isinstance(data_val[nested_key], list):
                                    result['data'] = data_val[nested_key]
                                    result['row_count'] = len(result['data'])
                                    logger.info(f"Extracted {result['row_count']} rows from message_dict['{key}']['{nested_key}']")
                                    break
                            if result['data']:
                                break
        
        # Log final result
        columns_in_result = result.get('columns')
        logger.info(f"Extraction complete - SQL: {'Found' if result['sql'] else 'Not found'}, "
                   f"Data rows: {len(result['data'])}, Row count: {result['row_count']}, Execution time: {result['execution_time']}")
        logger.info(f"📋 Columns in result: {columns_in_result}, count: {len(columns_in_result) if columns_in_result else 0}")
        
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

        # Additional criteria: demographics, medications, procedures
        demographics = criteria.get("demographics", [])
        drugs = criteria.get("drugs", [])
        procedures = criteria.get("procedures", [])
        conditions = criteria.get("conditions", [])
        
        # High-level filters
        lines.append("")
        lines.append("Additional cohort filters (high-level, you choose columns/tables):")
        
        if demographics:
            lines.append(f"- demographics: {', '.join(demographics)}")
        
        if drugs:
            lines.append(f"- medications/drugs: {', '.join(drugs)}")
        
        if procedures:
            lines.append(f"- procedures: {', '.join(procedures)}")
        
        if conditions and not code_details and not codes:
            # If we have conditions but no codes yet, include them
            lines.append(f"- conditions: {', '.join(conditions)}")
        
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
        if code_details or codes:
            lines.append("- Use EXACTLY the diagnosis codes listed above for the primary condition filter.")
        if drugs:
            lines.append("- Include patients who have been prescribed or taken the medications listed above.")
        if procedures:
            lines.append("- Include patients who have undergone the procedures listed above.")
        if demographics:
            lines.append("- Apply the demographic filters listed above (age, sex, setting, etc.).")
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


