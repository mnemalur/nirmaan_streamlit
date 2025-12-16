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
        self.w = WorkspaceClient(
            host=config.host,
            token=config.token
        )
        self.space_id = config.space_id
        self.max_poll_attempts = 60  # 60 attempts * 2 seconds = 2 minutes max
        self.poll_interval = 2  # seconds
    
    def create_cohort_query(self, criteria: Dict) -> Dict:
        """
        Use Genie to generate SQL for cohort
        Polls for completion instead of waiting
        
        Args:
            criteria: {
                'codes': ['I21.09', 'I21.01'],
                'timeframe': '30 days',
                'age': '> 50',
                'labs': []
            }
        
        Returns:
            {
                'sql': 'SELECT ...',
                'conversation_id': '...',
                'execution_time': 2.5,
                'row_count': 237
            }
        """
        
        # Build natural language query
        nl_query = self._build_nl_query(criteria)
        
        logger.info(f"Sending query to Genie: {nl_query}")
        
        # Create Genie message
        message = self.w.genie.create_message(
            space_id=self.space_id,
            content=nl_query
        )
        
        conversation_id = message.conversation_id
        message_id = message.id
        
        logger.info(f"Genie message created: conversation_id={conversation_id}, message_id={message_id}")
        
        # Poll for completion
        result = self._poll_for_completion(conversation_id, message_id)
        
        return {
            'sql': result['sql'],
            'conversation_id': conversation_id,
            'execution_time': result.get('execution_time'),
            'row_count': result.get('row_count', 0)
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
        """
        Convert structured criteria to natural language for Genie
        Uses actual table names: patdemo, paticd, patcpt
        """
        
        codes_str = ', '.join(criteria['codes'])
        vocabulary = criteria.get('vocabulary', 'ICD10CM')
        
        query_parts = [
            f"Find all patients from {config.patient_table_prefix}.patdemo",
            f"joined with {config.patient_table_prefix}.paticd for diagnoses",
        ]
        
        # Add diagnosis code filter
        query_parts.append(f"where diagnosis codes (from paticd table) include: {codes_str}")
        
        # Add timeframe
        if criteria.get('timeframe'):
            query_parts.append(f"and admission_date (from patdemo) is within the last {criteria['timeframe']}")
        
        # Add age filter
        if criteria.get('age'):
            query_parts.append(f"and age (from patdemo) is {criteria['age']}")
        
        # Add lab conditions
        if criteria.get('labs'):
            lab_conditions = ', '.join(criteria['labs'])
            query_parts.append(f"with lab conditions: {lab_conditions}")
        
        # Add procedure filters if any
        if criteria.get('procedures'):
            query_parts.append(f"joined with {config.patient_table_prefix}.patcpt for procedures")
            proc_codes = ', '.join(criteria['procedures'])
            query_parts.append(f"where procedure codes include: {proc_codes}")
        
        # Specify return columns
        query_parts.append("""
Return these columns:
- patient_id (from patdemo)
- admission_date (from patdemo)
- age (from patdemo)
- gender (from patdemo)
- length_of_stay (from patdemo)
- site_name (from patdemo)
- primary diagnosis code (from paticd where diagnosis_type = 'primary')

Make sure to use proper JOINs and handle multiple diagnoses per patient.
        """.strip())
        
        return " ".join(query_parts)


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


