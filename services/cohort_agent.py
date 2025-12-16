"""
LangGraph-based Conversational Cohort Agent
Manages state and handles multi-turn conversations for cohort building
"""

from typing import TypedDict, Annotated, Literal
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
import logging
from config import config

logger = logging.getLogger(__name__)


class AgentState(TypedDict, total=False):
    """State managed by the LangGraph agent"""
    messages: Annotated[list, add_messages]
    user_query: str
    codes: list
    cohort_table: str
    cohort_count: int
    genie_conversation_id: str
    current_step: str
    error: str
    session_id: str
    sql: str
    answer_data: dict


class CohortAgent:
    """Conversational agent for cohort building using LangGraph"""
    
    def __init__(self, vector_service, genie_service, cohort_manager):
        self.vector_service = vector_service
        self.genie_service = genie_service
        self.cohort_manager = cohort_manager
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("classify_query", self._classify_query)
        workflow.add_node("search_codes", self._search_codes)
        workflow.add_node("generate_sql", self._generate_sql)
        workflow.add_node("materialize_cohort", self._materialize_cohort)
        workflow.add_node("answer_question", self._answer_question)
        workflow.add_node("handle_error", self._handle_error)
        
        # Set entry point
        workflow.set_entry_point("classify_query")
        
        # Add edges based on query type
        workflow.add_conditional_edges(
            "classify_query",
            self._route_query,
            {
                "new_cohort": "search_codes",
                "follow_up": "answer_question",
                "insights": "answer_question",
                "error": "handle_error"
            }
        )
        
        # Flow for new cohort creation
        workflow.add_edge("search_codes", "generate_sql")
        workflow.add_edge("generate_sql", "materialize_cohort")
        workflow.add_edge("materialize_cohort", END)
        
        # Flow for questions/insights
        workflow.add_edge("answer_question", END)
        workflow.add_edge("handle_error", END)
        
        return workflow.compile()
    
    def _classify_query(self, state: AgentState) -> AgentState:
        """Classify the user query to determine next action"""
        query = state.get("user_query", "").lower()
        
        # Check if this is a follow-up question about existing cohort
        if state.get("cohort_table"):
            follow_up_keywords = [
                "what", "how many", "show me", "tell me", "explain",
                "demographics", "insights", "trends", "outcomes",
                "age", "gender", "race", "ethnicity", "site"
            ]
            
            if any(keyword in query for keyword in follow_up_keywords):
                state["current_step"] = "follow_up"
                return state
        
        # Check if asking for insights
        insights_keywords = ["insights", "analysis", "report", "demographics", "trends"]
        if any(keyword in query for keyword in insights_keywords) and state.get("cohort_table"):
            state["current_step"] = "insights"
            return state
        
        # Default: new cohort query
        state["current_step"] = "new_cohort"
        return state
    
    def _route_query(self, state: AgentState) -> Literal["new_cohort", "follow_up", "insights", "error"]:
        """Route to appropriate node based on query type"""
        step = state.get("current_step", "new_cohort")
        
        if step == "new_cohort":
            return "new_cohort"
        elif step == "follow_up":
            return "follow_up"
        elif step == "insights":
            return "insights"
        else:
            return "error"
    
    def _search_codes(self, state: AgentState) -> AgentState:
        """Search for relevant codes using vector search"""
        try:
            query = state.get("user_query", "")
            codes = self.vector_service.search_codes(query, limit=10)
            state["codes"] = codes
            
            if not codes:
                state["error"] = "No relevant codes found. Please try rephrasing your query."
                state["current_step"] = "error"
            
            return state
        except Exception as e:
            logger.error(f"Error in search_codes: {str(e)}")
            state["error"] = f"Error searching for codes: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _generate_sql(self, state: AgentState) -> AgentState:
        """Generate SQL query using Genie"""
        try:
            codes = state.get("codes", [])
            if not codes:
                state["error"] = "No codes available for SQL generation"
                state["current_step"] = "error"
                return state
            
            # Extract criteria from query (simplified - could be enhanced with NLP)
            criteria = {
                'codes': [c['code'] for c in codes[:5]],
                'timeframe': '30 days',  # Could extract from query
                'age': None,  # Could extract from query
                'patient_table_prefix': config.patient_table_prefix
            }
            
            result = self.genie_service.create_cohort_query(criteria)
            state["genie_conversation_id"] = result.get('conversation_id')
            state["sql"] = result.get('sql')
            
            return state
        except Exception as e:
            logger.error(f"Error in generate_sql: {str(e)}")
            state["error"] = f"Error generating SQL: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _materialize_cohort(self, state: AgentState) -> AgentState:
        """Materialize the cohort in Delta table"""
        try:
            sql = state.get("sql")
            session_id = state.get("session_id", "default")
            
            if not sql:
                state["error"] = "No SQL available for materialization"
                state["current_step"] = "error"
                return state
            
            cohort_result = self.cohort_manager.materialize_cohort(session_id, sql)
            state["cohort_table"] = cohort_result['cohort_table']
            state["cohort_count"] = cohort_result['count']
            
            return state
        except Exception as e:
            logger.error(f"Error in materialize_cohort: {str(e)}")
            state["error"] = f"Error materializing cohort: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _answer_question(self, state: AgentState) -> AgentState:
        """Answer questions about existing cohort"""
        try:
            query = state.get("user_query", "").lower()
            cohort_table = state.get("cohort_table")
            
            if not cohort_table:
                state["error"] = "No cohort available. Please create a cohort first."
                state["current_step"] = "error"
                return state
            
            # Handle different types of questions
            if "demographics" in query or "age" in query or "gender" in query:
                demographics = self.cohort_manager.get_demographics(cohort_table)
                state["answer_data"] = {"type": "demographics", "data": demographics}
            
            elif "site" in query or "hospital" in query:
                sites = self.cohort_manager.get_site_breakdown(cohort_table)
                state["answer_data"] = {"type": "sites", "data": sites}
            
            elif "trend" in query or "time" in query:
                trends = self.cohort_manager.get_admission_trends(cohort_table)
                state["answer_data"] = {"type": "trends", "data": trends}
            
            elif "outcome" in query or "mortality" in query or "readmission" in query:
                outcomes = self.cohort_manager.get_outcomes(cohort_table)
                state["answer_data"] = {"type": "outcomes", "data": outcomes}
            
            elif "how many" in query or "count" in query:
                count = state.get("cohort_count", 0)
                state["answer_data"] = {"type": "count", "data": count}
            
            else:
                # Use Genie for follow-up questions
                if state.get("genie_conversation_id"):
                    result = self.genie_service.follow_up_question(
                        state["genie_conversation_id"],
                        query
                    )
                    state["answer_data"] = {"type": "genie", "data": result}
                else:
                    state["error"] = "Unable to answer question. Please create a new cohort."
                    state["current_step"] = "error"
            
            return state
        except Exception as e:
            logger.error(f"Error in answer_question: {str(e)}")
            state["error"] = f"Error answering question: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _handle_error(self, state: AgentState) -> AgentState:
        """Handle errors in the workflow"""
        error = state.get("error", "Unknown error occurred")
        logger.error(f"Agent error: {error}")
        return state
    
    def process_query(self, user_query: str, session_id: str, existing_state: dict = None) -> dict:
        """
        Process a user query through the LangGraph workflow
        
        Args:
            user_query: User's natural language query
            session_id: Session identifier
            existing_state: Previous state to maintain context
        
        Returns:
            Updated state dictionary with results
        """
        # Initialize state
        initial_state = {
            "messages": [HumanMessage(content=user_query)],
            "user_query": user_query,
            "session_id": session_id,
            "codes": [],
            "cohort_table": existing_state.get("cohort_table") if existing_state else None,
            "cohort_count": existing_state.get("cohort_count") if existing_state else 0,
            "genie_conversation_id": existing_state.get("genie_conversation_id") if existing_state else None,
            "current_step": "",
            "error": None
        }
        
        # Run the graph
        try:
            final_state = self.graph.invoke(initial_state)
            return final_state
        except Exception as e:
            logger.error(f"Error processing query through graph: {str(e)}")
            return {
                **initial_state,
                "error": f"Error processing query: {str(e)}",
                "current_step": "error"
            }

