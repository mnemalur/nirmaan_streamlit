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
    diagnosis_phrases: list  # phrases extracted by LLM for diagnosis intent
    codes: list
    vocabularies: list  # list of vocabulary_ids / coding systems represented in codes
    genie_prompt: str   # enriched, code-aware request that would be sent to Genie
    cohort_table: str
    cohort_count: int
    genie_conversation_id: str
    current_step: str
    error: str
    session_id: str
    sql: str
    answer_data: dict
    reasoning_steps: list  # List of (step_name, description) tuples for transparency


class CohortAgent:
    """Conversational agent for cohort building using LangGraph"""
    
    def __init__(self, vector_service, genie_service, cohort_manager, intent_service=None):
        self.vector_service = vector_service
        self.genie_service = genie_service
        self.cohort_manager = cohort_manager
        self.intent_service = intent_service
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("classify_query", self._classify_query)
        workflow.add_node("interpret_intent", self._interpret_intent)
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
                "new_cohort": "interpret_intent",
                "follow_up": "answer_question",
                "insights": "answer_question",
                "error": "handle_error"
            }
        )
        
        # Flow for new cohort creation
        # LLM first interprets intent, then we search codes, then build the
        # enriched Genie request, generate SQL, and optionally materialize cohort
        workflow.add_edge("interpret_intent", "search_codes")
        workflow.add_edge("search_codes", "generate_sql")
        # For conversational flow, we generate SQL but don't auto-materialize
        # User can request materialization explicitly
        workflow.add_edge("generate_sql", END)
        # Full auto-flow (commented for now - user controls execution):
        # workflow.add_edge("generate_sql", "materialize_cohort")
        # workflow.add_edge("materialize_cohort", END)
        
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
    
    def _interpret_intent(self, state: AgentState) -> AgentState:
        """Use LLM to extract diagnosis phrases from the user query."""
        query = state.get("user_query", "") or ""
        if not query:
            return state

        # Track reasoning
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Interpret Intent", f"Analyzing your query to extract key clinical concepts..."))
        state["reasoning_steps"] = reasoning

        # If an intent service is wired, use it. Otherwise, fall back to the
        # raw query as a single phrase.
        try:
            if self.intent_service:
                phrases = self.intent_service.extract_diagnosis_phrases(query)
                reasoning.append(("Extract Phrases", f"Found {len(phrases)} key phrase(s): {', '.join(phrases[:3])}"))
            else:
                phrases = [query]
                reasoning.append(("Extract Phrases", "Using full query as search phrase"))
        except Exception as e:
            logger.warning(f"Intent extraction error, using raw query: {e}")
            phrases = [query]
            reasoning.append(("Extract Phrases", f"Intent extraction had an issue, using full query: {str(e)[:50]}"))

        state["diagnosis_phrases"] = phrases
        state["reasoning_steps"] = reasoning
        return state
    
    def _search_codes(self, state: AgentState) -> AgentState:
        """Search for relevant codes using vector search"""
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Vector Search", "Searching for standard clinical codes (ICD, SNOMED, etc.)..."))
        state["reasoning_steps"] = reasoning
        
        try:
            # Prefer diagnosis phrases extracted by the LLM; fall back to the
            # full user query if we don't have any.
            phrases = state.get("diagnosis_phrases") or [state.get("user_query", "")]
            search_text = "; ".join([p for p in phrases if p])

            codes = self.vector_service.search_codes(search_text, limit=10)
            state["codes"] = codes
            
            # Track which vocabularies / coding systems are represented
            # (e.g., ICD10CM, SNOMED, LOINC, etc.)
            if codes:
                vocabularies = sorted(
                    {c.get("vocabulary") for c in codes if c.get("vocabulary")}
                )
                state["vocabularies"] = vocabularies
                reasoning.append(("Code Search Results", f"Found {len(codes)} codes across {len(vocabularies)} vocabulary system(s): {', '.join(vocabularies)}"))
            if not codes:
                # No codes from vector search – don't hard fail. We'll fall back
                # to using only the original user query when building the Genie
                # request so the LLM still has a chance to interpret intent.
                logger.warning(
                    "Vector search returned no codes; will fall back to Genie with "
                    "original query only."
                )
                reasoning.append(("Code Search Results", "No codes found via vector search. Will use natural language query directly with Genie."))
            
            state["reasoning_steps"] = reasoning
            return state
        except Exception as e:
            logger.error(f"Error in search_codes: {str(e)}")
            state["error"] = f"Error searching for codes: {str(e)}"
            state["current_step"] = "error"
            reasoning.append(("Code Search Error", f"Error occurred: {str(e)[:100]}"))
            state["reasoning_steps"] = reasoning
            return state
    
    def _generate_sql(self, state: AgentState) -> AgentState:
        """Generate SQL query using Genie"""
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Generate SQL", "Building enriched request for Genie with codes and criteria..."))
        state["reasoning_steps"] = reasoning
        
        try:
            codes = state.get("codes", [])
            # If no codes came back from vector search, fall back to using only
            # the original user query to build a Genie request instead of
            # immediately failing. This lets the LLM still try to interpret
            # the intent even when the vector function returns nothing.
            if not codes:
                logger.warning("No codes found; falling back to Genie with original query only")
                criteria = {
                    'codes': [],
                    'original_query': state.get("user_query", ""),
                    'code_details': [],
                    'vocabularies': [],
                    'timeframe': '30 days',
                    'age': None,
                    'patient_table_prefix': config.patient_table_prefix,
                }
            else:
                # Extract criteria from query (simplified - could be enhanced with NLP)
                # Include both the original user query and full code details so Genie
                # gets a precise, disambiguated description of the clinical intent.
                top_codes = codes[:5]
                criteria = {
                    # Just the raw codes (used for WHERE clause)
                    'codes': [c['code'] for c in top_codes],
                    # Original natural language query from the user
                    'original_query': state.get("user_query", ""),
                    # Full code details (code + description + vocabulary) from vector search
                    'code_details': [
                        {
                            'code': c.get('code'),
                            'description': c.get('description'),
                            'vocabulary': c.get('vocabulary')
                        }
                        for c in top_codes
                    ],
                    # All vocabularies / coding systems involved (if any)
                    'vocabularies': state.get("vocabularies", []),
                    'timeframe': '30 days',  # Could extract from query later
                    'age': None,  # Could extract from query later
                    'patient_table_prefix': config.patient_table_prefix
                }
            
            # For conversational flow, start Genie conversation without blocking
            # This returns immediately so the UI doesn't hang
            result = self.genie_service.start_cohort_query(criteria)

            # Get the Genie response
            state["genie_prompt"] = result.get("prompt")
            state["genie_conversation_id"] = result.get('conversation_id')
            state["sql"] = result.get('sql')  # Will be None initially, populated after polling
            
            reasoning.append(("Genie Conversation Started", f"Started Genie conversation. SQL generation in progress..."))
            
            state["reasoning_steps"] = reasoning
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
            "error": None,
            "reasoning_steps": []
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

