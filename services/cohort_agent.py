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
    criteria_analysis: dict  # Structured breakdown: conditions, drugs, demographics, etc.
    codes: list  # All codes found from vector search
    selected_codes: list  # Codes user selected to use
    excluded_conditions: list  # Conditions user wants to exclude
    code_selection_mode: str  # "all" | "selected" | "excluded" | None
    vocabularies: list  # list of vocabulary_ids / coding systems represented in codes
    genie_prompt: str   # enriched, code-aware request that would be sent to Genie
    cohort_table: str
    cohort_count: int
    counts: dict  # dict with 'patients', 'visits', 'sites'
    genie_conversation_id: str
    current_step: str
    waiting_for: str  # "code_selection" | "analysis_decision" | None
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
        workflow.add_node("confirm_codes", self._confirm_codes)  # New: Handle code selection
        workflow.add_node("prepare_criteria", self._prepare_criteria)  # New: Prepare criteria with selected codes
        workflow.add_node("generate_sql", self._generate_sql)
        workflow.add_node("get_counts", self._get_counts)  # New: Get patients, visits, sites counts
        workflow.add_node("ask_for_analysis", self._ask_for_analysis)  # New: Ask about analysis
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
                "search_codes": "search_codes",  # User confirmed code search
                "code_selection": "confirm_codes",  # User responding to code selection
                "analysis": "ask_for_analysis",  # User wants to analyze
                "refine": "interpret_intent",  # User wants to refine - start over
                "follow_up": "answer_question",
                "insights": "answer_question",
                "error": "handle_error"
            }
        )
        
        # Flow for new cohort creation (conversational)
        # 1. Interpret intent → STOP (show structured breakdown, ask about code search)
        workflow.add_edge("interpret_intent", END)  # STOP here - show breakdown, ask about code search
        
        # After user confirms code search → 2. Search codes → 3. STOP (wait for user code selection)
        workflow.add_edge("search_codes", END)  # STOP here - wait for user to select codes
        
        # After user selects codes → 4. Confirm codes → 5. Prepare criteria → 6. Generate SQL → 7. Get counts → 8. STOP (ask about analysis)
        workflow.add_edge("confirm_codes", "prepare_criteria")
        workflow.add_edge("prepare_criteria", "generate_sql")
        workflow.add_edge("generate_sql", "get_counts")
        workflow.add_edge("get_counts", END)  # STOP here - ask user about analysis
        
        # User wants analysis → go to analysis
        workflow.add_edge("ask_for_analysis", END)
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
        # Normalize query: replace underscores and multiple spaces with single space
        query = query.replace("_", " ").replace("  ", " ").strip()
        waiting_for = state.get("waiting_for")
        
        # If we're waiting for code search confirmation
        if waiting_for == "code_search_confirmation":
            # Check if user wants to search for codes
            if any(phrase in query for phrase in ["yes", "search", "find codes", "look for codes", "get codes", "proceed"]):
                state["current_step"] = "search_codes"
                state["waiting_for"] = None
                return state
            elif any(phrase in query for phrase in ["no", "skip", "don't search"]):
                state["error"] = "Code search skipped. Please provide codes or refine your criteria."
                state["current_step"] = "error"
                return state
        
        # If we're waiting for code selection, check if user is responding
        if waiting_for == "code_selection":
            # Check for code selection responses - expanded list of phrases
            use_all_phrases = [
                "use all", "select all", "all codes", "all of them", "all", 
                "use all codes", "take all", "include all", "yes use all",
                "proceed with all", "all of these", "use them all",
                "use_all", "useall", "selectall"  # Handle variations with underscores/no spaces
            ]
            if any(phrase in query for phrase in use_all_phrases):
                state["code_selection_mode"] = "all"
                state["current_step"] = "code_selection"
                state["waiting_for"] = None
                return state
            
            # Check for specific code selection (user might list codes or say "select")
            selected_phrases = [
                "use selected", "use selected codes", "selected codes", "use these",
                "proceed with selected", "use the selected", "go with selected",
                "use my selection", "use my choices"
            ]
            if any(phrase in query for phrase in selected_phrases):
                # User confirmed selection from UI
                state["code_selection_mode"] = "selected"
                state["current_step"] = "code_selection"
                state["waiting_for"] = None
                return state
            
            # Check if user wants to select specific codes (but hasn't selected yet)
            select_phrases = [
                "i want to select", "select codes", "choose codes", "i want",
                "let me select", "i'll select", "i need to select", "show me selection",
                "i want to choose", "let me choose", "i'll choose"
            ]
            if any(phrase in query for phrase in select_phrases):
                # User wants to select - keep waiting_for as code_selection to show UI
                # Don't change waiting_for, just return to show selection UI
                state["current_step"] = "code_selection"
                return state
            
            # Check for exclusion
            exclude_phrases = [
                "exclude", "remove", "don't include", "without", "not include",
                "skip", "ignore", "don't want", "not these"
            ]
            if any(phrase in query for phrase in exclude_phrases):
                state["code_selection_mode"] = "excluded"
                state["current_step"] = "code_selection"
                state["waiting_for"] = None
                # Extract excluded conditions/codes
                return state
            
            # If user mentions specific codes (e.g., "I want E11.9, I50.9"), treat as selection
            # This is a simple heuristic - could be enhanced with regex
            if any(char.isdigit() or char == '.' for char in query) and len(query) < 100:
                # Might be listing codes - treat as wanting to select
                state["code_selection_mode"] = "selected"
                state["current_step"] = "code_selection"
                state["waiting_for"] = None
                return state
        
        # If we're waiting for analysis decision
        if waiting_for == "analysis_decision":
            if any(phrase in query for phrase in ["analyze", "analysis", "deep dive", "explore", "yes"]):
                state["current_step"] = "analysis"
                state["waiting_for"] = None
                return state
            elif any(phrase in query for phrase in ["refine", "change", "adjust", "modify", "different"]):
                state["current_step"] = "refine"
                state["waiting_for"] = None
                return state
        
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
    
    def _route_query(self, state: AgentState) -> Literal["new_cohort", "search_codes", "code_selection", "analysis", "refine", "follow_up", "insights", "error"]:
        """Route to appropriate node based on query type"""
        step = state.get("current_step", "new_cohort")
        
        if step == "new_cohort":
            return "new_cohort"
        elif step == "search_codes":
            return "search_codes"
        elif step == "code_selection":
            return "code_selection"
        elif step == "analysis":
            return "analysis"
        elif step == "refine":
            return "refine"
        elif step == "follow_up":
            return "follow_up"
        elif step == "insights":
            return "insights"
        else:
            return "error"
    
    def _interpret_intent(self, state: AgentState) -> AgentState:
        """Use LLM to analyze criteria and extract structured breakdown."""
        query = state.get("user_query", "") or ""
        if not query:
            return state

        # Track reasoning
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Interpret Intent", f"Analyzing your query to extract key clinical concepts..."))
        state["reasoning_steps"] = reasoning

        # Use analyze_criteria to get structured breakdown
        try:
            if self.intent_service:
                # Get structured analysis
                analysis = self.intent_service.analyze_criteria(query)
                state["criteria_analysis"] = analysis
                
                # Also extract diagnosis phrases for code search
                phrases = self.intent_service.extract_diagnosis_phrases(query)
                state["diagnosis_phrases"] = phrases
                
                reasoning.append(("Structured Analysis", f"Extracted conditions, demographics, medications, etc."))
            else:
                # Fallback: minimal structure
                analysis = {
                    "summary": query,
                    "conditions": [],
                    "drugs": [],
                    "procedures": [],
                    "demographics": [],
                    "timeframe": "",
                    "ambiguities": []
                }
                state["criteria_analysis"] = analysis
                state["diagnosis_phrases"] = [query]
                reasoning.append(("Extract Phrases", "Using full query as search phrase"))
        except Exception as e:
            logger.warning(f"Intent extraction error, using raw query: {e}")
            analysis = {
                "summary": query,
                "conditions": [],
                "drugs": [],
                "procedures": [],
                "demographics": [],
                "timeframe": "",
                "ambiguities": []
            }
            state["criteria_analysis"] = analysis
            state["diagnosis_phrases"] = [query]
            reasoning.append(("Extract Phrases", f"Intent extraction had an issue, using full query: {str(e)[:50]}"))

        # Set waiting_for to indicate we're waiting for code search confirmation
        state["waiting_for"] = "code_search_confirmation"
        state["reasoning_steps"] = reasoning
        return state
    
    def _search_codes(self, state: AgentState) -> AgentState:
        """Search for relevant codes using vector search based on structured criteria analysis"""
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Vector Search", "Searching for standard clinical codes (ICD, SNOMED, etc.)..."))
        state["reasoning_steps"] = reasoning
        
        try:
            # Use structured criteria_analysis to get conditions, medications, procedures
            analysis = state.get("criteria_analysis", {})
            all_codes = []
            
            # Search for conditions
            conditions = analysis.get("conditions", [])
            if conditions:
                for condition in conditions:
                    if condition and condition.strip():
                        try:
                            logger.info(f"Searching codes for condition: {condition}")
                            condition_codes = self.vector_service.search_codes(condition.strip(), limit=10)
                            # Ensure condition_codes is a list
                            if condition_codes:
                                # Tag each code with the condition it came from
                                for code in condition_codes:
                                    code = dict(code)  # Make a copy
                                    code["condition"] = condition.strip()
                                    all_codes.append(code)
                                logger.info(f"Found {len(condition_codes)} codes for condition: {condition}")
                            else:
                                logger.info(f"No codes found for condition: {condition}")
                        except Exception as e:
                            logger.error(f"Error searching codes for condition '{condition}': {e}")
                            continue
            
            # Search for medications/drugs
            drugs = analysis.get("drugs", [])
            if drugs:
                for drug in drugs:
                    if drug and drug.strip():
                        try:
                            logger.info(f"Searching codes for medication: {drug}")
                            drug_codes = self.vector_service.search_codes(drug.strip(), limit=10)
                            # Ensure drug_codes is a list
                            if drug_codes:
                                # Tag each code with the drug it came from
                                for code in drug_codes:
                                    code = dict(code)  # Make a copy
                                    code["drug"] = drug.strip()
                                    all_codes.append(code)
                                logger.info(f"Found {len(drug_codes)} codes for medication: {drug}")
                            else:
                                logger.info(f"No codes found for medication: {drug}")
                        except Exception as e:
                            logger.error(f"Error searching codes for drug '{drug}': {e}")
                            continue
            
            # Search for procedures
            procedures = analysis.get("procedures", [])
            if procedures:
                for procedure in procedures:
                    if procedure and procedure.strip():
                        try:
                            logger.info(f"Searching codes for procedure: {procedure}")
                            procedure_codes = self.vector_service.search_codes(procedure.strip(), limit=10)
                            # Ensure procedure_codes is a list
                            if procedure_codes:
                                # Tag each code with the procedure it came from
                                for code in procedure_codes:
                                    code = dict(code)  # Make a copy
                                    code["procedure"] = procedure.strip()
                                    all_codes.append(code)
                                logger.info(f"Found {len(procedure_codes)} codes for procedure: {procedure}")
                            else:
                                logger.info(f"No codes found for procedure: {procedure}")
                        except Exception as e:
                            logger.error(f"Error searching codes for procedure '{procedure}': {e}")
                            continue
            
            # If no structured analysis or no conditions/drugs/procedures, fall back to diagnosis phrases
            if not all_codes:
                phrases = state.get("diagnosis_phrases") or [state.get("user_query", "")]
                search_text = "; ".join([p for p in phrases if p])
                
                if search_text:
                    logger.info(f"Fallback: Searching codes using diagnosis phrases: {search_text}")
                    fallback_codes = self.vector_service.search_codes(search_text, limit=10)
                    # Ensure it's a list
                    if fallback_codes:
                        all_codes = fallback_codes if isinstance(fallback_codes, list) else list(fallback_codes)
                    else:
                        all_codes = []
                else:
                    logger.warning("No search text available for vector search")
                    state["codes"] = []
                    state["waiting_for"] = "code_selection"
                    reasoning.append(("Code Search Results", "No search text available"))
                    state["reasoning_steps"] = reasoning
                    return state
            
            # Ensure all_codes is always a list
            if not isinstance(all_codes, list):
                all_codes = []
            
            logger.info(f"Total codes found: {len(all_codes)}")
            
            # Ensure codes is always a list
            state["codes"] = all_codes
            
            # Track which vocabularies / coding systems are represented
            # (e.g., ICD10CM, SNOMED, LOINC, etc.)
            if all_codes:
                vocabularies = sorted(
                    {c.get("vocabulary") for c in all_codes if c.get("vocabulary")}
                )
                state["vocabularies"] = vocabularies
                reasoning.append(("Code Search Results", f"Found {len(all_codes)} codes across {len(vocabularies)} vocabulary system(s): {', '.join(vocabularies)}"))
            else:
                # No codes from vector search – don't hard fail. We'll fall back
                # to using only the original user query when building the Genie
                # request so the LLM still has a chance to interpret intent.
                logger.warning(
                    "Vector search returned no codes; will fall back to Genie with "
                    "original query only."
                )
                reasoning.append(("Code Search Results", "No codes found via vector search. Will use natural language query directly with Genie."))
            
            # Set waiting_for to indicate we're waiting for code selection
            state["waiting_for"] = "code_selection"
            state["reasoning_steps"] = reasoning
            return state
        except Exception as e:
            logger.error(f"Error in search_codes: {str(e)}")
            state["error"] = f"Error searching for codes: {str(e)}"
            state["current_step"] = "error"
            reasoning.append(("Code Search Error", f"Error occurred: {str(e)[:100]}"))
            state["reasoning_steps"] = reasoning
            return state
    
    def _confirm_codes(self, state: AgentState) -> AgentState:
        """Process user's code selection response"""
        reasoning = state.get("reasoning_steps", [])
        mode = state.get("code_selection_mode", "all")
        all_codes = state.get("codes", [])
        
        try:
            # Add friendly acknowledgment to reasoning
            reasoning.append(("Acknowledge Selection", "Accounting for your code selection..."))
            
            if mode == "all":
                # User wants to use all codes
                state["selected_codes"] = all_codes
                reasoning.append(("Code Selection", f"Using all {len(all_codes)} codes you selected"))
            elif mode == "selected":
                # User selected specific codes from UI
                # Check if selected_codes are already in state (from UI)
                pre_selected = state.get("selected_codes", [])
                if pre_selected:
                    state["selected_codes"] = pre_selected
                    reasoning.append(("Code Selection", f"Using {len(pre_selected)} selected codes from your choices"))
                else:
                    # Fallback: use all codes if nothing selected
                    state["selected_codes"] = all_codes
                    reasoning.append(("Code Selection", f"Using all {len(all_codes)} codes (no specific selection)"))
            elif mode == "excluded":
                # User wants to exclude certain conditions
                # For now, use all codes (could be enhanced to filter out excluded conditions)
                state["selected_codes"] = all_codes
                reasoning.append(("Code Selection", f"Using codes with exclusions applied"))
            
            reasoning.append(("Next Step", "Now finding matching patients with your selected codes..."))
            state["reasoning_steps"] = reasoning
            return state
        except Exception as e:
            logger.error(f"Error in confirm_codes: {str(e)}")
            state["error"] = f"Error processing code selection: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _prepare_criteria(self, state: AgentState) -> AgentState:
        """Prepare criteria with selected codes AND all other extracted criteria (demographics, medications, etc.) for Genie"""
        reasoning = state.get("reasoning_steps", [])
        selected_codes = state.get("selected_codes", [])
        criteria_analysis = state.get("criteria_analysis", {})
        
        reasoning.append(("Update Criteria", f"Updating criteria with {len(selected_codes)} selected codes"))
        reasoning.append(("Combine Criteria", "Combining selected codes with demographics, medications, and other criteria you specified"))
        state["reasoning_steps"] = reasoning
        
        try:
            selected_codes = state.get("selected_codes", [])
            original_query = state.get("user_query", "")
            
            # Extract all criteria components from structured analysis
            demographics = criteria_analysis.get("demographics", [])
            drugs = criteria_analysis.get("drugs", [])
            procedures = criteria_analysis.get("procedures", [])
            # Default to 5 years if user didn't specify, otherwise use their timeframe
            timeframe = criteria_analysis.get("timeframe", "") or "5 years"
            conditions = criteria_analysis.get("conditions", [])
            
            # Build enriched criteria combining codes with all other criteria
            if selected_codes:
                criteria = {
                    'codes': [c.get('code') for c in selected_codes if c.get('code')],
                    'original_query': original_query,
                    'code_details': [
                        {
                            'code': c.get('code'),
                            'description': c.get('description'),
                            'vocabulary': c.get('vocabulary')
                        }
                        for c in selected_codes
                    ],
                    'vocabularies': state.get("vocabularies", []),
                    # Include all extracted criteria
                    'demographics': demographics,
                    'drugs': drugs,
                    'procedures': procedures,
                    'conditions': conditions,  # Original conditions from analysis
                    'timeframe': timeframe,
                    'age': None,  # Could extract from demographics later
                    'patient_table_prefix': config.patient_table_prefix
                }
            else:
                # Fallback to original query with extracted criteria
                criteria = {
                    'codes': [],
                    'original_query': original_query,
                    'code_details': [],
                    'vocabularies': [],
                    'demographics': demographics,
                    'drugs': drugs,
                    'procedures': procedures,
                    'conditions': conditions,
                    'timeframe': timeframe,
                    'age': None,
                    'patient_table_prefix': config.patient_table_prefix
                }
            
            state["genie_prompt"] = self.genie_service._build_nl_query(criteria)
            state["reasoning_steps"] = reasoning
            return state
        except Exception as e:
            logger.error(f"Error in prepare_criteria: {str(e)}")
            state["error"] = f"Error preparing criteria: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _generate_sql(self, state: AgentState) -> AgentState:
        """Generate SQL query using Genie with selected codes AND all other criteria"""
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Send to Genie", "Sending your criteria to Genie to generate SQL query..."))
        reasoning.append(("Genie Processing", "Genie is analyzing your criteria and generating SQL (this may take a moment)..."))
        state["reasoning_steps"] = reasoning
        
        try:
            # Use selected_codes from code selection step
            selected_codes = state.get("selected_codes", [])
            original_query = state.get("user_query", "")
            criteria_analysis = state.get("criteria_analysis", {})
            
            # Extract all criteria components from structured analysis
            demographics = criteria_analysis.get("demographics", [])
            drugs = criteria_analysis.get("drugs", [])
            procedures = criteria_analysis.get("procedures", [])
            # Default to 5 years if user didn't specify, otherwise use their timeframe
            timeframe = criteria_analysis.get("timeframe", "") or "5 years"
            conditions = criteria_analysis.get("conditions", [])
            
            # Build criteria with selected codes AND all other extracted criteria
            if selected_codes:
                criteria = {
                    'codes': [c.get('code') for c in selected_codes if c.get('code')],
                    'original_query': original_query,
                    'code_details': [
                        {
                            'code': c.get('code'),
                            'description': c.get('description'),
                            'vocabulary': c.get('vocabulary')
                        }
                        for c in selected_codes
                    ],
                    'vocabularies': state.get("vocabularies", []),
                    # Include all extracted criteria
                    'demographics': demographics,
                    'drugs': drugs,
                    'procedures': procedures,
                    'conditions': conditions,
                    'timeframe': timeframe,
                    'age': None,
                    'patient_table_prefix': config.patient_table_prefix
                }
            else:
                # Fallback to original query with extracted criteria
                criteria = {
                    'codes': [],
                    'original_query': original_query,
                    'code_details': [],
                    'vocabularies': [],
                    'demographics': demographics,
                    'drugs': drugs,
                    'procedures': procedures,
                    'conditions': conditions,
                    'timeframe': timeframe,
                    'age': None,
                    'patient_table_prefix': config.patient_table_prefix
                }
            
            # Start Genie conversation and poll for completion
            result = self.genie_service.create_cohort_query(criteria)

            # Get the Genie response
            state["genie_prompt"] = result.get("prompt")
            state["genie_conversation_id"] = result.get('conversation_id')
            state["sql"] = result.get('sql')
            # Don't set cohort_count here - it will be set in _get_counts after extracting from the actual data
            # The row_count here is just the number of rows (1), not the patient count
            state["genie_data"] = result.get('data', [])  # Store data returned by Genie
            state["genie_columns"] = result.get('columns', [])  # Store column names from Genie
            
            if state["sql"]:
                reasoning.append(("SQL Generated", f"Genie generated SQL query ({len(state['sql'])} characters)"))
            else:
                reasoning.append(("SQL Generated", "SQL generation completed"))
            
            state["reasoning_steps"] = reasoning
            return state
        except Exception as e:
            logger.error(f"Error in generate_sql: {str(e)}")
            state["error"] = f"Error generating SQL: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _get_counts(self, state: AgentState) -> AgentState:
        """Get counts (patients, visits, sites) from Genie result
        
        CRITICAL: This extracts counts from the ACTUAL SQL execution results returned by Genie.
        These counts are used by users to assess criteria and decide if adjustments are needed.
        """
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Execute Query", "Executing SQL query to find matching patients..."))
        reasoning.append(("Get Counts", "Extracting patient, visit, and site counts from Genie SQL execution results..."))
        state["reasoning_steps"] = reasoning
        
        try:
            sql = state.get("sql")
            # CRITICAL: Use the ACTUAL data from Genie's SQL execution - this is the source of truth
            genie_data = state.get("genie_data", [])  # Data returned by Genie (actual SQL execution result)
            genie_columns = state.get("genie_columns", [])  # Column names from Genie result
            
            # Validate we have the actual Genie result
            if not genie_data:
                logger.error("⚠️ CRITICAL: No genie_data available - cannot extract counts from SQL execution results!")
                state["counts"] = {"patients": 0, "visits": 0, "sites": 0}
                state["cohort_count"] = 0
                reasoning.append(("Counts", "ERROR: No data from Genie SQL execution - cannot determine counts"))
                state["reasoning_steps"] = reasoning
                return state
            
            logger.info(f"🔍 CRITICAL: Extracting counts from Genie SQL execution result: {len(genie_data)} row(s), columns: {genie_columns}")
            
            # Check if Genie returned count results (single row with patient_count, visit_count, site_count)
            # This is the ACTUAL result from Genie's SQL execution - CRITICAL for user decision-making
                
                # Initialize cohort_count to 0 in case we can't extract it
                state["cohort_count"] = 0
                
                # If Genie returned a single row, it's likely count results - store it for UI to display
                if genie_data and len(genie_data) == 1:
                    # Check column names to see if this is a count result
                    column_names = [col.lower() if isinstance(col, str) else str(col).lower() for col in genie_columns] if genie_columns else []
                    has_count_columns = any(keyword in ' '.join(column_names) for keyword in ['patient_count', 'visit_count', 'site_count', 'count'])
                    
                    row = genie_data[0]
                    
                    # Convert row to dict if it's a list/tuple (using column names as keys)
                    if not isinstance(row, dict) and genie_columns:
                        # Row is a list/tuple - convert to dict using column names
                        try:
                            row = dict(zip(genie_columns, row))
                            logger.info(f"🔍 Converted list/tuple row to dict using columns: {genie_columns}")
                        except Exception as e:
                            logger.warning(f"Could not convert row to dict: {e}")
                    
                    # Store the row data for UI to display - simpler and more transparent
                    if isinstance(row, dict):
                        logger.info(f"🔍 CRITICAL: Genie SQL execution result row: {row}")
                        logger.info(f"🔍 CRITICAL: Column names from Genie: {genie_columns}")
                        logger.info(f"🔍 CRITICAL: Row keys: {list(row.keys())}")
                        logger.info(f"🔍 CRITICAL: Row values: {row}")
                        
                        # CRITICAL VALIDATION: Log what we're about to extract for user decision-making
                        logger.info(f"🔍 CRITICAL: This is the ACTUAL SQL execution result - counts will be used for user assessment")
                        
                        # Always store raw data for UI to display as dataframe
                        state["raw_count_data"] = {
                            "row": row,
                            "columns": genie_columns
                        }
                        
                        # Extract all numeric values first for fallback
                        numeric_values = []
                        for key, value in row.items():
                            try:
                                if value is not None:
                                    num_val = int(float(str(value)))
                                    if num_val > 0:  # Only positive counts
                                        numeric_values.append((key, num_val))
                            except (ValueError, TypeError):
                                pass
                        
                        # Sort by value (largest first) - patient count is usually largest
                        numeric_values.sort(key=lambda x: x[1], reverse=True)
                        logger.info(f"🔍 All numeric values found: {numeric_values}")
                        
                        # Simple extraction - look for common column name patterns
                        patient_count = 0
                        visit_count = 0
                        site_count = 0
                        
                        for key, value in row.items():
                            key_lower = str(key).lower()
                            try:
                                if value is not None:
                                    num_val = int(float(str(value)))
                                    # Match patient count - more flexible matching
                                    if 'patient' in key_lower and ('count' in key_lower or key_lower == 'patients'):
                                        patient_count = num_val
                                        logger.info(f"🔍 Found patient_count from column '{key}': {num_val}")
                                    # Match visit count
                                    elif ('visit' in key_lower or 'encounter' in key_lower) and ('count' in key_lower or key_lower in ['visits', 'encounter_count']):
                                        visit_count = num_val
                                        logger.info(f"🔍 Found visit_count from column '{key}': {num_val}")
                                    # Match site count
                                    elif ('site' in key_lower or 'provider' in key_lower) and ('count' in key_lower or key_lower in ['sites', 'provider_count']):
                                        site_count = num_val
                                        logger.info(f"🔍 Found site_count from column '{key}': {num_val}")
                            except (ValueError, TypeError):
                                pass
                        
                        # Fallback: If we didn't find specific matches, use numeric values in order
                        # BUT: Skip if the only numeric value is 1 (likely row_count, not actual count)
                        if patient_count == 0 and visit_count == 0 and site_count == 0 and numeric_values:
                            # Filter out value of 1 (likely row_count, not actual count)
                            filtered_values = [nv for nv in numeric_values if nv[1] > 1]
                            if filtered_values:
                                # Use first 3 filtered numeric values as patients, visits, sites
                                patient_count = filtered_values[0][1] if len(filtered_values) > 0 else 0
                                visit_count = filtered_values[1][1] if len(filtered_values) > 1 else 0
                                site_count = filtered_values[2][1] if len(filtered_values) > 2 else 0
                                logger.info(f"🔍 Used fallback numeric values: patients={patient_count}, visits={visit_count}, sites={site_count}")
                            else:
                                # If all values are 1 or 0, something is wrong - log it
                                logger.warning(f"🔍 WARNING: All numeric values are 1 or 0: {numeric_values}. This might be row_count, not actual counts!")
                                # Still try to use the first value if it exists
                                if numeric_values:
                                    patient_count = numeric_values[0][1]
                                    logger.info(f"🔍 Using first numeric value as fallback: {patient_count}")
                        
                        # CRITICAL: Store the extracted counts - these are from ACTUAL Genie SQL execution
                        state["counts"] = {
                            "patients": patient_count,
                            "visits": visit_count,
                            "sites": site_count
                        }
                        
                        # CRITICAL: Update cohort_count with the actual patient count from SQL execution
                        # This is what users will see to assess their criteria
                        if patient_count > 0:
                            state["cohort_count"] = patient_count
                            logger.info(f"✅ CRITICAL: Extracted patient_count={patient_count} from Genie SQL execution result")
                            logger.info(f"✅ CRITICAL: This count will be shown to user for criteria assessment")
                        elif numeric_values and len(numeric_values) > 0:
                            # Use the largest numeric value as fallback (already filtered to > 1)
                            fallback_count = numeric_values[0][1] if numeric_values[0][1] > 1 else 0
                            if fallback_count > 0:
                                state["cohort_count"] = fallback_count
                                patient_count = fallback_count  # Update for consistency
                                logger.warning(f"⚠️ CRITICAL: Used fallback extraction - patient_count={fallback_count} (column name matching failed)")
                                logger.warning(f"⚠️ CRITICAL: Row data: {row}, Columns: {genie_columns}")
                            else:
                                state["cohort_count"] = 0
                                logger.error(f"❌ CRITICAL: Could not extract patient count from Genie SQL execution result!")
                                logger.error(f"❌ CRITICAL: Row: {row}, Columns: {genie_columns}")
                        else:
                            # If we couldn't extract, set to 0
                            state["cohort_count"] = 0
                            logger.error(f"❌ CRITICAL: Could not extract patient count from Genie SQL execution result!")
                            logger.error(f"❌ CRITICAL: Row: {row}, Columns: {genie_columns}")
                        
                        # CRITICAL: Log final counts that will be shown to user
                        logger.info(f"✅ CRITICAL: Final counts from Genie SQL execution - patients={patient_count}, visits={visit_count}, sites={site_count}")
                        logger.info(f"✅ CRITICAL: User will see these counts to assess criteria and decide next steps")
                        reasoning.append(("Counts", f"Extracted from Genie SQL execution: {patient_count} patients, {visit_count} visits, {site_count} sites"))
                    else:
                        # Not a dict, store as-is
                        logger.warning(f"Genie returned single row but it's not a dict: {type(row)}")
                        state["raw_count_data"] = {
                            "row": row,
                            "columns": genie_columns
                        }
                        state["counts"] = {
                            "patients": 0,
                            "visits": 0,
                            "sites": 0
                        }
                        # Set cohort_count to 0 since we couldn't extract patient count
                        state["cohort_count"] = 0
                        reasoning.append(("Counts", "Stored raw data for display (could not extract counts)"))
                else:
                    # Genie returned patient-level data - need to calculate distinct counts
                    # Try to extract distinct counts from the data
                    row_count = state.get("cohort_count", 0)
                    
                    if genie_data and len(genie_data) > 0:
                        # Try to count distinct patients, visits, sites from the data
                        try:
                            patient_ids = set()
                            visit_ids = set()
                            site_ids = set()
                            
                            for row in genie_data:
                                if isinstance(row, dict):
                                    # Try common column names for patient/visit/site identifiers
                                    patient_id = row.get("patient_id") or row.get("PAT_KEY") or row.get("MEDREC_KEY") or row.get("patient_key")
                                    visit_id = row.get("visit_id") or row.get("encounter_id") or row.get("PAT_KEY") or row.get("patient_key")
                                    site_id = row.get("site_id") or row.get("PROV_ID") or row.get("PROVIDER_KEY") or row.get("provider_id")
                                    
                                    if patient_id:
                                        patient_ids.add(str(patient_id))
                                    if visit_id:
                                        visit_ids.add(str(visit_id))
                                    if site_id:
                                        site_ids.add(str(site_id))
                            
                            # Use distinct counts if found, otherwise use row_count
                            patient_count = len(patient_ids) if patient_ids else row_count
                            visit_count = len(visit_ids) if visit_ids else 0
                            site_count = len(site_ids) if site_ids else 0
                            
                            state["counts"] = {
                                "patients": patient_count,
                                "visits": visit_count,
                                "sites": site_count
                            }
                            # Update cohort_count with the actual patient count
                            state["cohort_count"] = patient_count
                            reasoning.append(("Counts", f"Found {patient_count} patients, {visit_count} visits, {site_count} sites (from data analysis)"))
                        except Exception as e:
                            logger.warning(f"Error analyzing data for counts: {e}")
                            # Fallback to row_count
                            state["counts"] = {
                                "patients": row_count,
                                "visits": 0,
                                "sites": 0
                            }
                            # Update cohort_count (this is a fallback, so row_count might be correct here)
                            state["cohort_count"] = row_count
                            reasoning.append(("Counts", f"Found {row_count} patients (using row count, couldn't analyze data)"))
                    else:
                        # No data, use row_count
                        state["counts"] = {
                            "patients": row_count,
                            "visits": 0,
                            "sites": 0
                        }
                        # Update cohort_count (this is a fallback, so row_count might be correct here)
                        state["cohort_count"] = row_count
                        reasoning.append(("Counts", f"Found {row_count} patients (no data returned)"))
            
            # Set waiting_for to indicate we're waiting for analysis decision
            state["waiting_for"] = "analysis_decision"
            state["reasoning_steps"] = reasoning
            return state
        except Exception as e:
            logger.error(f"Error in get_counts: {str(e)}")
            state["error"] = f"Error getting counts: {str(e)}"
            state["current_step"] = "error"
            return state
    
    def _ask_for_analysis(self, state: AgentState) -> AgentState:
        """Ask user if they want to analyze the cohort"""
        reasoning = state.get("reasoning_steps", [])
        reasoning.append(("Ask for Analysis", "Preparing to ask user about cohort analysis"))
        state["reasoning_steps"] = reasoning
        state["waiting_for"] = None  # Analysis decision handled
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
        # Initialize state - preserve important state from previous turns
        initial_state = {
            "messages": [HumanMessage(content=user_query)],
            "user_query": user_query,
            "session_id": session_id,
            "codes": existing_state.get("codes", []) if existing_state else [],
            "cohort_table": existing_state.get("cohort_table") if existing_state else None,
            "cohort_count": existing_state.get("cohort_count") if existing_state else 0,
            "genie_conversation_id": existing_state.get("genie_conversation_id") if existing_state else None,
            "waiting_for": existing_state.get("waiting_for") if existing_state else None,
            "criteria_analysis": existing_state.get("criteria_analysis") if existing_state else None,
            "diagnosis_phrases": existing_state.get("diagnosis_phrases", []) if existing_state else [],
            "selected_codes": existing_state.get("selected_codes", []) if existing_state else [],
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

