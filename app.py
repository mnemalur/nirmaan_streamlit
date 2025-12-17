"""
Clinical Cohort Assistant - Streamlit Version
Databricks-powered cohort builder with natural language interface
"""

import streamlit as st
import os

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Services will be imported and initialized after configuration
VectorSearchService = None
GenieService = None
CohortManager = None

# Page config
st.set_page_config(
    page_title="Clinical Cohort Assistant",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'cohort_table' not in st.session_state:
    st.session_state.cohort_table = None
if 'genie_conversation_id' not in st.session_state:
    st.session_state.genie_conversation_id = None
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'services_initialized' not in st.session_state:
    st.session_state.services_initialized = False
if 'config' not in st.session_state:
    st.session_state.config = {}
if 'agent_state' not in st.session_state:
    st.session_state.agent_state = {}
if 'criteria_analysis' not in st.session_state:
    st.session_state.criteria_analysis = None
if 'criteria_text' not in st.session_state:
    st.session_state.criteria_text = ""
if 'codes' not in st.session_state:
    st.session_state.codes = []
if 'selected_codes' not in st.session_state:
    st.session_state.selected_codes = []
if 'refined_criteria' not in st.session_state:
    st.session_state.refined_criteria = None
if 'refined_criteria_text' not in st.session_state:
    st.session_state.refined_criteria_text = ""
if 'code_search_text' not in st.session_state:
    st.session_state.code_search_text = ""
if 'code_search_error' not in st.session_state:
    st.session_state.code_search_error = ""
if 'selected_codes' not in st.session_state:
    st.session_state.selected_codes = []


def initialize_services():
    """Initialize Databricks services with configured credentials"""
    try:
        # If session state has config, use it (overrides .env)
        # Set environment variables from session state config first (if exists)
        if st.session_state.config:
            for key, value in st.session_state.config.items():
                if value:  # Only set if value is not empty
                    os.environ[key] = value
        
        # Import config (it loads from .env file automatically via load_dotenv())
        # Note: config object is created at import time, so it reads from os.environ
        # If session_state.config was set above, those values are now in os.environ
        # But config object was already created, so we need to check os.environ directly
        # OR reload the config module - but simpler: check os.environ directly for missing values
        from config import config as db_config
        
        # Re-check from os.environ in case config object was created before session_state values were set
        # This ensures we get the latest values
        import importlib
        import config as config_module
        importlib.reload(config_module)
        db_config = config_module.config
        
        # Verify required config (check both config object and env vars)
        missing = []
        if not db_config.host:
            missing.append('DATABRICKS_HOST')
        if not db_config.token:
            missing.append('DATABRICKS_TOKEN')
        if not db_config.space_id:
            missing.append('GENIE_SPACE_ID')
        if not db_config.patient_catalog:
            missing.append('PATIENT_CATALOG')
        if not db_config.patient_schema:
            missing.append('PATIENT_SCHEMA')
        if not db_config.warehouse_id:
            missing.append('SQL_WAREHOUSE_ID')
        
        if missing:
            st.error(f"Missing required configuration: {', '.join(missing)}")
            return False
        
        # Clear OAuth environment variables to avoid conflicts
        # WorkspaceClient will use OAuth if client_id is set, even if token is provided
        oauth_vars = ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET', 'DATABRICKS_OAUTH_CLIENT_ID', 'DATABRICKS_OAUTH_CLIENT_SECRET']
        for var in oauth_vars:
            if var in os.environ:
                logger.info(f"Clearing OAuth env var: {var}")
                del os.environ[var]
        
        # Verify host format (should be full URL, not just port)
        if db_config.host and not db_config.host.startswith('http'):
            logger.warning(f"Host format may be incorrect: {db_config.host}. Expected full URL like https://workspace.cloud.databricks.com")
        
        # Import and initialize services
        from services.vector_search import VectorSearchService
        from services.genie_service import GenieService
        from services.cohort_manager import CohortManager
        from services.intent_service import IntentService
        from services.cohort_agent import CohortAgent
        
        st.session_state.vector_service = VectorSearchService()
        st.session_state.genie_service = GenieService()
        st.session_state.cohort_manager = CohortManager()
        st.session_state.intent_service = IntentService()
        
        # Initialize LangGraph agent (LLM + vector search + Genie + cohort manager)
        st.session_state.cohort_agent = CohortAgent(
            st.session_state.vector_service,
            st.session_state.genie_service,
            st.session_state.cohort_manager,
            st.session_state.intent_service
        )
        
        st.session_state.services_initialized = True
        
        return True
    except Exception as e:
        st.error(f"Error initializing services: {str(e)}")
        logger.error(f"Service initialization error: {str(e)}", exc_info=True)
        return False


def run_databricks_health_check():
    """Run a simple SELECT 1 against the configured Databricks SQL Warehouse."""
    try:
        # Reload config to pick up any values saved via the UI/config form
        import importlib
        import config as config_module
        importlib.reload(config_module)
        db_config = config_module.config
        from databricks.sql import connect

        # Ensure required configuration is present
        missing = []
        if not db_config.host:
            missing.append("DATABRICKS_HOST")
        if not db_config.token:
            missing.append("DATABRICKS_TOKEN")
        if not db_config.warehouse_id:
            missing.append("SQL_WAREHOUSE_ID")

        if missing:
            return False, f"Missing required configuration: {', '.join(missing)}"

        server_hostname = db_config.host.replace("https://", "").replace("http://", "")
        http_path = f"/sql/1.0/warehouses/{db_config.warehouse_id}"

        # Open a short-lived connection and run a trivial query
        with connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=db_config.token,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                row = cursor.fetchone()

        return True, row[0] if row else None
    except Exception as e:
        logger.error(f"Databricks health check failed: {e}", exc_info=True)
        return False, str(e)


def refine_criteria_with_codes():
    """Build a refined, natural-language criteria description using selected codes."""
    analysis = st.session_state.get("criteria_analysis") or {}
    selected_codes = st.session_state.get("selected_codes") or []
    original_text = st.session_state.get("criteria_text") or ""

    if not selected_codes:
        st.warning("No codes selected to refine the criteria.")
        return ""

    summary = analysis.get("summary") or original_text
    conditions = analysis.get("conditions") or []
    demographics = analysis.get("demographics") or []
    timeframe = analysis.get("timeframe") or ""

    refined_text = summary.strip() if summary else original_text.strip()

    # We avoid repeating all individual codes in the user-facing text since the
    # user has already reviewed them and they will be used in the WHERE clause.
    # Instead, we acknowledge that we will use the selected standard codes for
    # the condition(s) they described.
    if conditions:
        if refined_text and not refined_text.endswith("."):
            refined_text += "."
        cond_text = ", ".join(conditions)
        refined_text += (
            f" I will use the standard diagnosis codes you just reviewed for: {cond_text}."
        )

    if timeframe:
        if not refined_text.endswith("."):
            refined_text += "."
        refined_text += f" The timeframe is {timeframe}."

    if demographics:
        if not refined_text.endswith("."):
            refined_text += "."
        refined_text += " Demographic filters include: " + ", ".join(demographics) + "."

    st.session_state.refined_criteria = {
        "original_text": original_text,
        "summary": summary,
        "conditions": conditions,
        "selected_codes": selected_codes,
        "demographics": demographics,
        "timeframe": timeframe,
    }
    st.session_state.refined_criteria_text = refined_text

    return refined_text


def search_codes_for_criteria(criteria_text: str):
    """Use vector search to find standard codes for the given criteria text.

    This only updates session state; the UI for reviewing and selecting codes
    is rendered in render_chat_page so that buttons work correctly across reruns.
    """
    if not criteria_text:
        st.session_state.code_search_error = "No criteria available to search for codes."
        st.session_state.codes = []
        return

    if not hasattr(st.session_state, "vector_service") or st.session_state.vector_service is None:
        st.session_state.code_search_error = "Vector search service is not initialized. Please check configuration."
        st.session_state.codes = []
        return

    # Prefer the structured "conditions" extracted during criteria analysis.
    analysis = st.session_state.get("criteria_analysis") or {}
    conditions = analysis.get("conditions") or []

    if conditions:
        search_text = "; ".join(conditions)
    else:
        # Fallback: use the intent service (if available) to extract diagnosis phrases
        try:
            if hasattr(st.session_state, "intent_service") and st.session_state.intent_service is not None:
                phrases = st.session_state.intent_service.extract_diagnosis_phrases(criteria_text)
                search_text = "; ".join([p for p in phrases if p])
            else:
                search_text = criteria_text
        except Exception as e:
            logger.warning(f"Intent extraction before code search failed, using raw criteria: {e}")
            search_text = criteria_text

    st.session_state.code_search_text = search_text

    try:
        with st.spinner("Looking up matching diagnosis and drug codes..."):
            codes = st.session_state.vector_service.search_codes(search_text, limit=10)
    except Exception as e:
        msg = f"Error searching for codes: {e}"
        st.session_state.code_search_error = msg
        st.session_state.codes = []
        logger.error(msg, exc_info=True)
        return

    st.session_state.code_search_error = ""
    st.session_state.codes = codes


def render_config_page():
    """Configuration page for Databricks credentials"""
    st.title("🔧 Configuration")
    st.markdown("Configure your Databricks connection details")
    
    with st.form("config_form"):
        st.subheader("Databricks Connection")
        host = st.text_input(
            "Databricks Host",
            value=st.session_state.config.get('DATABRICKS_HOST', ''),
            help="e.g., https://your-workspace.cloud.databricks.com",
            placeholder="https://your-workspace.cloud.databricks.com"
        )
        token = st.text_input(
            "Databricks Token",
            value=st.session_state.config.get('DATABRICKS_TOKEN', ''),
            type="password",
            help="Personal access token or service principal token"
        )
        
        st.subheader("Genie Configuration")
        space_id = st.text_input(
            "Genie Space ID",
            value=st.session_state.config.get('GENIE_SPACE_ID', ''),
            help="Genie space ID for SQL generation"
        )
        
        st.subheader("Patient Data Configuration")
        patient_catalog = st.text_input(
            "Patient Catalog",
            value=st.session_state.config.get('PATIENT_CATALOG', ''),
            help="Unity Catalog name for patient data",
            placeholder="main"
        )
        patient_schema = st.text_input(
            "Patient Schema",
            value=st.session_state.config.get('PATIENT_SCHEMA', ''),
            help="Schema name for patient tables",
            placeholder="clinical"
        )
        
        st.subheader("Vector Search Configuration")
        vector_catalog = st.text_input(
            "Vector Catalog",
            value=st.session_state.config.get('VECTOR_CATALOG', ''),
            help="Unity Catalog name for vector functions",
            placeholder="main"
        )
        vector_schema = st.text_input(
            "Vector Schema",
            value=st.session_state.config.get('VECTOR_SCHEMA', ''),
            help="Schema name for vector functions",
            placeholder="vector_functions"
        )
        
        st.subheader("SQL Warehouse")
        warehouse_id = st.text_input(
            "SQL Warehouse ID",
            value=st.session_state.config.get('SQL_WAREHOUSE_ID', ''),
            help="SQL Warehouse ID for query execution"
        )
        
        submitted = st.form_submit_button("Save Configuration", type="primary", use_container_width=True)
        
        if submitted:
            # Save config to session state
            st.session_state.config = {
                'DATABRICKS_HOST': host,
                'DATABRICKS_TOKEN': token,
                'GENIE_SPACE_ID': space_id,
                'PATIENT_CATALOG': patient_catalog,
                'PATIENT_SCHEMA': patient_schema,
                'VECTOR_CATALOG': vector_catalog or patient_catalog,
                'VECTOR_SCHEMA': vector_schema,
                'SQL_WAREHOUSE_ID': warehouse_id
            }
            
            # Try to initialize services
            with st.spinner("Initializing Databricks services..."):
                if initialize_services():
                    st.success("✅ Configuration saved and services initialized!")
                    st.rerun()
                else:
                    st.error("❌ Failed to initialize services. Please check your configuration.")

    st.markdown("### Connection Health Check")
    st.caption("Run a simple test query on your SQL Warehouse to validate connectivity.")
    if st.button("Test Databricks connection", use_container_width=True):
        with st.spinner("Running health check..."):
            ok, info = run_databricks_health_check()
        if ok:
            st.success(f"✅ Databricks SQL Warehouse is reachable (SELECT 1 returned {info}).")
        else:
            st.error(f"❌ Databricks health check failed: {info}")


def render_chat_page():
    """Main chat interface"""
    st.title("🏥 Clinical Cohort Assistant")
    st.markdown("Build patient cohorts using natural language queries")
    
    # Check if services are initialized
    if not st.session_state.services_initialized:
        # Debug info to help diagnose
        from config import config as db_config
        with st.expander("🔍 Debug Info (Click to see why auto-init didn't work)", expanded=False):
            st.write("**Environment Check:**")
            st.write(f"- .env file exists: {os.path.exists('.env')}")
            st.write(f"- Current working directory: {os.getcwd()}")
            st.write("**Config Values:**")
            st.write(f"- DATABRICKS_HOST: {'✅ Set' if db_config.host else '❌ Missing'}")
            st.write(f"- DATABRICKS_TOKEN: {'✅ Set' if db_config.token else '❌ Missing'}")
            st.write(f"- GENIE_SPACE_ID: {'✅ Set' if db_config.space_id else '❌ Missing'}")
            st.write(f"- PATIENT_CATALOG: {'✅ ' + str(db_config.patient_catalog) if db_config.patient_catalog else '❌ Missing'}")
            st.write(f"- PATIENT_SCHEMA: {'✅ ' + str(db_config.patient_schema) if db_config.patient_schema else '❌ Missing'}")
            st.write(f"- SQL_WAREHOUSE_ID: {'✅ Set' if db_config.warehouse_id else '❌ Missing'}")
            st.write("**Note:** If values show as Missing, check:")
            st.write("1. .env file exists in same directory as app.py")
            st.write("2. .env file has all required variables")
            st.write("3. No typos in variable names")
        
        st.warning("⚠️ Please configure Databricks connection in the sidebar first.")
        return

    # Criteria understanding (Milestone 1) – lightweight analysis before the full agent flow
    with st.expander("🧩 Understand my clinical criteria (Milestone 1)", expanded=True):
        st.caption(
            "I’ll read your draft criteria, summarize how I understand it, "
            "highlight key clinical concepts, and call out anything that seems ambiguous "
            "before moving on to look up standard diagnosis and drug codes."
        )
        with st.form("criteria_analysis_form"):
            criteria_text = st.text_area(
                "Describe your clinical criteria in natural language",
                value="",
                placeholder="e.g., Adults 50–80 with at least two encounters for heart failure in the last 3 years, currently on beta-blockers.",
                height=120,
            )
            analyze_submitted = st.form_submit_button("Analyze criteria", use_container_width=True)

        if analyze_submitted and criteria_text:
            if not hasattr(st.session_state, "intent_service") or st.session_state.intent_service is None:
                st.error("Intent service is not initialized. Please check configuration.")
            else:
                with st.spinner("Analyzing criteria..."):
                    analysis = st.session_state.intent_service.analyze_criteria(criteria_text)
                    st.session_state.criteria_analysis = analysis
                    st.session_state.criteria_text = criteria_text

        analysis = st.session_state.get("criteria_analysis")
        if analysis:
            st.subheader("How I understand your criteria")
            st.write(analysis.get("summary", ""))

            # Only show concept groups that actually have content, so it doesn't
            # feel like you're being told something is “missing” when you never
            # specified it.
            col1, col2 = st.columns(2)
            with col1:
                conditions = analysis.get("conditions") or []
                if conditions:
                    st.markdown("**Conditions**")
                    st.write(", ".join(conditions))

                drugs = analysis.get("drugs") or []
                if drugs:
                    st.markdown("**Drugs**")
                    st.write(", ".join(drugs))

                procedures = analysis.get("procedures") or []
                if procedures:
                    st.markdown("**Procedures**")
                    st.write(", ".join(procedures))

            with col2:
                demographics = analysis.get("demographics") or []
                if demographics:
                    st.markdown("**Demographics**")
                    st.write(", ".join(demographics))

                timeframe = analysis.get("timeframe") or ""
                if timeframe:
                    st.markdown("**Timeframe**")
                    st.write(timeframe)

            ambiguities = analysis.get("ambiguities", [])
            st.markdown("**Ambiguities / things to clarify**")
            if ambiguities:
                st.info(
                    f"I see {len(ambiguities)} point(s) that could affect how I match this "
                    "to patients. You can refine the text above and re-run the analysis, or "
                    "click **Continue with this criteria and search for codes** below and "
                    "I’ll still try to find the best matching codes based on what you wrote."
                )
                for a in ambiguities:
                    st.markdown(f"- {a}")
            else:
                st.success(
                    "I don’t see major ambiguities. This looks specific enough to start "
                    "mapping to standard codes in the next step. You can tweak the text above "
                    "or go ahead and click **Continue with this criteria and search for codes**."
                )

            # Let the user move from understanding → action in one click
            if st.button("Continue with this criteria and search for codes", use_container_width=True):
                criteria_text_for_codes = st.session_state.get("criteria_text") or ""
                search_codes_for_criteria(criteria_text_for_codes)

    # If we have attempted a code search, show what we did and let the user choose
    # how to use the codes.
    if st.session_state.code_search_text:
        st.markdown("**Text I used to search for codes**")
        st.write(st.session_state.code_search_text)

    if st.session_state.code_search_error:
        st.error(st.session_state.code_search_error)
    elif st.session_state.codes:
        codes = st.session_state.codes

        st.markdown(
            "I've taken your criteria and, based on the key clinical phrases, "
            "looked up relevant standard codes across the available vocabularies. "
            "Review them below and decide whether to use all of them or only a subset."
        )
        st.subheader("📋 Relevant codes I found")
        code_df = pd.DataFrame(codes)
        display_cols = ['code', 'description', 'vocabulary']
        available_cols = [col for col in display_cols if col in code_df.columns]
        st.dataframe(code_df[available_cols], use_container_width=True, hide_index=True)

        st.markdown("### How should I use these codes?")
        choice = st.radio(
            "",
            ["Use all suggested codes", "Let me choose specific codes"],
            index=0,
            horizontal=True,
        )

        selected_codes = codes
        if choice == "Let me choose specific codes":
            # Build nice labels for the multiselect
            label_to_code = {
                f"{c.get('code')} – {c.get('description')} ({c.get('vocabulary')})": c
                for c in codes
            }
            selected_labels = st.multiselect(
                "Choose the codes you want me to use when I go look for patients:",
                options=list(label_to_code.keys()),
            )
            selected_codes = [label_to_code[label] for label in selected_labels]

        st.session_state.selected_codes = selected_codes

        if selected_codes:
            st.success(
                f"I'll carry forward {len(selected_codes)} code(s) when we move on to "
                "build the cohort definition and, in the next milestone, search for patients."
            )

            if st.button("Add these codes and refine criteria", use_container_width=True):
                refined_text = refine_criteria_with_codes()
                if refined_text:
                    st.subheader("Refined criteria I'll use going forward")
                    st.write(refined_text)
        else:
            st.warning(
                "You haven't selected any codes yet. I won't be able to build a cohort "
                "until there is at least one code to represent the condition."
            )


def process_query(query: str):
    """Process user query using LangGraph agent"""
    # Add user message
    st.session_state.messages.append({"role": "user", "content": query})
    
    with st.chat_message("user"):
        st.markdown(query)
    
    # Process query through LangGraph agent
    with st.chat_message("assistant"):
        with st.spinner("Processing query..."):
            try:
                # Get existing state for context
                existing_state = {
                    "cohort_table": st.session_state.get("cohort_table"),
                    "cohort_count": st.session_state.get("cohort_count", 0),
                    "genie_conversation_id": st.session_state.get("genie_conversation_id")
                }
                
                # Process through agent
                result_state = st.session_state.cohort_agent.process_query(
                    query,
                    st.session_state.session_id,
                    existing_state
                )
                
                # Update session state with results
                if result_state.get("cohort_table"):
                    st.session_state.cohort_table = result_state["cohort_table"]
                if result_state.get("cohort_count"):
                    st.session_state.cohort_count = result_state["cohort_count"]
                if result_state.get("genie_conversation_id"):
                    st.session_state.genie_conversation_id = result_state["genie_conversation_id"]
                
                # Store agent state for next turn
                st.session_state.agent_state = result_state
                
                # Handle errors
                if result_state.get("error"):
                    st.error(result_state["error"])
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": result_state["error"]
                    })
                    return
                
                # Display results based on step type
                current_step = result_state.get("current_step", "")
                
                if current_step == "new_cohort":
                    # Display codes if available
                    codes = result_state.get("codes", [])
                    if codes:
                        # Trust-building explanation for the user
                        st.markdown(
                            "I've interpreted your request and looked up relevant standard codes "
                            "across the available vocabularies. Review these codes to see how I'm "
                            "making your request more precise before sending it on to Genie."
                        )
                        st.subheader("📋 Relevant Codes Found")
                        code_df = pd.DataFrame(codes)
                        display_cols = ['code', 'description', 'vocabulary', 'confidence']
                        available_cols = [col for col in display_cols if col in code_df.columns]
                        st.dataframe(code_df[available_cols], use_container_width=True, hide_index=True)
                    
                    # Show preview of the Genie request (prompt) instead of calling Genie
                    genie_prompt = result_state.get("genie_prompt")
                    if genie_prompt:
                        st.subheader("🧠 Genie Request Preview (not yet executed)")
                        st.code(genie_prompt, language="markdown")
                        st.info("This is the enriched, code-aware request that would be sent to Genie.")
                    
                    # Display SQL if available (will be None in preview mode)
                    sql = result_state.get("sql")
                    if sql:
                        st.subheader("📝 Generated SQL")
                        st.code(sql, language="sql")
                    
                    # Display cohort results (not used in preview mode)
                    count = result_state.get("cohort_count", 0)
                    if count > 0:
                        st.success(f"✅ Found {count} patients")
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": f"Found {count} patients matching your criteria.",
                            "data": {
                                "count": count,
                                "sql": sql,
                                "codes": codes
                            }
                        })
                
                elif current_step in ["follow_up", "insights"]:
                    # Handle follow-up questions and insights
                    answer_data = result_state.get("answer_data", {})
                    answer_type = answer_data.get("type")
                    data = answer_data.get("data")
                    
                    if answer_type == "demographics":
                        st.subheader("👥 Demographics")
                        display_demographics(data)
                    elif answer_type == "sites":
                        st.subheader("🏥 Site Characteristics")
                        display_sites(data)
                    elif answer_type == "trends":
                        st.subheader("📈 Admission Trends")
                        display_trends(data)
                    elif answer_type == "outcomes":
                        st.subheader("📊 Outcomes")
                        display_outcomes(data)
                    elif answer_type == "count":
                        st.info(f"📊 The cohort contains {data} patients")
                    elif answer_type == "genie":
                        st.subheader("🤖 Genie Response")
                        if data.get("sql"):
                            st.code(data["sql"], language="sql")
                        if data.get("data"):
                            st.dataframe(pd.DataFrame(data["data"]), use_container_width=True)
                    
                    # Add response to messages
                    response_text = f"Here's the information you requested."
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response_text,
                        "data": answer_data
                    })
                
            except Exception as e:
                error_msg = f"Error processing query: {str(e)}"
                st.error(error_msg)
                logger.error(error_msg, exc_info=True)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })


def display_demographics(data: dict):
    """Display demographics data"""
    if data.get('age_gender'):
        st.write("**Age & Gender Distribution**")
        df = pd.DataFrame(data['age_gender'])
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    if data.get('gender'):
        st.write("**Gender Distribution**")
        df = pd.DataFrame(data['gender'])
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    if data.get('race'):
        st.write("**Race Distribution**")
        df = pd.DataFrame(data['race'])
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    if data.get('ethnicity'):
        st.write("**Ethnicity Distribution**")
        df = pd.DataFrame(data['ethnicity'])
        st.dataframe(df, use_container_width=True, hide_index=True)


def display_sites(data: dict):
    """Display site breakdown data"""
    if data.get('teaching_status'):
        st.write("**Teaching Status**")
        df = pd.DataFrame(data['teaching_status'])
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    if data.get('urban_rural'):
        st.write("**Urban vs Rural**")
        df = pd.DataFrame(data['urban_rural'])
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    if data.get('bed_count'):
        st.write("**Bed Count Groups**")
        df = pd.DataFrame(data['bed_count'])
        st.dataframe(df, use_container_width=True, hide_index=True)


def display_trends(data: list):
    """Display admission trends"""
    if data:
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True, hide_index=True)


def display_outcomes(data: dict):
    """Display outcomes data"""
    if data:
        st.json(data)


def generate_insights():
    """Generate comprehensive insights report"""
    if not st.session_state.cohort_table:
        st.error("No cohort created yet. Please create a cohort first.")
        return
    
    with st.spinner("Generating insights..."):
        try:
            # Get insights
            demographics = st.session_state.cohort_manager.get_demographics(st.session_state.cohort_table)
            sites = st.session_state.cohort_manager.get_site_breakdown(st.session_state.cohort_table)
            trends = st.session_state.cohort_manager.get_admission_trends(st.session_state.cohort_table)
            outcomes = st.session_state.cohort_manager.get_outcomes(st.session_state.cohort_table)
            
            # Display insights
            st.header("📊 Cohort Analysis Report")
            
            # Patient Characteristics
            st.subheader("👥 Patient Characteristics")
            col1, col2 = st.columns(2)
            
            with col1:
                # Age & Gender
                if demographics.get('age_gender') and len(demographics['age_gender']) > 0:
                    age_gender_data = demographics['age_gender']
                    age_groups = sorted(set(d['age_group'] for d in age_gender_data))
                    male_data = [
                        next((d['count'] for d in age_gender_data if d['age_group'] == age and d['gender'] == 'M'), 0)
                        for age in age_groups
                    ]
                    female_data = [
                        next((d['count'] for d in age_gender_data if d['age_group'] == age and d['gender'] == 'F'), 0)
                        for age in age_groups
                    ]
                    
                    fig = go.Figure(data=[
                        go.Bar(name='Male', x=age_groups, y=male_data, marker_color='rgba(59, 130, 246, 0.8)'),
                        go.Bar(name='Female', x=age_groups, y=female_data, marker_color='rgba(236, 72, 153, 0.8)')
                    ])
                    fig.update_layout(
                        barmode='group',
                        title='Age & Gender Distribution',
                        xaxis_title='Age Group',
                        yaxis_title='Patient Count',
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Age & Gender data not available")
                
                # Race
                if demographics.get('race') and len(demographics['race']) > 0:
                    race_data = demographics['race']
                    fig = go.Figure(data=[
                        go.Bar(
                            x=[r['count'] for r in race_data],
                            y=[r['value'] for r in race_data],
                            orientation='h',
                            marker_color='rgba(139, 92, 246, 0.8)',
                            text=[f"{r['count']} ({r['percentage']}%)" for r in race_data],
                            textposition='outside'
                        )
                    ])
                    fig.update_layout(
                        title='Race Distribution',
                        xaxis_title='Patient Count',
                        yaxis_title='Race',
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Race data not available")
            
            with col2:
                # Gender
                if demographics.get('gender') and len(demographics['gender']) > 0:
                    gender_data = demographics['gender']
                    fig = go.Figure(data=[
                        go.Pie(
                            labels=[g['value'] for g in gender_data],
                            values=[g['count'] for g in gender_data],
                            hole=0.4,
                            marker_colors=['rgba(59, 130, 246, 0.8)', 'rgba(236, 72, 153, 0.8)']
                        )
                    ])
                    fig.update_layout(
                        title='Gender Distribution',
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Gender data not available")
                
                # Ethnicity
                if demographics.get('ethnicity') and len(demographics['ethnicity']) > 0:
                    ethnicity_data = demographics['ethnicity']
                    fig = go.Figure(data=[
                        go.Pie(
                            labels=[e['value'] for e in ethnicity_data],
                            values=[e['count'] for e in ethnicity_data],
                            hole=0.4,
                            marker_colors=['rgba(16, 185, 129, 0.8)', 'rgba(245, 158, 11, 0.8)']
                        )
                    ])
                    fig.update_layout(
                        title='Ethnicity Distribution',
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Ethnicity data not available")
            
            # Site Characteristics
            st.subheader("🏥 Site Characteristics")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if sites.get('teaching_status') and len(sites['teaching_status']) > 0:
                    teaching_data = sites['teaching_status']
                    fig = go.Figure(data=[
                        go.Pie(
                            labels=[t['value'] for t in teaching_data],
                            values=[t['patient_count'] for t in teaching_data],
                            hole=0.4,
                            marker_colors=['rgba(59, 130, 246, 0.8)', 'rgba(156, 163, 175, 0.8)']
                        )
                    ])
                    fig.update_layout(title='Teaching Status', height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Teaching status data not available")
            
            with col2:
                if sites.get('urban_rural') and len(sites['urban_rural']) > 0:
                    urban_rural_data = sites['urban_rural']
                    fig = go.Figure(data=[
                        go.Pie(
                            labels=[u['value'] for u in urban_rural_data],
                            values=[u['patient_count'] for u in urban_rural_data],
                            hole=0.4,
                            marker_colors=['rgba(16, 185, 129, 0.8)', 'rgba(234, 179, 8, 0.8)']
                        )
                    ])
                    fig.update_layout(title='Urban vs Rural', height=300)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Urban/Rural data not available")
            
            with col3:
                if sites.get('bed_count') and len(sites['bed_count']) > 0:
                    bed_count_data = sites['bed_count']
                    fig = go.Figure(data=[
                        go.Bar(
                            x=[b['value'] for b in bed_count_data],
                            y=[b['patient_count'] for b in bed_count_data],
                            marker_color='rgba(139, 92, 246, 0.8)',
                            text=[f"{b['patient_count']} ({b['percentage']}%)" for b in bed_count_data],
                            textposition='outside'
                        )
                    ])
                    fig.update_layout(
                        title='Bed Count Groups',
                        xaxis_title='Bed Count Group',
                        yaxis_title='Patient Count',
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Bed count data not available")
            
            # Trends
            if trends and len(trends) > 0:
                st.subheader("📈 Admission Trends Over Time")
                fig = go.Figure(data=[
                    go.Scatter(
                        x=[t['week_start'] for t in trends],
                        y=[t['admission_count'] for t in trends],
                        mode='lines+markers',
                        line=dict(color='rgb(16, 185, 129)', width=3),
                        marker=dict(size=8, color='rgb(16, 185, 129)'),
                        fill='tozeroy',
                        fillcolor='rgba(16, 185, 129, 0.2)'
                    )
                ])
                fig.update_layout(
                    title='Admission Trends',
                    xaxis_title='Week',
                    yaxis_title='Admission Count',
                    height=300
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Trends data not available")
            
            # Outcomes
            if outcomes:
                st.subheader("📊 Key Outcomes")
                outcome_labels = []
                outcome_values = []
                outcome_colors = []
                
                if outcomes.get('avg_los'):
                    outcome_labels.append('Avg LOS (days)')
                    outcome_values.append(outcomes['avg_los'])
                    outcome_colors.append('rgba(139, 92, 246, 0.8)')
                if outcomes.get('readmission_rate'):
                    outcome_labels.append('Readmission Rate (%)')
                    outcome_values.append(outcomes['readmission_rate'])
                    outcome_colors.append('rgba(239, 68, 68, 0.8)')
                if outcomes.get('mortality_rate'):
                    outcome_labels.append('Mortality Rate (%)')
                    outcome_values.append(outcomes['mortality_rate'])
                    outcome_colors.append('rgba(107, 114, 128, 0.8)')
                if outcomes.get('complication_rate'):
                    outcome_labels.append('Complication Rate (%)')
                    outcome_values.append(outcomes['complication_rate'])
                    outcome_colors.append('rgba(245, 158, 11, 0.8)')
                
                if outcome_labels:
                    fig = go.Figure(data=[
                        go.Bar(
                            x=outcome_labels,
                            y=outcome_values,
                            marker_color=outcome_colors,
                            text=[f"{v:.1f}" for v in outcome_values],
                            textposition='outside'
                        )
                    ])
                    fig.update_layout(
                        title='Outcome Metrics',
                        xaxis_title='Outcome Metric',
                        yaxis_title='Value',
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error generating insights: {str(e)}")
            logger.error(f"Insights generation error: {str(e)}", exc_info=True)


def main():
    """Main application"""
    # Auto-initialize from .env file if available and not already initialized
    if not st.session_state.services_initialized:
        # Check if .env file has values
        from config import config as db_config
        
        # Debug: Check if .env file exists and show values (for troubleshooting)
        env_file_exists = os.path.exists('.env')
        logger.info(f".env file exists: {env_file_exists}")
        logger.info(f"Config values - host: {bool(db_config.host)}, token: {bool(db_config.token)}, space_id: {bool(db_config.space_id)}, catalog: {db_config.patient_catalog}, schema: {db_config.patient_schema}, warehouse: {bool(db_config.warehouse_id)}")
        
        # Check all required values
        has_host = bool(db_config.host)
        has_token = bool(db_config.token)
        has_space_id = bool(db_config.space_id)
        has_catalog = bool(db_config.patient_catalog)
        has_schema = bool(db_config.patient_schema)
        has_warehouse = bool(db_config.warehouse_id)
        
        if has_host and has_token and has_space_id and has_catalog and has_schema and has_warehouse:
            # Try to auto-initialize from .env
            try:
                logger.info("Attempting auto-initialization from .env file")
                if initialize_services():
                    # Success - services initialized, rerun to refresh UI
                    logger.info("Auto-initialization successful, rerunning...")
                    st.rerun()
                else:
                    logger.warning("Auto-initialization returned False - check logs for details")
            except Exception as e:
                # If auto-init fails, user will see config page
                logger.error(f"Auto-initialization failed: {e}", exc_info=True)
                st.error(f"Auto-initialization failed: {str(e)}")
        else:
            # Show which values are missing for debugging
            missing = []
            if not has_host: missing.append('DATABRICKS_HOST')
            if not has_token: missing.append('DATABRICKS_TOKEN')
            if not has_space_id: missing.append('GENIE_SPACE_ID')
            if not has_catalog: missing.append('PATIENT_CATALOG')
            if not has_schema: missing.append('PATIENT_SCHEMA')
            if not has_warehouse: missing.append('SQL_WAREHOUSE_ID')
            logger.info(f"Missing required config values: {missing}, showing config page")
    
    # Sidebar navigation
    with st.sidebar:
        st.title("🏥 Clinical Cohort Assistant")
        page = st.radio(
            "Navigation",
            ["Chat", "Configuration"],
            label_visibility="collapsed"
        )
    
    # Route to appropriate page
    if page == "Configuration":
        render_config_page()
    else:
        render_chat_page()


if __name__ == "__main__":
    main()

