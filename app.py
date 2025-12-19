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
if 'genie_result' not in st.session_state:
    st.session_state.genie_result = None
if 'genie_error' not in st.session_state:
    st.session_state.genie_error = None
if 'genie_running' not in st.session_state:
    st.session_state.genie_running = False
if 'cohort_table_info' not in st.session_state:
    st.session_state.cohort_table_info = None
if 'cohort_table_creating' not in st.session_state:
    st.session_state.cohort_table_creating = False
if 'cohort_table_error' not in st.session_state:
    st.session_state.cohort_table_error = None
if 'dimension_results' not in st.session_state:
    st.session_state.dimension_results = None
if 'dimension_analyzing' not in st.session_state:
    st.session_state.dimension_analyzing = False


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
        from services.dimension_analysis import DimensionAnalysisService
        
        st.session_state.vector_service = VectorSearchService()
        st.session_state.genie_service = GenieService()
        st.session_state.cohort_manager = CohortManager()
        st.session_state.intent_service = IntentService()
        st.session_state.dimension_service = DimensionAnalysisService()
        
        # Pre-discover schema for faster dimension analysis (cache it)
        try:
            from services.schema_discovery import SchemaDiscoveryService
            schema_service = SchemaDiscoveryService()
            # Cache schema context upfront (non-blocking, happens in background)
            schema_context = schema_service.get_schema_context_for_llm(
                config.patient_catalog,
                config.patient_schema
            )
            logger.info(f"Schema context cached ({len(schema_context)} chars)")
        except Exception as e:
            logger.warning(f"Could not pre-discover schema: {str(e)}")
            # Non-critical, dimension analysis will discover schema on-demand
        
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


def run_genie_health_check():
    """Check if Genie service is active and accessible."""
    try:
        # Reload config to pick up any values saved via the UI/config form
        import importlib
        import config as config_module
        importlib.reload(config_module)
        db_config = config_module.config
        
        # Ensure required configuration is present
        missing = []
        if not db_config.host:
            missing.append("DATABRICKS_HOST")
        if not db_config.token:
            missing.append("DATABRICKS_TOKEN")
        if not db_config.space_id:
            missing.append("GENIE_SPACE_ID")
        
        if missing:
            return False, f"Missing required configuration: {', '.join(missing)}"
        
        # Create a temporary GenieService instance to check health
        from services.genie_service import GenieService
        genie_service = GenieService()
        
        # Run the health check
        is_healthy, message = genie_service.check_genie_health()
        return is_healthy, message
    except Exception as e:
        logger.error(f"Genie health check failed: {e}", exc_info=True)
        return False, str(e)


def refine_criteria_with_codes():
    """Build a refined, natural-language criteria description using selected codes."""
    analysis = st.session_state.get("criteria_analysis") or {}
    selected_codes = st.session_state.get("selected_codes") or []
    original_text = st.session_state.get("criteria_text") or ""

    if not selected_codes or len(selected_codes) == 0:
        error_msg = (
            f"No codes selected to refine the criteria. "
            f"Session state has {len(st.session_state.get('selected_codes', []))} codes. "
            f"Please select codes using 'Use all suggested codes' or choose specific codes."
        )
        st.warning(error_msg)
        logger.warning(error_msg)
        return ""

    summary = analysis.get("summary") or original_text
    # Prefer the set of conditions that actually have selected codes, so that
    # the refined criteria text matches what will be used downstream.
    conditions_from_codes = sorted(
        {c.get("condition") for c in selected_codes if c.get("condition")}
    )
    if conditions_from_codes:
        conditions = conditions_from_codes
    else:
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


def create_cohort_table_from_genie_sql():
    """Create cohort temp table directly from Genie SQL (efficient - uses full query, not sample data)"""
    genie_result = st.session_state.get("genie_result")
    if not genie_result:
        st.session_state.cohort_table_error = "No Genie result available"
        return
    
    sql = genie_result.get("sql")
    if not sql:
        st.session_state.cohort_table_error = "No SQL available from Genie result"
        return
    
    if not hasattr(st.session_state, "dimension_service") or st.session_state.dimension_service is None:
        st.session_state.cohort_table_error = "Dimension analysis service is not initialized"
        return
    
    try:
        st.session_state.cohort_table_creating = True
        st.session_state.cohort_table_error = None
        
        logger.info("Creating cohort table from Genie SQL (using full query, not sample data)")
        
        # Create cohort table directly from SQL
        session_id = st.session_state.get("session_id", "default")
        table_info = st.session_state.dimension_service.create_cohort_table_from_sql(
            session_id, 
            sql
        )
        
        st.session_state.cohort_table_info = table_info
        st.session_state.cohort_table_creating = False
        logger.info(f"Cohort table created successfully: {table_info['cohort_table']} with {table_info['count']} rows")
        
    except Exception as e:
        logger.error(f"Error creating cohort table: {e}", exc_info=True)
        st.session_state.cohort_table_error = f"Error creating cohort table: {str(e)}"
        st.session_state.cohort_table_creating = False


def run_genie_for_refined_criteria():
    """Call Genie with the refined criteria and selected codes, and store the result."""
    refined = st.session_state.get("refined_criteria") or {}
    refined_text = st.session_state.get("refined_criteria_text") or ""
    selected_codes = st.session_state.get("selected_codes") or []

    if not refined_text or not selected_codes:
        st.session_state.genie_error = "I need both a refined criteria and at least one selected code before calling Genie."
        return

    if not hasattr(st.session_state, "genie_service") or st.session_state.genie_service is None:
        st.session_state.genie_error = "Genie service is not initialized. Please check configuration."
        return

    # Build the criteria dict expected by GenieService._build_nl_query
    unique_codes = sorted({c.get("code") for c in selected_codes if c.get("code")})
    code_details = []
    vocabularies = set()
    for c in selected_codes:
        code = c.get("code")
        if not code:
            continue
        desc = c.get("description") or ""
        vocab = c.get("vocabulary")
        if vocab:
            vocabularies.add(vocab)
        code_details.append(
            {
                "code": code,
                "description": desc,
                "vocabulary": vocab,
            }
        )

    genie_criteria = {
        "codes": unique_codes,
        "code_details": code_details,
        "vocabularies": sorted(vocabularies),
        # We use the refined natural-language criteria as the primary description.
        "original_query": refined_text,
        "timeframe": refined.get("timeframe"),
        "age": None,
    }

    genie = st.session_state.genie_service

    # Clear any previous error
    st.session_state.genie_error = None

    try:
        # This will poll Genie until completion (can take up to 5 minutes)
        result = genie.create_cohort_query(genie_criteria)
        st.session_state.genie_result = result
        st.session_state.genie_conversation_id = result.get("conversation_id")
        st.session_state.genie_error = None
        st.session_state.genie_running = False  # Ensure spinner stops
        logger.info(f"Genie completed successfully. SQL: {bool(result.get('sql'))}, Row count: {result.get('row_count', 0)}, Data rows: {len(result.get('data', []))}")
    except Exception as e:
        error_msg = f"Error while calling Genie: {e}"
        st.session_state.genie_error = error_msg
        st.session_state.genie_running = False  # Ensure spinner stops even on error
        logger.error(f"Genie error: {e}", exc_info=True)


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

    codes = []

    if conditions:
        # If there are multiple conditions (e.g., diabetes AND cancer), call the
        # vector lookup separately for each condition phrase so that each one
        # gets a clean, precise search term.
        search_terms = [c.strip() for c in conditions if c and c.strip()]
        st.session_state.code_search_text = "; ".join(search_terms)

        for term in search_terms:
            try:
                with st.spinner(f"Looking up codes for: {term}"):
                    term_codes = st.session_state.vector_service.search_codes(term, limit=10)
            except Exception as e:
                logger.error(f"Code search error for '{term}': {e}", exc_info=True)
                continue

            for c in term_codes:
                # Tag each code with the condition phrase it came from so we
                # can show this context in the UI.
                c = dict(c)
                c["condition"] = term
                codes.append(c)
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

    if not codes:
        st.session_state.code_search_error = (
            "I couldn't find any standard codes from this description. "
            "You may want to specify a more precise condition or drug name in your criteria."
        )
        st.session_state.codes = []
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

    st.markdown("### Connection Health Checks")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.caption("Test SQL Warehouse connectivity")
        if st.button("Test Databricks connection", use_container_width=True):
            with st.spinner("Running health check..."):
                ok, info = run_databricks_health_check()
            if ok:
                st.success(f"✅ Databricks SQL Warehouse is reachable (SELECT 1 returned {info}).")
            else:
                st.error(f"❌ Databricks health check failed: {info}")
    
    with col2:
        st.caption("Test Genie service availability")
        if st.button("Test Genie connection", use_container_width=True):
            with st.spinner("Checking Genie service..."):
                ok, info = run_genie_health_check()
            if ok:
                st.success(f"✅ Genie service is active: {info}")
            else:
                st.error(f"❌ Genie health check failed: {info}")


def render_chat_page():
    """Main chat interface"""
    # Compact header
    col_header1, col_header2 = st.columns([3, 1])
    with col_header1:
        st.title("🏥 Clinical Cohort Assistant")
    with col_header2:
        st.caption("Build patient cohorts using natural language queries")
    
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
    # Only expand if no analysis exists yet
    has_analysis = st.session_state.get("criteria_analysis") is not None
    with st.expander("🧩 Step 1: Enter & Analyze Clinical Criteria", expanded=not has_analysis):
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

    # Step 2: Code Selection - Only show if we have codes or errors
    has_codes = len(st.session_state.get("codes", [])) > 0
    has_code_error = bool(st.session_state.code_search_error)
    
    if has_codes or has_code_error or st.session_state.code_search_text:
        # Only expand if we're actively working on code selection
        has_selected = len(st.session_state.get("selected_codes", [])) > 0
        with st.expander("🔍 Step 2: Select Codes", expanded=(has_codes and not has_selected)):
            if st.session_state.code_search_text:
                st.caption(f"**Searched for:** {st.session_state.code_search_text}")
            
            if st.session_state.code_search_error:
                st.error(st.session_state.code_search_error)
            elif st.session_state.codes:
                codes = st.session_state.codes
                
                # Group codes by condition phrase
                grouped: dict[str, list[dict]] = {}
                for c in codes:
                    cond = c.get("condition") or "Unspecified condition"
                    grouped.setdefault(cond, []).append(c)

                # Compact summary
                st.caption(f"Found {len(codes)} code(s) across {len(grouped)} condition(s)")
                for cond, cond_codes in grouped.items():
                    st.caption(f"• **{cond}**: {len(cond_codes)} code(s)")

                overall_choice = st.radio(
                    "Code selection method",
                    ["Use all suggested codes (recommended)", "Customize codes per condition"],
                    index=0,
                    horizontal=True,
                    key="code_selection_choice",
                    label_visibility="collapsed",
                )

                selected_codes: list[dict] = []

                if overall_choice.startswith("Use all"):
                    if not grouped:
                        st.warning("No codes were found to select.")
                    else:
                        for cond_codes in grouped.values():
                            if cond_codes:
                                selected_codes.extend(cond_codes)
                        if selected_codes:
                            st.session_state.selected_codes = selected_codes
                            logger.info(f"Selected {len(selected_codes)} codes via 'Use all'")
                else:
                    # Customize mode - show expanders for each condition
                    for idx, (cond, cond_codes) in enumerate(grouped.items()):
                        with st.expander(f"📋 {cond} ({len(cond_codes)} codes)", expanded=(len(grouped) == 1)):
                            code_df = pd.DataFrame(cond_codes)
                            display_cols = ['code', 'description', 'vocabulary']
                            available_cols = [col for col in display_cols if col in code_df.columns]
                            st.dataframe(code_df[available_cols], use_container_width=True, hide_index=True)

                            label_to_code = {
                                f"{c.get('code')} – {c.get('description')} ({c.get('vocabulary')})": c
                                for c in cond_codes
                            }
                            options = list(label_to_code.keys())

                            selected_labels = st.multiselect(
                                f"Select codes for {cond}:",
                                options=options,
                                key=f"codes_select_{idx}",
                            )

                            for label in selected_labels:
                                selected_codes.append(label_to_code[label])

                # Always update session state
                st.session_state.selected_codes = selected_codes
                final_selected = st.session_state.get("selected_codes") or selected_codes
                
                if final_selected and len(final_selected) > 0:
                    st.success(f"✅ {len(final_selected)} code(s) selected")
                    if st.button("➡️ Refine Criteria with Selected Codes", use_container_width=True, type="primary"):
                        refined_text = refine_criteria_with_codes()
                        if refined_text:
                            st.rerun()
                elif not codes or len(codes) == 0:
                    st.warning("No codes were found from the vector search.")
                else:
                    st.warning("Please select codes above to continue.")

    # Step 3: Refined Criteria & Genie
    refined_text = st.session_state.get("refined_criteria_text")
    has_genie_result = st.session_state.get("genie_result") is not None
    has_genie_running = st.session_state.get("genie_running", False)
    
    if refined_text:
        with st.expander("✨ Step 3: Refined Criteria & Query", expanded=(not has_genie_result)):
            st.markdown("**Refined criteria:**")
            st.write(refined_text)
            
            if st.session_state.get("genie_running"):
                # Actually call Genie now (this will poll and take time)
                with st.spinner("⏳ Genie is processing... This may take up to 5 minutes. Please wait..."):
                    run_genie_for_refined_criteria()
                    st.session_state.genie_running = False
                    st.rerun()
            elif st.session_state.get("genie_error"):
                st.error(f"❌ {st.session_state.genie_error}")
                st.session_state.genie_running = False
            elif not st.session_state.get("genie_result"):
                if st.button("🚀 Ask Genie to Find Patients", use_container_width=True, type="primary"):
                    st.session_state.genie_running = True
                    st.rerun()

    # Step 4: Genie Results
    genie_result = st.session_state.get("genie_result")
    if genie_result:
        sql = genie_result.get("sql")
        data = genie_result.get("data", [])
        row_count = genie_result.get("row_count", 0)
        exec_time = genie_result.get("execution_time")

        # Show SQL outside the results expander to avoid nesting
        if sql:
            with st.expander("📝 Generated SQL", expanded=False):
                st.code(sql, language="sql")
        
        with st.expander("📊 Step 4: Query Results", expanded=True):

            # Display row count with clear messaging
            if row_count is not None and row_count > 0:
                if data and len(data) > 0:
                    if len(data) < row_count:
                        st.info(
                            f"📊 **Total Results:** {row_count:,} row(s) | "
                            f"**Displayed:** {len(data):,} row(s) "
                            f"(showing up to {min(len(data), 5000):,} rows in table below)"
                        )
                    else:
                        st.info(
                            f"📊 **Results:** {row_count:,} row(s) "
                            f"(showing up to {min(len(data), 5000):,} rows in table below)"
                        )
                else:
                    st.info(f"📊 **Total Results:** {row_count:,} row(s) (data array not available - may be truncated)")
            elif data and len(data) > 0:
                st.info(f"📊 **Results:** {len(data):,} row(s) (showing up to {min(len(data), 5000):,} rows in table below)")
            elif row_count == 0:
                st.info("📊 **Results:** 0 row(s) - No patients match this criteria")

            # Display execution time
            if exec_time is not None:
                st.caption(f"⏱️ Query execution time: {exec_time}")

            # Display the actual data if available (limit to 5000 rows max)
            MAX_DISPLAY_ROWS = 5000
            if data and len(data) > 0:
                try:
                    # Convert data to DataFrame for better display
                    import pandas as pd
                    # Get column names from genie_result if available
                    columns = genie_result.get("columns")
                    logger.info(f"🔍 DEBUG: columns from genie_result: {columns}, type: {type(columns)}, length: {len(columns) if columns else 0}")
                    logger.info(f"🔍 DEBUG: genie_result keys: {list(genie_result.keys())}")
                    
                    # Check data structure
                    if data and len(data) > 0:
                        first_row = data[0]
                        is_list_of_dicts = isinstance(first_row, dict)
                        is_list_of_lists = isinstance(first_row, (list, tuple))
                        logger.info(f"🔍 DEBUG: first_row type: {type(first_row)}, is_list_of_dicts: {is_list_of_dicts}, is_list_of_lists: {is_list_of_lists}")
                        
                        if columns and len(columns) > 0:
                            logger.info(f"✅ Using extracted columns: {columns} (count: {len(columns)})")
                            logger.info(f"🔍 DEBUG: Data sample - first row length: {len(first_row) if hasattr(first_row, '__len__') else 'N/A'}, first row: {str(first_row)[:100]}")
                            
                            # Use provided column names
                            if is_list_of_lists:
                                # Data is list of lists, use columns parameter
                                logger.info(f"🔍 DEBUG: Creating DataFrame from list of lists with {len(columns)} column names")
                                logger.info(f"🔍 DEBUG: First row has {len(first_row)} values, columns list has {len(columns)} names")
                                if len(first_row) != len(columns):
                                    logger.warning(f"⚠️ MISMATCH: First row has {len(first_row)} values but we have {len(columns)} column names!")
                                try:
                                    df = pd.DataFrame(data, columns=columns)
                                    logger.info(f"✅ DataFrame created successfully with columns parameter")
                                except Exception as e:
                                    logger.error(f"❌ Error creating DataFrame with columns: {e}")
                                    # Fallback: create without columns
                                    df = pd.DataFrame(data)
                                    logger.warning(f"⚠️ Fallback: Created DataFrame without column names")
                            elif is_list_of_dicts:
                                # Data is list of dicts, but we have column names - use them to reorder/select
                                logger.info(f"🔍 DEBUG: Creating DataFrame from list of dicts")
                                df = pd.DataFrame(data)
                                logger.info(f"🔍 DEBUG: DataFrame from dicts has columns: {list(df.columns)}")
                                # If column names match dict keys, reorder; otherwise use provided columns
                                if set(columns).issubset(set(df.columns)):
                                    logger.info(f"✅ Column names match dict keys, reordering...")
                                    df = df[columns]
                                else:
                                    logger.warning(f"⚠️ Column names don't match dict keys. Dict keys: {list(df.columns)}, Provided columns: {columns}")
                                    # Try to create with provided columns (may fail if mismatch)
                                    try:
                                        df = pd.DataFrame(data, columns=columns)
                                        logger.info(f"✅ Created DataFrame with provided columns despite mismatch")
                                    except Exception as e:
                                        logger.error(f"❌ Error: {e}, keeping original DataFrame")
                            else:
                                logger.info(f"🔍 DEBUG: Unknown data structure, trying to create with columns anyway")
                                try:
                                    df = pd.DataFrame(data, columns=columns)
                                except Exception as e:
                                    logger.error(f"❌ Error: {e}")
                                    df = pd.DataFrame(data)
                            
                            logger.info(f"✅ Created DataFrame with {len(columns)} columns: {columns[:5]}..." if len(columns) > 5 else f"✅ Created DataFrame with columns: {columns}")
                            logger.info(f"🔍 DEBUG: DataFrame.columns after creation: {list(df.columns)}")
                            logger.info(f"🔍 DEBUG: DataFrame.shape: {df.shape}")
                            logger.info(f"🔍 DEBUG: Column names match? Expected: {columns}, Got: {list(df.columns)}")
                        else:
                            # No column names provided - try to infer from data structure
                            if is_list_of_dicts:
                                # List of dicts - pandas will use dict keys as column names
                                df = pd.DataFrame(data)
                                logger.info(f"Created DataFrame from list of dicts with columns: {list(df.columns)}")
                            elif is_list_of_lists:
                                # List of lists - will get numeric indices, which is the problem
                                df = pd.DataFrame(data)
                                logger.warning(f"DataFrame created with numeric column indices (0, 1, 2...). Column names not available from Genie. Data shape: {df.shape}")
                                st.warning("⚠️ Column names not available. Showing numeric indices. This may indicate an issue with Genie response parsing.")
                            else:
                                df = pd.DataFrame(data)
                                logger.warning(f"Unknown data structure type: {type(first_row)}. Created DataFrame with columns: {list(df.columns)}")
                    
                    # Limit to max 5000 rows for display
                    total_rows = len(df)
                    display_df = df.head(MAX_DISPLAY_ROWS)
                    
                    # Log before display to verify columns are still correct
                    logger.info(f"🔍 DEBUG: display_df.columns before st.dataframe: {list(display_df.columns)}")
                    logger.info(f"🔍 DEBUG: Are columns still named (not numeric)? {all(not str(col).isdigit() for col in display_df.columns) if len(display_df.columns) > 0 else 'N/A'}")
                    
                    # Show info about row limits
                    if total_rows > MAX_DISPLAY_ROWS:
                        st.info(
                            f"📊 Showing first {MAX_DISPLAY_ROWS:,} of {total_rows:,} total rows. "
                            f"Data is limited to {MAX_DISPLAY_ROWS:,} rows for performance. "
                            f"Use the generated SQL to query the full dataset if needed."
                        )
                    elif row_count and row_count > total_rows:
                        # Genie reported more rows than we have in data (truncated)
                        st.info(
                            f"📊 Showing {total_rows:,} rows. Genie reported {row_count:,} total rows. "
                            f"Data may be truncated. Use the generated SQL to query the full dataset."
                        )
                    
                    # Display the data (already limited to MAX_DISPLAY_ROWS)
                    # Note: st.dataframe should preserve column names, but let's verify
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                    
                    # Log after display (though this won't help if st.dataframe modifies it)
                    logger.info(f"🔍 DEBUG: DataFrame columns after st.dataframe call (should be unchanged): {list(display_df.columns)}")
                    
                    # Show summary statistics if numeric columns exist
                    numeric_cols = display_df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        st.markdown("**📈 Summary Statistics**")
                        st.dataframe(display_df[numeric_cols].describe(), use_container_width=True)
                except Exception as e:
                    # If DataFrame conversion fails, show raw data (but still limit)
                    logger.warning(f"Could not convert Genie data to DataFrame: {e}")
                    limited_data = data[:MAX_DISPLAY_ROWS] if len(data) > MAX_DISPLAY_ROWS else data
                    if len(data) > MAX_DISPLAY_ROWS:
                        st.info(f"Showing first {MAX_DISPLAY_ROWS:,} of {len(data):,} rows.")
                    st.json(limited_data)
            elif row_count and row_count > 0:
                # We have a row count but no data array (data might be truncated or not extracted)
                st.warning(
                    f"Genie reported {row_count:,} row(s) were returned, but the data array is not available. "
                    f"This may be because:\n"
                    f"- The result set is very large and was truncated\n"
                    f"- Data extraction encountered an issue\n\n"
                    f"You can use the generated SQL above to query the full dataset directly."
                )
            
            # Dimension Analysis (cohort table created automatically in background)
            st.markdown("---")
            st.markdown("### 📊 Dimension Analysis")
            
            cohort_table_info = st.session_state.get("cohort_table_info")
            dimension_results = st.session_state.get("dimension_results")
            dimension_analyzing = st.session_state.get("dimension_analyzing", False)
            
            if cohort_table_info:
                # Table already created, ready for dimension analysis
                if dimension_results:
                    # Show dimension analysis results
                    display_dimension_results(dimension_results)
                elif dimension_analyzing:
                    with st.spinner("Analyzing cohort dimensions... This may take a minute."):
                        # This will be handled by the button click handler
                        pass
                else:
                    st.success(f"✅ Ready for dimension analysis ({cohort_table_info['count']:,} patients)")
                    if st.button("📊 Analyze Cohort Dimensions", use_container_width=True, type="primary"):
                        st.session_state.dimension_analyzing = True
                        st.rerun()
            elif st.session_state.get("cohort_table_creating"):
                # Creating table in background - user doesn't need to know details
                with st.spinner("Preparing dimension analysis..."):
                    create_cohort_table_from_genie_sql()
                    st.rerun()
            elif st.session_state.get("cohort_table_error"):
                st.error(f"❌ {st.session_state.cohort_table_error}")
                if st.button("🔄 Retry", use_container_width=True):
                    st.session_state.cohort_table_error = None
                    st.session_state.cohort_table_creating = True
                    st.rerun()
            else:
                # Auto-create table when user clicks to analyze dimensions
                if st.button("📊 Analyze Cohort Dimensions", use_container_width=True, type="primary"):
                    st.session_state.cohort_table_creating = True
                    st.rerun()
            
            # Handle dimension analysis execution
            if dimension_analyzing and cohort_table_info and not dimension_results:
                try:
                    cohort_table = cohort_table_info.get('cohort_table')
                    has_medrec = cohort_table_info.get('has_medrec_key', False)
                    
                    if cohort_table and hasattr(st.session_state, 'dimension_service'):
                        with st.spinner("Discovering schema and generating dimension queries in parallel..."):
                            # Use dynamic mode: schema discovery + LLM-generated SQL (parallel)
                            results = st.session_state.dimension_service.analyze_dimensions(
                                cohort_table=cohort_table,
                                has_medrec_key=has_medrec,
                                use_dynamic=True  # Enable dynamic schema-based generation
                            )
                            st.session_state.dimension_results = results
                            st.session_state.dimension_analyzing = False
                            st.rerun()
                    else:
                        st.error("Cannot analyze dimensions: cohort table information missing")
                        st.session_state.dimension_analyzing = False
                except Exception as e:
                    st.error(f"Error analyzing dimensions: {str(e)}")
                    logger.error(f"Dimension analysis error: {str(e)}", exc_info=True)
                    st.session_state.dimension_analyzing = False


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


def display_dimension_results(results: dict):
    """
    Display dimension analysis results with visualizations
    
    Args:
        results: Dictionary with 'dimensions' (dict of dimension results) and 'errors' (dict of errors)
    """
    dimensions = results.get('dimensions', {})
    errors = results.get('errors', {})
    generated_queries = results.get('generated_queries', {})
    validation_results = results.get('validation_results', {})
    
    # Show SQL validation summary
    if validation_results:
        valid_count = sum(1 for v in validation_results.values() if v.get('is_valid', False))
        total_count = len(validation_results)
        if valid_count == total_count:
            st.success(f"✅ All {total_count} dimension SQL queries validated successfully")
        else:
            st.warning(f"⚠️ SQL Validation: {valid_count}/{total_count} queries passed validation")
    
    # Show generated SQL queries (collapsible)
    if generated_queries:
        with st.expander("🔍 View Generated SQL Queries", expanded=False):
            for dim_name, sql in generated_queries.items():
                validation = validation_results.get(dim_name, {})
                is_valid = validation.get('is_valid', False)
                
                st.markdown(f"### {dim_name}")
                if not is_valid:
                    st.error("❌ Validation Failed")
                    warnings = validation.get('warnings', [])
                    for warning in warnings:
                        st.warning(warning)
                else:
                    st.success("✅ Validated")
                
                st.code(sql, language='sql')
                st.markdown("---")
    
    if errors:
        st.warning(f"⚠️ Some dimensions failed to load: {', '.join(errors.keys())}")
        with st.expander("View Errors", expanded=False):
            for dim_name, error_msg in errors.items():
                st.error(f"**{dim_name}**: {error_msg}")
                # Show SQL if available
                if dim_name in generated_queries:
                    st.code(generated_queries[dim_name], language='sql')
    
    if not dimensions or all(not v for v in dimensions.values()):
        st.info("No dimension data available")
        return
    
    # Patient-Level Demographics Section (left to right)
    st.subheader("👥 Patient Demographics")
    demo_col1, demo_col2, demo_col3, demo_col4 = st.columns(4)
    
    with demo_col1:
        # Age Groups
        if dimensions.get('age_groups'):
            age_df = pd.DataFrame(dimensions['age_groups'])
            if not age_df.empty and 'age_group' in age_df.columns and 'patient_count' in age_df.columns:
                fig = px.bar(
                    age_df, 
                    x='age_group', 
                    y='patient_count',
                    title='Age Distribution',
                    labels={'patient_count': 'Count', 'age_group': 'Age Group'},
                    color='patient_count',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=250, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Age Data", expanded=False):
                    st.dataframe(age_df, use_container_width=True, hide_index=True)
            elif not age_df.empty:
                st.warning(f"Missing columns: {list(age_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(age_df, use_container_width=True, hide_index=True)
    
    with demo_col2:
        # Gender
        if dimensions.get('gender'):
            gender_df = pd.DataFrame(dimensions['gender'])
            if not gender_df.empty and 'gender' in gender_df.columns and 'patient_count' in gender_df.columns:
                fig = go.Figure(data=[
                    go.Pie(
                        labels=gender_df['gender'],
                        values=gender_df['patient_count'],
                        hole=0.4,
                        marker_colors=['rgba(59, 130, 246, 0.8)', 'rgba(236, 72, 153, 0.8)', 'rgba(156, 163, 175, 0.8)']
                    )
                ])
                fig.update_layout(title='Gender', height=250, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Gender Data", expanded=False):
                    st.dataframe(gender_df, use_container_width=True, hide_index=True)
            elif not gender_df.empty:
                st.warning(f"Missing columns: {list(gender_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(gender_df, use_container_width=True, hide_index=True)
    
    with demo_col3:
        # Race
        if dimensions.get('race'):
            race_df = pd.DataFrame(dimensions['race'])
            if not race_df.empty and 'race' in race_df.columns and 'patient_count' in race_df.columns:
                fig = px.bar(
                    race_df.head(8),  # Top 8 races
                    x='race',
                    y='patient_count',
                    title='Race (Top 8)',
                    labels={'patient_count': 'Count', 'race': 'Race'},
                    color='patient_count',
                    color_continuous_scale='Greens'
                )
                fig.update_layout(height=250, showlegend=False, xaxis_tickangle=-45, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Race Data", expanded=False):
                    st.dataframe(race_df, use_container_width=True, hide_index=True)
            elif not race_df.empty:
                st.warning(f"Missing columns: {list(race_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(race_df, use_container_width=True, hide_index=True)
    
    with demo_col4:
        # Ethnicity
        if dimensions.get('ethnicity'):
            ethnicity_df = pd.DataFrame(dimensions['ethnicity'])
            if not ethnicity_df.empty and 'ethnicity' in ethnicity_df.columns and 'patient_count' in ethnicity_df.columns:
                fig = go.Figure(data=[
                    go.Pie(
                        labels=ethnicity_df['ethnicity'],
                        values=ethnicity_df['patient_count'],
                        hole=0.4,
                        marker_colors=['rgba(16, 185, 129, 0.8)', 'rgba(245, 158, 11, 0.8)', 'rgba(156, 163, 175, 0.8)']
                    )
                ])
                fig.update_layout(title='Ethnicity', height=250, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Ethnicity Data", expanded=False):
                    st.dataframe(ethnicity_df, use_container_width=True, hide_index=True)
            elif not ethnicity_df.empty:
                st.warning(f"Missing columns: {list(ethnicity_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(ethnicity_df, use_container_width=True, hide_index=True)
    
    # Visit Characteristics Section (left to right)
    st.subheader("🏥 Visit Characteristics")
    visit_col1, visit_col2, visit_col3 = st.columns(3)
    
    with visit_col1:
        # Visit Level
        if dimensions.get('visit_level'):
            visit_df = pd.DataFrame(dimensions['visit_level'])
            if not visit_df.empty and 'visit_level' in visit_df.columns and 'encounter_count' in visit_df.columns:
                fig = px.bar(
                    visit_df,
                    x='visit_level',
                    y='encounter_count',
                    title='Visit Level',
                    labels={'encounter_count': 'Count', 'visit_level': 'Visit Level'},
                    color='encounter_count',
                    color_continuous_scale='Purples'
                )
                fig.update_layout(height=250, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Visit Level Data", expanded=False):
                    st.dataframe(visit_df, use_container_width=True, hide_index=True)
            elif not visit_df.empty:
                st.warning(f"Missing columns: {list(visit_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(visit_df, use_container_width=True, hide_index=True)
    
    with visit_col2:
        # Admit Source
        if dimensions.get('admit_source'):
            admit_source_df = pd.DataFrame(dimensions['admit_source'])
            if not admit_source_df.empty and 'admit_source' in admit_source_df.columns and 'encounter_count' in admit_source_df.columns:
                fig = px.bar(
                    admit_source_df.head(8),  # Top 8
                    x='admit_source',
                    y='encounter_count',
                    title='Admit Source (Top 8)',
                    labels={'encounter_count': 'Count', 'admit_source': 'Admit Source'},
                    color='encounter_count',
                    color_continuous_scale='Oranges'
                )
                fig.update_layout(height=250, showlegend=False, xaxis_tickangle=-45, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Admit Source Data", expanded=False):
                    st.dataframe(admit_source_df, use_container_width=True, hide_index=True)
            elif not admit_source_df.empty:
                st.warning(f"Missing columns: {list(admit_source_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(admit_source_df, use_container_width=True, hide_index=True)
    
    with visit_col3:
        # Admit Type
        if dimensions.get('admit_type'):
            admit_type_df = pd.DataFrame(dimensions['admit_type'])
            if not admit_type_df.empty and 'admit_type' in admit_type_df.columns and 'encounter_count' in admit_type_df.columns:
                fig = px.bar(
                    admit_type_df,
                    x='admit_type',
                    y='encounter_count',
                    title='Admit Type',
                    labels={'encounter_count': 'Count', 'admit_type': 'Admit Type'},
                    color='encounter_count',
                    color_continuous_scale='Reds'
                )
                fig.update_layout(height=250, showlegend=False, xaxis_tickangle=-45, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Admit Type Data", expanded=False):
                    st.dataframe(admit_type_df, use_container_width=True, hide_index=True)
            elif not admit_type_df.empty:
                st.warning(f"Missing columns: {list(admit_type_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(admit_type_df, use_container_width=True, hide_index=True)
    
    # Site Characteristics Section (left to right)
    st.subheader("🏛️ Site Characteristics")
    site_col1, site_col2, site_col3 = st.columns(3)
    
    with site_col1:
        # Urban/Rural
        if dimensions.get('urban_rural'):
            urban_rural_df = pd.DataFrame(dimensions['urban_rural'])
            if not urban_rural_df.empty and 'location_type' in urban_rural_df.columns and 'patient_count' in urban_rural_df.columns:
                fig = go.Figure(data=[
                    go.Pie(
                        labels=urban_rural_df['location_type'],
                        values=urban_rural_df['patient_count'],
                        hole=0.4
                    )
                ])
                fig.update_layout(title='Urban vs Rural', height=250, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Urban/Rural Data", expanded=False):
                    st.dataframe(urban_rural_df, use_container_width=True, hide_index=True)
            elif not urban_rural_df.empty:
                st.warning(f"Missing columns: {list(urban_rural_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(urban_rural_df, use_container_width=True, hide_index=True)
    
    with site_col2:
        # Teaching Status
        if dimensions.get('teaching'):
            teaching_df = pd.DataFrame(dimensions['teaching'])
            if not teaching_df.empty and 'teaching_status' in teaching_df.columns and 'patient_count' in teaching_df.columns:
                fig = go.Figure(data=[
                    go.Pie(
                        labels=teaching_df['teaching_status'],
                        values=teaching_df['patient_count'],
                        hole=0.4,
                        marker_colors=['rgba(59, 130, 246, 0.8)', 'rgba(156, 163, 175, 0.8)', 'rgba(239, 68, 68, 0.8)']
                    )
                ])
                fig.update_layout(title='Teaching Status', height=250, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Teaching Status Data", expanded=False):
                    st.dataframe(teaching_df, use_container_width=True, hide_index=True)
            elif not teaching_df.empty:
                st.warning(f"Missing columns: {list(teaching_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(teaching_df, use_container_width=True, hide_index=True)
    
    with site_col3:
        # Bed Count
        if dimensions.get('bed_count'):
            bed_count_df = pd.DataFrame(dimensions['bed_count'])
            if not bed_count_df.empty and 'bed_count_group' in bed_count_df.columns and 'patient_count' in bed_count_df.columns:
                fig = px.bar(
                    bed_count_df,
                    x='bed_count_group',
                    y='patient_count',
                    title='Bed Count Groups',
                    labels={'patient_count': 'Count', 'bed_count_group': 'Bed Count'},
                    color='patient_count',
                    color_continuous_scale='Teal'
                )
                fig.update_layout(height=250, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Bed Count Data", expanded=False):
                    st.dataframe(bed_count_df, use_container_width=True, hide_index=True)
            elif not bed_count_df.empty:
                st.warning(f"Missing columns: {list(bed_count_df.columns)}")
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(bed_count_df, use_container_width=True, hide_index=True)


def display_demographics(data: dict):
    """Display demographics data (legacy function - kept for compatibility)"""
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
    
    # Sidebar navigation and workflow progress
    with st.sidebar:
        st.title("🏥 Clinical Cohort Assistant")
        page = st.radio(
            "Navigation",
            ["Chat", "Configuration"],
            label_visibility="collapsed"
        )
        
        # Show workflow progress if on Chat page
        if page == "Chat" and st.session_state.services_initialized:
            st.markdown("---")
            st.markdown("### Workflow Progress")
            
            # Determine current step
            has_analysis = st.session_state.get("criteria_analysis") is not None
            has_codes = len(st.session_state.get("codes", [])) > 0
            has_selected_codes = len(st.session_state.get("selected_codes", [])) > 0
            has_refined = st.session_state.get("refined_criteria_text") != ""
            has_genie_result = st.session_state.get("genie_result") is not None
            
            steps = [
                ("1️⃣", "Enter Criteria", has_analysis),
                ("2️⃣", "Select Codes", has_selected_codes),
                ("3️⃣", "Refine Criteria", has_refined),
                ("4️⃣", "View Results", has_genie_result),
            ]
            
            for icon, label, completed in steps:
                if completed:
                    st.markdown(f"{icon} ✅ **{label}**")
                else:
                    st.markdown(f"{icon} ⏳ {label}")
            
            # Quick actions in sidebar
            st.markdown("---")
            st.markdown("### Quick Actions")
            if st.button("🔄 Reset Workflow", use_container_width=True):
                # Reset all workflow-related session state
                st.session_state.criteria_analysis = None
                st.session_state.criteria_text = ""
                st.session_state.codes = []
                st.session_state.selected_codes = []
                st.session_state.refined_criteria = None
                st.session_state.refined_criteria_text = ""
                st.session_state.code_search_text = ""
                st.session_state.code_search_error = ""
                st.session_state.genie_result = None
                st.session_state.genie_error = None
                st.session_state.genie_running = False
                st.rerun()
    
    # Route to appropriate page
    if page == "Configuration":
        render_config_page()
    else:
        render_chat_page()


if __name__ == "__main__":
    main()

