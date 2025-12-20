"""
Clinical Cohort Assistant - Streamlit Version
Databricks-powered cohort builder with natural language interface
"""

import streamlit as st
import os
import sys
from pathlib import Path

# Add current directory to Python path for Databricks compatibility
# This ensures local modules (config, services) can be imported
current_dir = Path(__file__).parent.absolute()
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

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
        
        # Pre-discover schema and exact column mappings for faster dimension analysis (cache it)
        try:
            from services.schema_discovery import SchemaDiscoveryService
            from services.dynamic_dimension_analysis import DynamicDimensionAnalysisService
            
            schema_service = SchemaDiscoveryService()
            dynamic_service = DynamicDimensionAnalysisService()
            
            # Cache general schema context upfront
            schema_context = schema_service.get_schema_context_for_llm(
                config.patient_catalog,
                config.patient_schema
            )
            logger.info(f"Schema context cached ({len(schema_context)} chars)")
            
            # Pre-discover and cache exact column names for ALL dimensions
            # This ensures the system is "hot" and ready to use immediately
            dimension_names = [
                # Patient-level dimensions
                'gender', 'race', 'ethnicity',
                # Visit-level dimensions
                'visit_level', 'admit_type', 'admit_source',
                # Site-level dimensions
                'urban_rural', 'teaching', 'bed_count'
            ]
            
            logger.info(f"Pre-discovering exact column mappings for {len(dimension_names)} dimensions...")
            for dim_name in dimension_names:
                try:
                    exact_columns = schema_service.get_exact_column_names_for_dimension(
                        config.patient_catalog,
                        config.patient_schema,
                        dim_name
                    )
                    # Cache in dynamic service
                    cache_key = f"{config.patient_catalog}.{config.patient_schema}.{dim_name}"
                    dynamic_service._exact_column_cache[cache_key] = exact_columns
                    logger.info(f"  ✓ {dim_name}: {exact_columns}")
                except Exception as dim_error:
                    logger.warning(f"  ✗ {dim_name}: {str(dim_error)}")
            
            # Store dynamic service in session state and dimension service for reuse
            st.session_state.dynamic_dimension_service = dynamic_service
            # Also store in dimension service so it can reuse the cached service
            if hasattr(st.session_state, 'dimension_service'):
                st.session_state.dimension_service._cached_dynamic_service = dynamic_service
            logger.info(f"✅ Schema discovery complete - system is hot and ready!")
            
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
    """Main conversational chat interface using LangGraph agent"""
    # Compact header
    col_header1, col_header2 = st.columns([3, 1])
    with col_header1:
        st.title("🏥 Clinical Cohort Assistant")
    with col_header2:
        st.caption("Conversational cohort builder")
    
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

    # Ensure cohort_agent is initialized
    if 'cohort_agent' not in st.session_state or st.session_state.cohort_agent is None:
        try:
            from services.cohort_agent import CohortAgent
            st.session_state.cohort_agent = CohortAgent(
                vector_service=st.session_state.vector_service,
                genie_service=st.session_state.genie_service,
                cohort_manager=st.session_state.cohort_manager,
                intent_service=st.session_state.intent_service
            )
        except Exception as e:
            st.error(f"Failed to initialize cohort agent: {str(e)}")
            logger.error(f"Agent initialization error: {e}", exc_info=True)
            return

    # Display chat history
    for idx, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            
            # Show additional data if available (codes, SQL, etc.)
            if "data" in message:
                data = message["data"]
                if isinstance(data, dict):
                    # Show codes if available
                    if "codes" in data and data["codes"]:
                        with st.expander("📋 Found Codes", expanded=False):
                            code_df = pd.DataFrame(data["codes"])
                            display_cols = ['code', 'description', 'vocabulary']
                            available_cols = [col for col in display_cols if col in code_df.columns]
                            if available_cols:
                                st.dataframe(code_df[available_cols], use_container_width=True, hide_index=True)
                    
                    # Show SQL if available
                    if "sql" in data and data["sql"]:
                        with st.expander("📝 Generated SQL", expanded=False):
                            st.code(data["sql"], language="sql")
                    
                    # Show count if available
                    if "count" in data:
                        st.info(f"📊 Found {data['count']:,} patients")

    # Chat input
    if prompt := st.chat_input("Describe your clinical criteria or ask a question..."):
        # Process the query through the LangGraph agent
        process_query_conversational(prompt)
        st.rerun()


def process_query_conversational(query: str):
    """Process user query using LangGraph agent with conversational flow and reasoning traces"""
    # Add user message
    st.session_state.messages.append({"role": "user", "content": query})
    
    # Process query through LangGraph agent
    with st.chat_message("assistant"):
        with st.spinner("🤔 Thinking..."):
            try:
                # Get existing state for context
                existing_state = {
                    "cohort_table": st.session_state.get("cohort_table"),
                    "cohort_count": st.session_state.get("cohort_count", 0),
                    "genie_conversation_id": st.session_state.get("genie_conversation_id"),
                    "waiting_for": st.session_state.get("waiting_for"),
                    "codes": st.session_state.get("codes", []),
                    "selected_codes": st.session_state.get("selected_codes", []),
                    "criteria_analysis": st.session_state.get("criteria_analysis"),
                    "diagnosis_phrases": st.session_state.get("diagnosis_phrases", [])
                }
                
                # If user has selected codes in UI, pass them to agent
                selection_key = f"code_selection_{st.session_state.session_id}"
                if selection_key in st.session_state and st.session_state.get("codes"):
                    selected_code_values = st.session_state[selection_key]
                    selected_codes = [c for c in st.session_state.get("codes", []) if c.get('code') in selected_code_values]
                    if selected_codes:
                        existing_state["selected_codes"] = selected_codes
                
                # Process through agent
                result_state = st.session_state.cohort_agent.process_query(
                    query,
                    st.session_state.session_id,
                    existing_state
                )
                
                # Display reasoning steps if available (after spinner completes)
                reasoning_steps = result_state.get("reasoning_steps", [])
                if reasoning_steps:
                    msg_idx = len(st.session_state.messages)
                    with st.expander("🔍 What I'm doing (reasoning steps)", expanded=False):
                        for step_name, description in reasoning_steps:
                            st.markdown(f"**{step_name}**: {description}")
                
                # Update session state with results
                if result_state.get("cohort_table"):
                    st.session_state.cohort_table = result_state["cohort_table"]
                if result_state.get("cohort_count"):
                    st.session_state.cohort_count = result_state["cohort_count"]
                if result_state.get("genie_conversation_id"):
                    st.session_state.genie_conversation_id = result_state["genie_conversation_id"]
                if result_state.get("waiting_for") is not None:
                    st.session_state.waiting_for = result_state["waiting_for"]
                if result_state.get("counts"):
                    st.session_state.counts = result_state["counts"]
                if result_state.get("codes"):
                    st.session_state.codes = result_state["codes"]
                if result_state.get("selected_codes"):
                    st.session_state.selected_codes = result_state["selected_codes"]
                if result_state.get("criteria_analysis"):
                    st.session_state.criteria_analysis = result_state["criteria_analysis"]
                
                # Store agent state for next turn
                st.session_state.agent_state = result_state
                
                # Handle errors
                if result_state.get("error"):
                    error_msg = f"❌ {result_state['error']}"
                    st.error(error_msg)
                    response_text = f"I encountered an error: {result_state['error']}"
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response_text
                    })
                    # Still display the error message
                    st.markdown(response_text)
                    return
                
                # Build conversational response based on what happened
                response_parts = []
                current_step = result_state.get("current_step", "")
                waiting_for = result_state.get("waiting_for")
                response_text = ""  # Initialize to avoid UnboundLocalError
                
                # Check if we're waiting for code search confirmation
                if waiting_for == "code_search_confirmation":
                    # Show structured breakdown
                    analysis = result_state.get("criteria_analysis", {})
                    
                    if analysis:
                        response_parts.append("I understood your criteria. Here's what I found:")
                        response_parts.append("")
                        
                        # Show conditions
                        conditions = analysis.get("conditions", [])
                        if conditions:
                            response_parts.append(f"**Conditions:** {', '.join(conditions)}")
                        
                        # Show medications
                        drugs = analysis.get("drugs", [])
                        if drugs:
                            response_parts.append(f"**Medications:** {', '.join(drugs)}")
                        
                        # Show demographics
                        demographics = analysis.get("demographics", [])
                        if demographics:
                            response_parts.append(f"**Demographics:** {', '.join(demographics)}")
                        
                        # Show procedures
                        procedures = analysis.get("procedures", [])
                        if procedures:
                            response_parts.append(f"**Procedures:** {', '.join(procedures)}")
                        
                        # Show timeframe
                        timeframe = analysis.get("timeframe", "")
                        if timeframe:
                            response_parts.append(f"**Timeframe:** {timeframe}")
                        
                        response_parts.append("")
                        response_parts.append("**Would you like me to search for standard clinical codes for these criteria?**")
                        response_text = "\n".join(response_parts)
                    else:
                        # Fallback if no analysis
                        diagnosis_phrases = result_state.get("diagnosis_phrases", [])
                        if diagnosis_phrases:
                            response_parts.append(f"I understood you're looking for: **{', '.join(diagnosis_phrases)}**")
                        response_parts.append("\n\n**Would you like me to search for standard clinical codes?**")
                        response_text = "\n".join(response_parts)
                
                # Check if we're waiting for code selection
                elif waiting_for == "code_selection":
                    # Show what I understood
                    diagnosis_phrases = result_state.get("diagnosis_phrases", [])
                    if diagnosis_phrases:
                        response_parts.append(f"I understood you're looking for: **{', '.join(diagnosis_phrases)}**")
                    
                    # Show codes found
                    codes = result_state.get("codes", [])
                    if codes:
                        response_parts.append(f"I found **{len(codes)} relevant clinical codes**.")
                        
                        # Initialize selected codes in session state if not exists
                        selection_key = f"code_selection_{st.session_state.session_id}"
                        if selection_key not in st.session_state:
                            st.session_state[selection_key] = [c.get('code') for c in codes]
                        
                        # Show codes with interactive selection
                        msg_idx = len(st.session_state.messages)
                        with st.expander(f"📋 Select Codes ({len(codes)} found)", expanded=True):
                            code_df = pd.DataFrame(codes)
                            display_cols = ['code', 'description', 'vocabulary']
                            available_cols = [col for col in display_cols if col in code_df.columns]
                            
                            if available_cols:
                                # Add checkbox column
                                selected_codes_list = st.session_state.get(selection_key, [])
                                
                                # Quick action buttons
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    if st.button("✅ Select All", key=f"select_all_{msg_idx}"):
                                        st.session_state[selection_key] = [c.get('code') for c in codes if c.get('code')]
                                        st.rerun()
                                with col2:
                                    if st.button("❌ Deselect All", key=f"deselect_all_{msg_idx}"):
                                        st.session_state[selection_key] = []
                                        st.rerun()
                                with col3:
                                    if st.button("🚀 Use Selected", key=f"use_selected_{msg_idx}"):
                                        # Process selected codes
                                        selected_codes = [c for c in codes if c.get('code') in st.session_state.get(selection_key, [])]
                                        if selected_codes:
                                            # Update agent state and proceed
                                            st.session_state.selected_codes = selected_codes
                                            # Trigger code confirmation
                                            process_query_conversational("use selected codes")
                                            st.rerun()
                                        else:
                                            st.warning("Please select at least one code")
                                
                                st.markdown("---")
                                
                                # Show codes with checkboxes
                                for idx, code_row in code_df.iterrows():
                                    code_value = code_row.get('code', '')
                                    if not code_value:
                                        continue
                                    
                                    is_selected = code_value in st.session_state.get(selection_key, [])
                                    checkbox_key = f"code_checkbox_{msg_idx}_{idx}_{code_value}"
                                    
                                    checkbox_label = f"**{code_value}** - {code_row.get('description', 'N/A')} ({code_row.get('vocabulary', 'N/A')})"
                                    
                                    if st.checkbox(checkbox_label, value=is_selected, key=checkbox_key):
                                        # Add to selection
                                        if code_value not in st.session_state.get(selection_key, []):
                                            if selection_key not in st.session_state:
                                                st.session_state[selection_key] = []
                                            st.session_state[selection_key].append(code_value)
                                    else:
                                        # Remove from selection
                                        if code_value in st.session_state.get(selection_key, []):
                                            st.session_state[selection_key].remove(code_value)
                                
                                # Show selection count
                                selected_count = len(st.session_state.get(selection_key, []))
                                st.info(f"📊 {selected_count} of {len(codes)} codes selected")
                        
                        # Ask about code selection conversationally
                        response_parts.append("\n\n**Select codes above, then click 'Use Selected', or say 'use all' to use all codes.**")
                    else:
                        # No codes found
                        response_parts.append("I searched for codes but didn't find any matching results.")
                        response_parts.append("Would you like to refine your criteria or proceed with natural language query?")
                    
                    response_text = "\n".join(response_parts) if response_parts else "I've searched for codes. See the results above."
                
                # Check if we're waiting for analysis decision
                elif waiting_for == "analysis_decision":
                    # Show counts
                    counts = result_state.get("counts", {})
                    patients = counts.get("patients", 0)
                    visits = counts.get("visits", 0)
                    sites = counts.get("sites", 0)
                    
                    if patients > 0:
                        count_text = f"✅ Found **{patients:,} patients**"
                        if visits > 0:
                            count_text += f" across **{visits:,} visits**"
                        if sites > 0:
                            count_text += f" at **{sites} sites**"
                        response_parts.append(count_text)
                    else:
                        response_parts.append("✅ Generated SQL query. Ready to execute.")
                    
                    # Show SQL if available
                    sql = result_state.get("sql")
                    if sql:
                        msg_idx = len(st.session_state.messages)
                        with st.expander("📝 View Generated SQL", expanded=False):
                            st.code(sql, language="sql")
                    
                    # Ask about analysis conversationally
                    response_parts.append("\n\n**Would you like to explore this cohort further, or would you like to adjust your criteria?**")
                    response_text = "\n".join(response_parts)
                
                elif current_step == "new_cohort":
                    # Show what I understood
                    diagnosis_phrases = result_state.get("diagnosis_phrases", [])
                    if diagnosis_phrases:
                        response_parts.append(f"I understood you're looking for: **{', '.join(diagnosis_phrases)}**")
                    
                    # Show codes found
                    codes = result_state.get("codes", [])
                    if codes:
                        response_parts.append(f"I found **{len(codes)} relevant clinical codes**.")
                        
                        # Show codes in expandable section
                        msg_idx = len(st.session_state.messages)
                        with st.expander(f"📋 View {len(codes)} Codes Found", expanded=False):
                            code_df = pd.DataFrame(codes)
                            display_cols = ['code', 'description', 'vocabulary']
                            available_cols = [col for col in display_cols if col in code_df.columns]
                            if available_cols:
                                st.dataframe(code_df[available_cols], use_container_width=True, hide_index=True)
                    
                    # Show Genie prompt/enrichment
                    genie_prompt = result_state.get("genie_prompt")
                    if genie_prompt:
                        msg_idx = len(st.session_state.messages)
                        with st.expander("🧠 How I'm enriching your request for Genie", expanded=False):
                            st.markdown(genie_prompt)
                    
                    # Show SQL if generated
                    sql = result_state.get("sql")
                    if sql:
                        response_parts.append("I've generated a SQL query to find matching patients.")
                        msg_idx = len(st.session_state.messages)
                        with st.expander("📝 View Generated SQL", expanded=False):
                            st.code(sql, language="sql")
                        
                        # Offer to execute
                        if not result_state.get("cohort_table"):
                            button_key = f"execute_cohort_{st.session_state.session_id}_{len(st.session_state.messages)}"
                            if st.button("🚀 Execute Query & Create Cohort", key=button_key):
                                execute_and_materialize_cohort(result_state)
                                st.rerun()
                    
                    # Show cohort count if available
                    count = result_state.get("cohort_count", 0)
                    if count > 0:
                        response_parts.append(f"✅ Found **{count:,} patients** matching your criteria!")
                    
                    # Build final response
                    response_text = "\n".join(response_parts) if response_parts else "I've processed your request. Review the details above."
                    
                elif current_step in ["follow_up", "insights"]:
                    # Handle follow-up questions
                    answer_data = result_state.get("answer_data", {})
                    answer_type = answer_data.get("type")
                    data = answer_data.get("data")
                    
                    if answer_type == "demographics":
                        response_text = "Here are the demographic characteristics of your cohort:"
                        display_demographics(data)
                    elif answer_type == "sites":
                        response_text = "Here are the site characteristics:"
                        display_sites(data)
                    elif answer_type == "trends":
                        response_text = "Here are the admission trends:"
                        display_trends(data)
                    elif answer_type == "outcomes":
                        response_text = "Here are the outcomes:"
                        display_outcomes(data)
                    elif answer_type == "count":
                        response_text = f"The cohort contains **{data:,} patients**."
                    elif answer_type == "genie":
                        response_text = "Here's what I found:"
                        if data.get("sql"):
                            msg_idx = len(st.session_state.messages)
                            with st.expander("📝 SQL Used", expanded=False):
                                st.code(data["sql"], language="sql")
                        if data.get("data"):
                            st.dataframe(pd.DataFrame(data["data"]), use_container_width=True)
                    else:
                        response_text = "I've processed your question. See the results above."
                
                else:
                    response_text = "I've processed your request."
                
                # Display main response
                st.markdown(response_text)
                
                # Add to message history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response_text,
                    "data": {
                        "codes": result_state.get("codes", []),
                        "sql": result_state.get("sql"),
                        "count": result_state.get("cohort_count", 0),
                        "genie_prompt": result_state.get("genie_prompt")
                    }
                })
                
            except Exception as e:
                error_msg = f"I encountered an error while processing your request: {str(e)}"
                st.error(error_msg)
                logger.error(f"Error in conversational query processing: {e}", exc_info=True)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })


def check_genie_status(conversation_id: str):
    """Check Genie conversation status and update session state"""
    with st.spinner("Checking Genie status..."):
        try:
            # Get message_id from conversation if needed
            # For now, try polling with just conversation_id
            # The polling method will try to get message_id if needed
            result = st.session_state.genie_service._poll_for_completion(
                conversation_id,
                None  # message_id - will be fetched during polling
            )
            
            # Update session state with results
            if result.get('sql'):
                # Update agent state if it exists
                if 'agent_state' in st.session_state:
                    st.session_state.agent_state['sql'] = result['sql']
                    st.session_state.agent_state['cohort_count'] = result.get('row_count', 0)
                st.success("✅ SQL generated! You can now execute the query.")
            else:
                st.info("⏳ Genie is still processing. Please check again in a moment.")
        except Exception as e:
            st.error(f"Error checking Genie status: {str(e)}")
            logger.error(f"Genie status check error: {e}", exc_info=True)


def execute_and_materialize_cohort(result_state: dict):
    """Execute SQL and materialize cohort table"""
    sql = result_state.get("sql")
    if not sql:
        st.error("No SQL available to execute. Please check Genie status first.")
        return
    
    with st.spinner("Executing query and creating cohort table..."):
        try:
            # Use cohort_manager to materialize
            cohort_result = st.session_state.cohort_manager.materialize_cohort(
                st.session_state.session_id,
                sql
            )
            
            st.session_state.cohort_table = cohort_result['cohort_table']
            st.session_state.cohort_count = cohort_result['count']
            st.session_state.cohort_table_info = {
                'cohort_table': cohort_result['cohort_table'],
                'count': cohort_result['count'],
                'has_medrec_key': cohort_result.get('has_medrec_key', False)
            }
            
            st.success(f"✅ Cohort created: {cohort_result['count']:,} patients")
        except Exception as e:
            st.error(f"Error creating cohort: {str(e)}")
            logger.error(f"Cohort materialization error: {e}", exc_info=True)


def display_dimension_results_compact(results: dict):
    """
    Display dimension analysis results with visualizations in a compact grid layout
    
    Args:
        results: Dictionary with 'dimensions' (dict of dimension results) and 'errors' (dict of errors)
    """
    dimensions = results.get('dimensions', {})
    errors = results.get('errors', {})
    generated_queries = results.get('generated_queries', {})
    validation_results = results.get('validation_results', {})
    
    # Compact validation summary
    if validation_results:
        valid_count = sum(1 for v in validation_results.values() if v.get('is_valid', False))
        total_count = len(validation_results)
        if valid_count == total_count:
            st.success(f"✅ All {total_count} dimension queries validated")
        else:
            st.warning(f"⚠️ {valid_count}/{total_count} queries validated")
    
    # Show errors compactly
    if errors:
        st.warning(f"⚠️ Some dimensions failed: {', '.join(errors.keys())}")
        with st.expander("🔍 View Errors", expanded=False):
            for dim_name, error_msg in errors.items():
                st.error(f"**{dim_name}**: {error_msg}")
                if dim_name in generated_queries:
                    st.code(generated_queries[dim_name], language='sql')
    
    # Show SQL queries in expander
    if generated_queries:
        with st.expander("🔍 View Generated SQL Queries", expanded=False):
            for dim_name, sql in generated_queries.items():
                validation = validation_results.get(dim_name, {})
                is_valid = validation.get('is_valid', False)
                st.markdown(f"**{dim_name}** {'✅' if is_valid else '❌'}")
                st.code(sql, language='sql')
                st.markdown("---")
    
    if not dimensions or all(not v for v in dimensions.values()):
        st.info("No dimension data available")
        return
    
    # Professional color palette (blue/yellow/green - simple and clean)
    COLOR_PALETTE = ['#2563eb', '#fbbf24', '#10b981', '#3b82f6', '#f59e0b', '#059669', '#1d4ed8', '#d97706']
    COLOR_SCALE = 'Blues'  # For continuous scales, use Blues
    
    # Organize charts into tabs
    chart_tabs = st.tabs(["👥 Patient Demographics", "🏥 Visit Characteristics", "🏛️ Site Characteristics"])
    
    # Tab 1: Patient Demographics
    with chart_tabs[0]:
        demo_col1, demo_col2, demo_col3 = st.columns(3)
        
        with demo_col1:
            if dimensions.get('gender'):
                gender_df = pd.DataFrame(dimensions['gender'])
                if not gender_df.empty and 'gender' in gender_df.columns and 'patient_count' in gender_df.columns:
                    # Toggle for chart/data view
                    show_data = st.checkbox("Show data table", key="gender_data_toggle")
                    if show_data:
                        st.dataframe(gender_df, use_container_width=True, hide_index=True)
                    else:
                        fig = go.Figure(data=[go.Pie(
                            labels=gender_df['gender'], 
                            values=gender_df['patient_count'], 
                            hole=0.4,
                            marker_colors=COLOR_PALETTE[:len(gender_df)]
                        )])
                        fig.update_layout(title='Gender', height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=True)
                        st.plotly_chart(fig, use_container_width=True)
        
        with demo_col2:
            if dimensions.get('race'):
                race_df = pd.DataFrame(dimensions['race'])
                if not race_df.empty and 'race' in race_df.columns and 'patient_count' in race_df.columns:
                    show_data = st.checkbox("Show data table", key="race_data_toggle")
                    if show_data:
                        st.dataframe(race_df, use_container_width=True, hide_index=True)
                    else:
                        fig = px.bar(race_df.head(8), x='race', y='patient_count', title='Race (Top 8)', 
                                   labels={'patient_count': 'Count', 'race': 'Race'}, 
                                   color='patient_count', color_continuous_scale=COLOR_SCALE)
                        fig.update_layout(height=300, showlegend=False, xaxis_tickangle=-45, margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig, use_container_width=True)
        
        with demo_col3:
            if dimensions.get('ethnicity'):
                ethnicity_df = pd.DataFrame(dimensions['ethnicity'])
                if not ethnicity_df.empty and 'ethnicity' in ethnicity_df.columns and 'patient_count' in ethnicity_df.columns:
                    show_data = st.checkbox("Show data table", key="ethnicity_data_toggle")
                    if show_data:
                        st.dataframe(ethnicity_df, use_container_width=True, hide_index=True)
                    else:
                        fig = go.Figure(data=[go.Pie(
                            labels=ethnicity_df['ethnicity'], 
                            values=ethnicity_df['patient_count'], 
                            hole=0.4,
                            marker_colors=COLOR_PALETTE[:len(ethnicity_df)]
                        )])
                        fig.update_layout(title='Ethnicity', height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=True)
                        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 2: Visit Characteristics
    with chart_tabs[1]:
        visit_col1, visit_col2, visit_col3 = st.columns(3)
        
        with visit_col1:
            if dimensions.get('visit_level'):
                visit_df = pd.DataFrame(dimensions['visit_level'])
                if not visit_df.empty and 'visit_level' in visit_df.columns and 'encounter_count' in visit_df.columns:
                    show_data = st.checkbox("Show data table", key="visit_level_data_toggle")
                    if show_data:
                        st.dataframe(visit_df, use_container_width=True, hide_index=True)
                    else:
                        fig = px.bar(visit_df, x='visit_level', y='encounter_count', title='Visit Level',
                                   labels={'encounter_count': 'Count', 'visit_level': 'Visit Level'}, 
                                   color='encounter_count', color_continuous_scale=COLOR_SCALE)
                        fig.update_layout(height=300, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig, use_container_width=True)
        
        with visit_col2:
            if dimensions.get('admit_type'):
                admit_type_df = pd.DataFrame(dimensions['admit_type'])
                if not admit_type_df.empty and 'admit_type' in admit_type_df.columns and 'encounter_count' in admit_type_df.columns:
                    show_data = st.checkbox("Show data table", key="admit_type_data_toggle")
                    if show_data:
                        st.dataframe(admit_type_df, use_container_width=True, hide_index=True)
                    else:
                        fig = px.bar(admit_type_df, x='admit_type', y='encounter_count', title='Admit Type',
                                   labels={'encounter_count': 'Count', 'admit_type': 'Admit Type'}, 
                                   color='encounter_count', color_continuous_scale=COLOR_SCALE)
                        fig.update_layout(height=300, showlegend=False, xaxis_tickangle=-45, margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig, use_container_width=True)
        
        with visit_col3:
            if dimensions.get('admit_source'):
                admit_source_df = pd.DataFrame(dimensions['admit_source'])
                if not admit_source_df.empty and 'admit_source' in admit_source_df.columns and 'encounter_count' in admit_source_df.columns:
                    show_data = st.checkbox("Show data table", key="admit_source_data_toggle")
                    if show_data:
                        st.dataframe(admit_source_df, use_container_width=True, hide_index=True)
                    else:
                        fig = px.bar(admit_source_df.head(8), x='admit_source', y='encounter_count', title='Admit Source (Top 8)',
                                   labels={'encounter_count': 'Count', 'admit_source': 'Admit Source'}, 
                                   color='encounter_count', color_continuous_scale=COLOR_SCALE)
                        fig.update_layout(height=300, showlegend=False, xaxis_tickangle=-45, margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 3: Site Characteristics
    with chart_tabs[2]:
        site_col1, site_col2, site_col3 = st.columns(3)
        
        with site_col1:
            if dimensions.get('urban_rural'):
                urban_rural_df = pd.DataFrame(dimensions['urban_rural'])
                if not urban_rural_df.empty and 'location_type' in urban_rural_df.columns and 'patient_count' in urban_rural_df.columns:
                    show_data = st.checkbox("Show data table", key="urban_rural_data_toggle")
                    if show_data:
                        st.dataframe(urban_rural_df, use_container_width=True, hide_index=True)
                    else:
                        fig = go.Figure(data=[go.Pie(
                            labels=urban_rural_df['location_type'], 
                            values=urban_rural_df['patient_count'], 
                            hole=0.4,
                            marker_colors=COLOR_PALETTE[:len(urban_rural_df)]
                        )])
                        fig.update_layout(title='Urban/Rural', height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=True)
                        st.plotly_chart(fig, use_container_width=True)
        
        with site_col2:
            if dimensions.get('teaching'):
                teaching_df = pd.DataFrame(dimensions['teaching'])
                if not teaching_df.empty and 'teaching_status' in teaching_df.columns and 'patient_count' in teaching_df.columns:
                    show_data = st.checkbox("Show data table", key="teaching_data_toggle")
                    if show_data:
                        st.dataframe(teaching_df, use_container_width=True, hide_index=True)
                    else:
                        fig = go.Figure(data=[go.Pie(
                            labels=teaching_df['teaching_status'], 
                            values=teaching_df['patient_count'], 
                            hole=0.4,
                            marker_colors=COLOR_PALETTE[:len(teaching_df)]
                        )])
                        fig.update_layout(title='Teaching Status', height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=True)
                        st.plotly_chart(fig, use_container_width=True)
        
        with site_col3:
            if dimensions.get('bed_count'):
                bed_count_df = pd.DataFrame(dimensions['bed_count'])
                if not bed_count_df.empty and 'bed_count_group' in bed_count_df.columns and 'patient_count' in bed_count_df.columns:
                    show_data = st.checkbox("Show data table", key="bed_count_data_toggle")
                    if show_data:
                        st.dataframe(bed_count_df, use_container_width=True, hide_index=True)
                    else:
                        fig = px.bar(bed_count_df, x='bed_count_group', y='patient_count', title='Bed Count Groups',
                                   labels={'patient_count': 'Count', 'bed_count_group': 'Bed Count'}, 
                                   color='patient_count', color_continuous_scale=COLOR_SCALE)
                        fig.update_layout(height=300, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig, use_container_width=True)


def display_dimension_results(results: dict):
    """
    Legacy function - redirects to compact version
    """
    display_dimension_results_compact(results)
    
    # Patient-Level Demographics Section (left to right)
    st.subheader("👥 Patient Demographics")
    demo_col1, demo_col2, demo_col3 = st.columns(3)
    
    with demo_col1:
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
            elif not gender_df.empty:
                st.warning(f"Missing columns: {list(gender_df.columns)}")
    
    with demo_col2:
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
            elif not race_df.empty:
                st.warning(f"Missing columns: {list(race_df.columns)}")
    
    with demo_col3:
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
            elif not ethnicity_df.empty:
                st.warning(f"Missing columns: {list(ethnicity_df.columns)}")
    
    # Data tables section (outside columns to avoid nesting)
    st.markdown("---")
    with st.expander("📊 View Patient Demographics Data Tables", expanded=False):
        data_col1, data_col2, data_col3 = st.columns(3)
        with data_col1:
            if dimensions.get('gender'):
                gender_df = pd.DataFrame(dimensions['gender'])
                if not gender_df.empty:
                    st.markdown("**Gender**")
                    st.dataframe(gender_df, use_container_width=True, hide_index=True)
        with data_col2:
            if dimensions.get('race'):
                race_df = pd.DataFrame(dimensions['race'])
                if not race_df.empty:
                    st.markdown("**Race**")
                    st.dataframe(race_df, use_container_width=True, hide_index=True)
        with data_col3:
            if dimensions.get('ethnicity'):
                ethnicity_df = pd.DataFrame(dimensions['ethnicity'])
                if not ethnicity_df.empty:
                    st.markdown("**Ethnicity**")
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
            elif not visit_df.empty:
                st.warning(f"Missing columns: {list(visit_df.columns)}")
    
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
            elif not admit_source_df.empty:
                st.warning(f"Missing columns: {list(admit_source_df.columns)}")
    
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
            elif not admit_type_df.empty:
                st.warning(f"Missing columns: {list(admit_type_df.columns)}")
    
    # Data tables section (outside columns to avoid nesting)
    st.markdown("---")
    with st.expander("📊 View Visit Characteristics Data Tables", expanded=False):
        data_col1, data_col2, data_col3 = st.columns(3)
        with data_col1:
            if dimensions.get('visit_level'):
                visit_df = pd.DataFrame(dimensions['visit_level'])
                if not visit_df.empty:
                    st.markdown("**Visit Level**")
                    st.dataframe(visit_df, use_container_width=True, hide_index=True)
        with data_col2:
            if dimensions.get('admit_source'):
                admit_source_df = pd.DataFrame(dimensions['admit_source'])
                if not admit_source_df.empty:
                    st.markdown("**Admit Source**")
                    st.dataframe(admit_source_df, use_container_width=True, hide_index=True)
        with data_col3:
            if dimensions.get('admit_type'):
                admit_type_df = pd.DataFrame(dimensions['admit_type'])
                if not admit_type_df.empty:
                    st.markdown("**Admit Type**")
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
                        hole=0.4,
                        marker_colors=['rgba(34, 197, 94, 0.8)', 'rgba(251, 191, 36, 0.8)', 'rgba(156, 163, 175, 0.8)']
                    )
                ])
                fig.update_layout(title='Urban/Rural', height=250, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
            elif not urban_rural_df.empty:
                st.warning(f"Missing columns: {list(urban_rural_df.columns)}")
    
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
                        marker_colors=['rgba(139, 92, 246, 0.8)', 'rgba(236, 72, 153, 0.8)', 'rgba(156, 163, 175, 0.8)']
                    )
                ])
                fig.update_layout(title='Teaching Status', height=250, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
            elif not teaching_df.empty:
                st.warning(f"Missing columns: {list(teaching_df.columns)}")
    
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
            elif not bed_count_df.empty:
                st.warning(f"Missing columns: {list(bed_count_df.columns)}")
    
    # Data tables section (outside columns to avoid nesting)
    st.markdown("---")
    with st.expander("📊 View Site Characteristics Data Tables", expanded=False):
        data_col1, data_col2, data_col3 = st.columns(3)
        with data_col1:
            if dimensions.get('urban_rural'):
                urban_rural_df = pd.DataFrame(dimensions['urban_rural'])
                if not urban_rural_df.empty:
                    st.markdown("**Urban/Rural**")
                    st.dataframe(urban_rural_df, use_container_width=True, hide_index=True)
        with data_col2:
            if dimensions.get('teaching'):
                teaching_df = pd.DataFrame(dimensions['teaching'])
                if not teaching_df.empty:
                    st.markdown("**Teaching Status**")
                    st.dataframe(teaching_df, use_container_width=True, hide_index=True)
        with data_col3:
            if dimensions.get('bed_count'):
                bed_count_df = pd.DataFrame(dimensions['bed_count'])
                if not bed_count_df.empty:
                    st.markdown("**Bed Count**")
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
        
        # Show conversational context if on Chat page
        if page == "Chat" and st.session_state.services_initialized:
            st.markdown("---")
            st.markdown("### Current Context")
            
            # Show current cohort if exists
            cohort_table = st.session_state.get("cohort_table")
            cohort_count = st.session_state.get("cohort_count", 0)
            
            if cohort_table:
                st.success(f"✅ Active Cohort: {cohort_count:,} patients")
                st.caption(f"Table: `{cohort_table}`")
            else:
                st.info("💬 Start a conversation to build a cohort")
            
            # Show message count
            msg_count = len(st.session_state.get("messages", []))
            if msg_count > 0:
                st.caption(f"💬 {msg_count} message(s) in conversation")
            
            # Quick actions in sidebar
            st.markdown("---")
            st.markdown("### Quick Actions")
            if st.button("🔄 Clear Conversation", use_container_width=True):
                # Reset conversational state
                st.session_state.messages = []
                st.session_state.agent_state = {}
                st.session_state.cohort_table = None
                st.session_state.cohort_count = 0
                st.session_state.genie_conversation_id = None
                st.session_state.criteria_analysis = None
                st.session_state.codes = []
                st.session_state.selected_codes = []
                st.rerun()
    
    # Route to appropriate page
    if page == "Configuration":
        render_config_page()
    else:
        render_chat_page()


if __name__ == "__main__":
    main()

