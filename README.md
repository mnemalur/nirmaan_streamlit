# Clinical Cohort Assistant - Streamlit Version

Streamlit-based interface for the Clinical Cohort Assistant with Databricks integration.

## Features

- 🔧 **Easy Configuration**: Configure Databricks credentials through the UI
- 💬 **Natural Language Interface**: Build cohorts using plain English queries
- 📊 **Interactive Visualizations**: Rich charts using Plotly.js
- 🔄 **State Management**: Proper handling of Streamlit state refresh using forms
- 🏥 **Real Databricks Integration**: Connects to actual Databricks workspaces (no mock data)

## Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the app**:
   ```bash
   streamlit run app.py
   ```

3. **Configure Databricks**:
   - Navigate to the "Configuration" page in the sidebar
   - Enter your Databricks credentials:
     - Databricks Host (e.g., `https://your-workspace.cloud.databricks.com`)
     - Databricks Token (personal access token or service principal token)
     - Genie Space ID
     - Patient Catalog and Schema
     - Vector Catalog and Schema
     - SQL Warehouse ID
   - Click "Save Configuration"

4. **Start Building Cohorts**:
   - Go to the "Chat" page
   - Enter natural language queries like:
     - "Find patients with heart failure"
     - "Show me diabetes patients over 65"
     - "Patients with myocardial infarction in the last 30 days"

## Configuration Details

### Required Configuration

- **DATABRICKS_HOST**: Your Databricks workspace URL
- **DATABRICKS_TOKEN**: Personal access token or service principal token
- **GENIE_SPACE_ID**: Genie space ID for SQL generation
- **PATIENT_CATALOG**: Unity Catalog name for patient data (e.g., `main`)
- **PATIENT_SCHEMA**: Schema name for patient tables (e.g., `clinical`)
- **SQL_WAREHOUSE_ID**: SQL Warehouse ID for query execution

### Optional Configuration

- **VECTOR_CATALOG**: Unity Catalog name for vector functions (defaults to PATIENT_CATALOG)
- **VECTOR_SCHEMA**: Schema name for vector functions (e.g., `vector_functions`)

## Architecture

- **Services**: Reuses services from parent directory (`../services/`)
- **Configuration**: Managed through Streamlit session state and environment variables
- **State Management**: Uses Streamlit forms to prevent state refresh issues
- **Visualizations**: Plotly charts for interactive data exploration

## Differences from Flask Version

- **No Mock Data**: This version only works with real Databricks connections
- **UI Configuration**: Configure credentials through the web UI instead of `.env` file
- **Streamlit Forms**: Uses forms extensively to handle state refresh properly
- **Simplified Interface**: Streamlined UI optimized for Streamlit's component model

## Troubleshooting

### Services Not Initializing

- Check that all required configuration fields are filled
- Verify your Databricks token has proper permissions
- Ensure SQL Warehouse is running and accessible

### Charts Not Displaying

- Check that your patient data tables have the required columns:
  - `patdemo` table: `age`, `gender`, `race`, `ethnicity`, `teaching_flag`, `location_type`, `bed_count`
  - If columns are missing, charts will show "No data"

### Query Errors

- Verify your Genie Space ID is correct
- Check that patient catalog and schema names are correct
- Ensure vector function exists in the specified catalog/schema

