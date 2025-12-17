# Configuration Flow - Where Databricks Details Are Stored

## Current Flow

### 1. **User Input (UI)**
   - Location: `app.py` → `render_config_page()` function
   - User enters details in the Streamlit Configuration page:
     - Databricks Host
     - Databricks Token
     - Genie Space ID
     - Patient Catalog & Schema
     - Vector Catalog & Schema
     - SQL Warehouse ID

### 2. **Session State Storage**
   - Location: `st.session_state.config` (in-memory dictionary)
   - Code: Lines 162-171 in `app.py`
   ```python
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
   ```

### 3. **Environment Variables (Temporary)**
   - Location: `initialize_services()` function (lines 54-56)
   - Values are copied from session state to `os.environ`
   ```python
   for key, value in st.session_state.config.items():
       if value:
           os.environ[key] = value
   ```

### 4. **Config Module**
   - Location: `config.py`
   - Reads from environment variables: `os.getenv("GENIE_SPACE_ID")`, etc.

### 5. **Services Use Config**
   - Services import `config` and use values:
     - `VectorSearchService()` uses `config.host`, `config.token`
     - `GenieService()` uses `config.space_id`
     - `CohortManager()` uses `config.warehouse_id`

## ⚠️ Important: Configuration is NOT Persisted

**Current behavior:**
- ✅ Configuration works during the session
- ❌ Configuration is **lost** when:
  - App restarts
  - Streamlit server restarts
  - Session expires

**Storage location:** Only in memory (`st.session_state`)

## Options to Persist Configuration

### Option 1: Environment Variables (Recommended for Production)
Set environment variables before running the app:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export GENIE_SPACE_ID="your-space-id"
export PATIENT_CATALOG="main"
export PATIENT_SCHEMA="clinical"
export SQL_WAREHOUSE_ID="your-warehouse-id"
```

### Option 2: .env File (For Local Development)
Create `.env` file (already supported by `config.py`):
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token
GENIE_SPACE_ID=your-space-id
PATIENT_CATALOG=main
PATIENT_SCHEMA=clinical
SQL_WAREHOUSE_ID=your-warehouse-id
```

### Option 3: Databricks Secrets (When Running in Databricks)
Use Databricks secrets:
```python
from pyspark.dbutils import DBUtils
dbutils = DBUtils()
token = dbutils.secrets.get(scope="tokens", key="databricks_token")
```

### Option 4: Save to File (Add Persistence)
We could add code to save config to a file and load it on startup.

## Current Configuration Values

To see what's currently configured, check:
1. **In the app:** Go to Configuration page in sidebar
2. **In code:** `st.session_state.config` dictionary
3. **Environment:** `os.getenv("GENIE_SPACE_ID")` etc.

## Recommendation

For testing/deployment:
- **Local testing:** Use `.env` file (add to `.gitignore`)
- **Production/Databricks:** Use environment variables or Databricks secrets
- **UI config:** Keep for convenience, but add file persistence option


