# How Configuration Works - Understanding the Flow

## Current Setup

### 1. **config.py** - The Config File Reader
```python
from dotenv import load_dotenv
load_dotenv()  # This loads .env file if it exists

host: str = os.getenv("DATABRICKS_HOST")  # Reads from os.environ
space_id: str = os.getenv("GENIE_SPACE_ID")
# etc...
```

**What `load_dotenv()` does:**
- Looks for `.env` file in the project directory
- If found, loads variables from `.env` into `os.environ`
- If not found, does nothing (no error)

**What `os.getenv()` does:**
- Reads from `os.environ` (Python's environment variables)
- Returns `None` if variable doesn't exist

### 2. **Two Ways to Set Configuration**

#### **Option A: Using .env File (Config File Approach)**

Create a `.env` file in your project:
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token-here
GENIE_SPACE_ID=your-space-id
PATIENT_CATALOG=main
PATIENT_SCHEMA=clinical
SQL_WAREHOUSE_ID=your-warehouse-id
VECTOR_CATALOG=main
VECTOR_SCHEMA=vector_functions
```

**Flow:**
1. `.env` file exists
2. `load_dotenv()` runs → loads variables into `os.environ`
3. `config.py` reads from `os.environ` via `os.getenv()`
4. Services use `config` object

**✅ This is what you want - config file approach!**

#### **Option B: Using UI Form (Current Default)**

**Flow:**
1. User enters values in Streamlit UI
2. Saved to `st.session_state.config` (in-memory)
3. `initialize_services()` copies to `os.environ`:
   ```python
   for key, value in st.session_state.config.items():
       os.environ[key] = value
   ```
4. `config.py` reads from `os.environ` via `os.getenv()`
5. Services use `config` object

**⚠️ This is temporary - lost on restart**

## How os.environ Gets the Details

### Scenario 1: Using .env File
```
.env file → load_dotenv() → os.environ → os.getenv() → config object
```

### Scenario 2: Using UI Form
```
UI form → st.session_state.config → os.environ (via initialize_services) → os.getenv() → config object
```

### Scenario 3: System Environment Variables
```
System env vars → os.environ (already set) → os.getenv() → config object
```

## Priority Order (if multiple sources exist)

1. **System environment variables** (highest priority)
2. **Values set in code** (`os.environ[key] = value`)
3. **`.env` file** (loaded by `load_dotenv()`)
4. **None** (if not found anywhere)

## To Use Config File Approach

1. **Create `.env` file** in project root:
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-token
   GENIE_SPACE_ID=your-space-id
   PATIENT_CATALOG=main
   PATIENT_SCHEMA=clinical
   SQL_WAREHOUSE_ID=your-warehouse-id
   VECTOR_CATALOG=main
   VECTOR_SCHEMA=vector_functions
   ```

2. **Add to .gitignore** (already done):
   ```
   .env
   ```

3. **That's it!** When app starts:
   - `load_dotenv()` loads `.env` → `os.environ`
   - `config.py` reads from `os.environ`
   - Services use config

4. **UI form becomes optional** - you can still use it to override, but `.env` is the source of truth

## Summary

**Yes, you ARE using a config file approach!**

- `config.py` is designed to read from `.env` file
- `load_dotenv()` is already there
- Just create `.env` file with your values
- The UI form is just an alternative way to set values (but not persisted)

**The config file is `.env`** - create it and it will work automatically!



