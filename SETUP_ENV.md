# Setting Up .env File

## Workflow

### On Your Local Machine (Development)

1. **Create `.env` file** (NOT pushed to Git):
   ```bash
   # Copy the example template (Windows PowerShell)
   copy env.example .env
   
   # Or on Linux/Mac:
   cp env.example .env
   
   # Or create manually
   # Edit .env and add your actual values
   ```

2. **Fill in your actual values** in `.env`:
   ```
   DATABRICKS_HOST=https://your-actual-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-actual-token
   GENIE_SPACE_ID=your-actual-space-id
   PATIENT_CATALOG=main
   PATIENT_SCHEMA=clinical
   SQL_WAREHOUSE_ID=your-actual-warehouse-id
   ```

3. **`.env` is already in `.gitignore`** - it won't be committed

### On Your Network Machine (After Pulling Repo)

1. **Pull the code:**
   ```bash
   git pull origin main
   ```

2. **Create `.env` file:**
   ```bash
   # Copy the example template (Windows PowerShell)
   copy env.example .env
   
   # Or on Linux/Mac:
   cp env.example .env
   
   # Or create manually
   ```

3. **Fill in your actual values** (same as local, or different if needed)

4. **Run the app** - it will automatically load from `.env`

## What Gets Pushed to Git

✅ **Pushed to Git:**
- `env.example` - Template with placeholder values
- `config.py` - Reads from `.env`
- All other code files

❌ **NOT Pushed to Git:**
- `.env` - Your actual credentials (in `.gitignore`)

## Why This Approach?

- ✅ **Security**: Real credentials never go in Git
- ✅ **Flexibility**: Each machine can have different values
- ✅ **Template**: `.env.example` shows what's needed
- ✅ **Easy Setup**: Just copy template and fill in values

## Quick Setup Commands

```bash
# On Windows PowerShell:
copy env.example .env

# On Linux/Mac:
cp env.example .env

# Then edit .env with your actual values
```

