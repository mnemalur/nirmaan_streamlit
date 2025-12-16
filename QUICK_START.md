# Quick Start Guide - Git Sync & Databricks Deployment

## For Your Workflow (Local → Network → Databricks)

### Step 1: Set Up Git Repository (One-time)

**On your local machine:**

```bash
# Navigate to your project
cd streamlit_app

# Initialize Git (if not already done)
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit"

# Create repo on GitHub/GitLab and add remote
git remote add origin <your-repo-url>
git branch -M main
git push -u origin main
```

### Step 2: Daily Development Workflow

**On your local machine (where you make changes):**

```bash
# Make your code changes...

# Run pre-push checklist (recommended)
python pre_push_checklist.py

# OR manually clean up unwanted files
python cleanup_before_push.py

# Stage changes
git add .

# Commit with descriptive message
git commit -m "Added LangGraph agent for conversational state"

# Push to remote
git push origin main
```

**On your network machine (where you deploy):**

```bash
# Navigate to project folder
cd streamlit_app

# Pull latest changes
git pull origin main

# Verify files updated
git status

# Upload to Databricks (choose one method)
```

### Step 3: Upload to Databricks

#### Option A: Using Sync Script (Recommended)

```bash
# Install databricks-cli if needed
pip install databricks-cli

# Authenticate
databricks auth login
# OR set environment variables:
# export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
# export DATABRICKS_TOKEN=your-token

# Run sync script
python sync_to_databricks.py
```

#### Option B: Manual Upload via Databricks UI

1. Go to Databricks workspace
2. Navigate to **Workspace** → **Users** → **your-email**
3. Create folder `streamlit_app` if it doesn't exist
4. Upload files manually:
   - `app.py`
   - `config.py`
   - `config_databricks.py`
   - `requirements.txt`
   - `services/` folder (all files)

#### Option C: Using Databricks CLI

```bash
# Install databricks-cli
pip install databricks-cli

# Configure
databricks configure --token

# Upload files
databricks workspace import_dir . /Workspace/Users/your-email/streamlit_app --overwrite
```

### Step 4: Create Streamlit App in Databricks

1. Go to **Apps** → **Create App**
2. Select **Streamlit**
3. Set path to: `/Workspace/Users/your-email/streamlit_app/app.py`
4. Configure cluster/library dependencies
5. Launch app

## File Checklist

Make sure these files are in Git (not ignored):

- ✅ `app.py`
- ✅ `config.py`
- ✅ `config_databricks.py`
- ✅ `requirements.txt`
- ✅ `services/__init__.py`
- ✅ `services/cohort_agent.py`
- ✅ `services/cohort_manager.py`
- ✅ `services/genie_service.py`
- ✅ `services/vector_search.py`

## Troubleshooting

### Git Issues

**"Remote repository not found"**
- Check repository URL: `git remote -v`
- Verify you have access to the repo

**"Changes not syncing"**
- Check if files are committed: `git status`
- Verify push succeeded: `git log --oneline -5`

### Databricks Upload Issues

**"Authentication failed"**
- Run: `databricks auth login`
- Or set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` env vars

**"Files not appearing in workspace"**
- Check workspace path is correct
- Verify you have write permissions
- Check file paths use forward slashes `/`

## Quick Commands Reference

```bash
# Local machine - Push changes
git add . && git commit -m "Your message" && git push

# Network machine - Pull and sync
git pull && python sync_to_databricks.py

# Check Git status
git status
git log --oneline -5

# Verify Databricks connection
databricks workspace ls /Workspace/Users/your-email/
```

