# Git Sync Workflow for Databricks Deployment

## Overview
This guide helps you sync code from your local development machine to your network environment for Databricks deployment.

## Workflow

### Step 1: Initial Git Setup (One-time)

#### On Your Local Machine (This Machine):

1. **Initialize Git Repository:**
   ```bash
   cd streamlit_app
   git init
   git add .
   git commit -m "Initial commit - Clinical Cohort Assistant Streamlit app"
   ```

2. **Create Remote Repository:**
   - Create a new repository on GitHub/GitLab/Bitbucket
   - Or use your organization's Git server
   - Note the repository URL

3. **Add Remote and Push:**
   ```bash
   git remote add origin <your-repo-url>
   git branch -M main
   git push -u origin main
   ```

### Step 2: Daily Development Workflow

#### On Your Local Machine (Make Changes):

1. **Make your code changes**
2. **Commit changes:**
   ```bash
   git add .
   git commit -m "Description of changes"
   git push origin main
   ```

#### On Your Network Machine (Sync & Deploy):

1. **Clone/Pull Latest Code:**
   ```bash
   # First time - Clone
   git clone <your-repo-url>
   cd streamlit_app
   
   # Subsequent times - Pull updates
   git pull origin main
   ```

2. **Upload to Databricks:**
   - Follow the Direct File Upload steps in `DEPLOY_DATABRICKS.md`
   - Or use the sync script (see below)

## Quick Sync Script

I'll create a `sync_to_databricks.py` script that you can run on your network machine to:
- Pull latest code from Git
- Upload files to Databricks workspace
- Verify deployment

## Alternative: Manual Sync Checklist

If you prefer manual sync:

### Local Machine Checklist:
- [ ] Code changes complete
- [ ] Tested locally
- [ ] Committed to Git
- [ ] Pushed to remote repository

### Network Machine Checklist:
- [ ] Pulled latest code from Git
- [ ] Verified files are updated
- [ ] Uploaded to Databricks workspace
- [ ] Tested app in Databricks

## File Structure for Git

Your repo should include:
```
streamlit_app/
├── .gitignore
├── README.md
├── DEPLOY_DATABRICKS.md
├── SYNC_WORKFLOW.md
├── requirements.txt
├── app.py
├── config.py
├── config_databricks.py
└── services/
    ├── __init__.py
    ├── cohort_agent.py
    ├── cohort_manager.py
    ├── genie_service.py
    └── vector_search.py
```

## Troubleshooting

### "Code not syncing"
- Check Git remote: `git remote -v`
- Verify push succeeded: `git log --oneline`
- Check network machine has latest: `git status`

### "Files missing in Databricks"
- Verify all files are committed (not in .gitignore)
- Check Databricks workspace path is correct
- Ensure file permissions allow upload

