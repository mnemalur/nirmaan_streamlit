# Deploying to Databricks - Streamlit App

## Better Approach: Direct Deployment

Instead of creating a dummy app and replacing files, here's the **more efficient approach**:

## Option 1: Using Databricks Repos (Recommended)

1. **Create a Repo in Databricks:**
   - Go to Repos → Create Repo
   - Connect to your Git repository (or create a new one)
   - Clone your `streamlit_app` folder into the repo

2. **Create Streamlit App:**
   - In Databricks, go to **Apps** → **Create App**
   - Select **Streamlit** as the app type
   - Point to your repo path: `/Repos/your-username/your-repo/streamlit_app/app.py`
   - Databricks will automatically detect it's a Streamlit app

3. **Install Dependencies:**
   - The app will use the cluster's Python environment
   - Add `requirements.txt` to your repo or install via cluster libraries

## Option 2: Direct File Upload

1. **Create Streamlit App in Databricks:**
   - Go to **Apps** → **Create App** → **Streamlit**
   - This creates a workspace folder structure

2. **Upload Your Files:**
   - Use Databricks File System (DBFS) or Workspace Files
   - Upload entire `streamlit_app` folder structure:
     ```
     /Workspace/Users/your-email/streamlit_app/
       ├── app.py
       ├── config.py
       ├── requirements.txt
       └── services/
           ├── __init__.py
           ├── cohort_agent.py
           ├── cohort_manager.py
           ├── genie_service.py
           └── vector_search.py
     ```

3. **Point App to Your File:**
   - In the app settings, set path to `/Workspace/Users/your-email/streamlit_app/app.py`

## Option 3: Databricks Asset Bundles (Most Professional)

Use Databricks Asset Bundles for CI/CD deployment:

```yaml
# databricks.yml
resources:
  apps:
    cohort_assistant:
      type: streamlit
      path: app.py
      environment: PROD
```

## Important: Databricks-Specific Optimizations

When running **inside** Databricks, you can simplify authentication:

1. **Use Workspace Context** - No need for manual token/host configuration
2. **Use dbutils** - Access workspace utilities directly
3. **Automatic Credentials** - Current user's credentials are used automatically

## Next Steps

I'll create a Databricks-optimized version that:
- Uses `dbutils` for workspace access
- Automatically detects Databricks environment
- Simplifies configuration (no manual token entry needed)
- Uses workspace context for authentication

Would you like me to create the Databricks-optimized version?

