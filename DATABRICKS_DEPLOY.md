# Deploy to Databricks - Step by Step

## Method 1: Databricks Repos (Easiest)

1. **Open Databricks Workspace**
   - Go to your Databricks workspace
   - Navigate to **Repos** in the left sidebar

2. **Add Repository**
   - Click **Add Repo** button
   - Select **GitHub** as provider
   - Enter repository URL: `https://github.com/mnemalur/nirmaan_streamlit.git`
   - Select branch: `langgraph-integration`
   - Click **Create Repo**

3. **Access Your Code**
   - Your code will be available in `/Repos/your-username/nirmaan_streamlit`
   - All files from the branch will be there

## Method 2: Workspace Git Import

1. **In Databricks Workspace**
   - Go to **Workspace** → Your user folder
   - Right-click → **Import** → **Git**

2. **Import Settings**
   - Git repository URL: `https://github.com/mnemalur/nirmaan_streamlit.git`
   - Branch: `langgraph-integration`
   - Path: `/Users/your-username/nirmaan_streamlit` (or custom path)
   - Click **Import**

## Method 3: Using Databricks CLI (Advanced)

```bash
# Install Databricks CLI if not already installed
pip install databricks-cli

# Configure (if not done)
databricks configure --token

# Clone repo to Databricks workspace
databricks repos create \
  --url https://github.com/mnemalur/nirmaan_streamlit.git \
  --provider gitHub \
  --path /Repos/your-username/nirmaan_streamlit \
  --branch langgraph-integration
```

## After Pulling the Branch

1. **Set up Environment Variables**
   - Create `.env` file in Databricks (or use Databricks Secrets)
   - Copy from `env.example` and fill in your values

2. **Install Dependencies**
   - In Databricks notebook or terminal:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run Streamlit App**
   - If using Databricks SQL Warehouse:
     ```bash
     streamlit run app.py
     ```
   - Or deploy as Databricks App (if configured)

## Important Notes

✅ **Branch**: `langgraph-integration` (main is safe)  
✅ **Repository**: `https://github.com/mnemalur/nirmaan_streamlit.git`  
✅ **All M6 changes** are in this branch

## Updating Later

If you make changes and push to GitHub:
- In Databricks Repos, click **Pull** to get latest changes
- Or use: `databricks repos update --path /Repos/your-username/nirmaan_streamlit`

