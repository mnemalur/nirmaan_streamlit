# Simple Steps to Push to GitHub

## Step 1: Add GitHub Remote

Replace `YOUR_USERNAME` and `YOUR_REPO_NAME` with your actual GitHub details:

```bash
git remote add github https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
```

**OR if you haven't created the repo yet:**

1. Go to https://github.com/new
2. Create a new repository (don't initialize with README)
3. Copy the repository URL
4. Run: `git remote add github <YOUR_REPO_URL>`

## Step 2: Push to GitHub

```bash
git push -u github langgraph-integration
```

That's it! Your code will be on GitHub on the `langgraph-integration` branch (main is safe).

## If you need to update later:

```bash
git push github langgraph-integration
```

