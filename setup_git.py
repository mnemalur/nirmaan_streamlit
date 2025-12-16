"""
Setup script to initialize Git repository for syncing
Run this once on your local machine to set up Git sync
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd, check=True):
    """Run a shell command"""
    print(f"Running: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return False

def check_git_installed():
    """Check if Git is installed"""
    return run_command("git --version", check=False)

def initialize_git():
    """Initialize Git repository"""
    print("\n📦 Initializing Git repository...")
    
    # Check if already initialized
    if Path(".git").exists():
        print("⚠️  Git repository already initialized")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return False
    
    # Initialize Git
    if not run_command("git init"):
        return False
    
    # Add all files
    print("\n📝 Adding files to Git...")
    if not run_command("git add ."):
        return False
    
    # Initial commit
    print("\n💾 Creating initial commit...")
    if not run_command('git commit -m "Initial commit - Clinical Cohort Assistant Streamlit app"'):
        return False
    
    print("\n✅ Git repository initialized!")
    return True

def setup_remote():
    """Set up remote repository"""
    print("\n🔗 Setting up remote repository...")
    print("\nYou need to:")
    print("1. Create a repository on GitHub/GitLab/Bitbucket")
    print("2. Copy the repository URL")
    print("3. Enter it when prompted")
    
    repo_url = input("\nEnter your Git repository URL (or press Enter to skip): ").strip()
    
    if not repo_url:
        print("⚠️  Skipping remote setup. You can add it later with:")
        print("   git remote add origin <your-repo-url>")
        return False
    
    # Add remote
    if run_command(f'git remote add origin {repo_url}'):
        print(f"✅ Remote added: {repo_url}")
        
        # Set main branch
        run_command("git branch -M main")
        
        # Push
        push = input("\nPush to remote now? (y/n): ")
        if push.lower() == 'y':
            if run_command("git push -u origin main"):
                print("✅ Code pushed to remote!")
                return True
            else:
                print("⚠️  Push failed. You can push later with: git push -u origin main")
    
    return True

def main():
    """Main setup function"""
    print("🚀 Git Setup for Streamlit App Sync")
    print("=" * 50)
    
    # Check Git installation
    if not check_git_installed():
        print("❌ Git is not installed. Please install Git first.")
        print("   Download from: https://git-scm.com/downloads")
        sys.exit(1)
    
    print("✅ Git is installed")
    
    # Initialize Git
    if not initialize_git():
        print("❌ Failed to initialize Git repository")
        sys.exit(1)
    
    # Setup remote
    setup_remote()
    
    print("\n" + "=" * 50)
    print("✅ Setup complete!")
    print("\n📋 Next steps:")
    print("1. Make your code changes")
    print("2. Commit: git add . && git commit -m 'Your message'")
    print("3. Push: git push origin main")
    print("4. On network machine: git pull origin main")
    print("\nSee QUICK_START.md for detailed workflow")

if __name__ == "__main__":
    main()

