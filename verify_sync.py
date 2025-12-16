"""
Verify that all necessary files are ready for Git sync
Run this before pushing to ensure nothing is missing
"""

from pathlib import Path
import sys

# Required files and directories
REQUIRED_FILES = [
    "app.py",
    "config.py",
    "config_databricks.py",
    "requirements.txt",
    "README.md",
    "DEPLOY_DATABRICKS.md",
    "SYNC_WORKFLOW.md",
    "QUICK_START.md",
    ".gitignore",
]

REQUIRED_DIRS = [
    "services",
]

REQUIRED_SERVICE_FILES = [
    "services/__init__.py",
    "services/cohort_agent.py",
    "services/cohort_manager.py",
    "services/genie_service.py",
    "services/vector_search.py",
]

def check_files():
    """Check if all required files exist"""
    print("🔍 Checking required files...")
    missing = []
    
    for file in REQUIRED_FILES:
        if not Path(file).exists():
            missing.append(file)
            print(f"  ❌ Missing: {file}")
        else:
            print(f"  ✅ Found: {file}")
    
    return missing

def check_directories():
    """Check if all required directories exist"""
    print("\n🔍 Checking required directories...")
    missing = []
    
    for dir_name in REQUIRED_DIRS:
        if not Path(dir_name).exists():
            missing.append(dir_name)
            print(f"  ❌ Missing: {dir_name}")
        else:
            print(f"  ✅ Found: {dir_name}")
    
    return missing

def check_service_files():
    """Check if all service files exist"""
    print("\n🔍 Checking service files...")
    missing = []
    
    for file in REQUIRED_SERVICE_FILES:
        if not Path(file).exists():
            missing.append(file)
            print(f"  ❌ Missing: {file}")
        else:
            print(f"  ✅ Found: {file}")
    
    return missing

def check_git_status():
    """Check Git repository status"""
    print("\n🔍 Checking Git status...")
    
    if not Path(".git").exists():
        print("  ⚠️  Git repository not initialized")
        print("     Run: python setup_git.py")
        return False
    
    import subprocess
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            capture_output=True,
            text=True
        )
        
        if result.stdout.strip():
            print("  ⚠️  Uncommitted changes:")
            for line in result.stdout.strip().split('\n'):
                print(f"     {line}")
            print("\n  💡 Commit changes before syncing:")
            print("     git add .")
            print("     git commit -m 'Your message'")
        else:
            print("  ✅ All changes committed")
        
        return True
    except Exception as e:
        print(f"  ⚠️  Could not check Git status: {e}")
        return False

def main():
    """Main verification function"""
    print("🔎 Verifying Sync Readiness")
    print("=" * 50)
    
    all_good = True
    
    # Check files
    missing_files = check_files()
    if missing_files:
        all_good = False
    
    # Check directories
    missing_dirs = check_directories()
    if missing_dirs:
        all_good = False
    
    # Check service files
    missing_services = check_service_files()
    if missing_services:
        all_good = False
    
    # Check Git status
    git_ok = check_git_status()
    
    print("\n" + "=" * 50)
    if all_good and git_ok:
        print("✅ All checks passed! Ready to sync.")
        print("\n📋 Next steps:")
        print("1. Commit changes: git add . && git commit -m 'Your message'")
        print("2. Push to remote: git push origin main")
        print("3. On network machine: git pull origin main")
        return 0
    else:
        print("❌ Some checks failed. Please fix issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

