"""
Pre-push checklist - Run this before pushing to Git
Ensures code is clean and ready for sync
"""

import subprocess
import sys
from pathlib import Path

def check_git_status():
    """Check if there are uncommitted changes"""
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True
        )
        return result.stdout.strip()
    except Exception as e:
        print(f"⚠️  Could not check Git status: {e}")
        return None

def check_unwanted_files():
    """Check for unwanted files"""
    unwanted = []
    unwanted_files = [
        "app_databricks.py",
        "__pycache__",
        "*.pyc",
    ]
    
    for pattern in unwanted_files:
        if "*" in pattern:
            for file in Path(".").rglob(pattern):
                unwanted.append(file)
        else:
            if Path(pattern).exists():
                unwanted.append(Path(pattern))
    
    return unwanted

def run_cleanup():
    """Run cleanup script"""
    print("🧹 Running cleanup...")
    try:
        result = subprocess.run(
            [sys.executable, "cleanup_before_push.py"],
            text=True
        )
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error running cleanup: {e}")
        return False

def main():
    """Main checklist"""
    print("✅ Pre-Push Checklist")
    print("=" * 50)
    
    all_checks_passed = True
    
    # Check 1: Git status
    print("\n1️⃣ Checking Git status...")
    uncommitted = check_git_status()
    if uncommitted:
        print("  ⚠️  You have uncommitted changes:")
        for line in uncommitted.split('\n'):
            if line.strip():
                print(f"     {line}")
        print("\n  💡 Commit changes before pushing:")
        print("     git add .")
        print("     git commit -m 'Your message'")
    else:
        print("  ✅ All changes committed")
    
    # Check 2: Unwanted files
    print("\n2️⃣ Checking for unwanted files...")
    unwanted = check_unwanted_files()
    if unwanted:
        print(f"  ⚠️  Found {len(unwanted)} unwanted files:")
        for file in unwanted[:5]:  # Show first 5
            print(f"     {file}")
        if len(unwanted) > 5:
            print(f"     ... and {len(unwanted) - 5} more")
        
        response = input("\n  🧹 Run cleanup script? (y/n): ").strip().lower()
        if response == 'y':
            if not run_cleanup():
                all_checks_passed = False
    else:
        print("  ✅ No unwanted files found")
    
    # Check 3: Verify sync readiness
    print("\n3️⃣ Verifying sync readiness...")
    try:
        result = subprocess.run(
            [sys.executable, "verify_sync.py"],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print("  ⚠️  Some verification checks failed")
            print(result.stdout)
            all_checks_passed = False
        else:
            print("  ✅ All files verified")
    except Exception as e:
        print(f"  ⚠️  Could not run verification: {e}")
    
    # Final summary
    print("\n" + "=" * 50)
    if all_checks_passed:
        print("✅ All checks passed! Ready to push.")
        print("\n📋 Push to Git:")
        print("   git push origin main")
        return 0
    else:
        print("❌ Some checks failed. Please fix issues above.")
        print("\n💡 Quick fixes:")
        print("   - Run cleanup: python cleanup_before_push.py")
        print("   - Verify files: python verify_sync.py")
        print("   - Commit changes: git add . && git commit -m 'Your message'")
        return 1

if __name__ == "__main__":
    sys.exit(main())

