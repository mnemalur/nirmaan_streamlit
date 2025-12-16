"""
Cleanup script to remove unwanted files before pushing to Git
Run this before committing to keep your repository clean
"""

import os
import shutil
from pathlib import Path
import sys

# Files that are likely unwanted (templates, incomplete files, etc.)
UNWANTED_FILES = [
    "app_databricks.py",  # Incomplete template file
    "run.bat",  # Windows-specific, not needed in repo
    "run.sh",  # Shell script, not needed if using Python scripts
]

# Files to keep but might want to review
REVIEW_FILES = [
    "config_databricks.py",  # Useful for Databricks deployment
    "sync_to_databricks.py",  # Useful for deployment
    "setup_git.py",  # Useful for initial setup
    "verify_sync.py",  # Useful for verification
]

# Directories to check for unwanted files
UNWANTED_PATTERNS = [
    "__pycache__",
    "*.pyc",
    ".pytest_cache",
    ".mypy_cache",
    ".ipynb_checkpoints",
    "*.egg-info",
    ".DS_Store",
    "Thumbs.db",
]


def find_unwanted_files():
    """Find files that match unwanted patterns"""
    unwanted = []
    
    for pattern in UNWANTED_PATTERNS:
        if "*" in pattern:
            # Glob pattern
            for file in Path(".").rglob(pattern):
                if file.is_file():
                    unwanted.append(file)
        else:
            # Directory pattern
            for dir_path in Path(".").rglob(pattern):
                if dir_path.is_dir():
                    unwanted.append(dir_path)
    
    return unwanted


def list_unwanted_files():
    """List all potentially unwanted files"""
    unwanted = []
    
    # Check explicit unwanted files
    for file in UNWANTED_FILES:
        if Path(file).exists():
            unwanted.append(Path(file))
    
    # Check pattern-based unwanted files
    unwanted.extend(find_unwanted_files())
    
    return unwanted


def remove_file(file_path):
    """Remove a file or directory"""
    try:
        if file_path.is_dir():
            shutil.rmtree(file_path)
            print(f"  ✅ Removed directory: {file_path}")
        else:
            file_path.unlink()
            print(f"  ✅ Removed file: {file_path}")
        return True
    except Exception as e:
        print(f"  ❌ Error removing {file_path}: {e}")
        return False


def update_gitignore():
    """Ensure .gitignore is up to date"""
    gitignore_path = Path(".gitignore")
    
    if not gitignore_path.exists():
        print("⚠️  .gitignore not found, creating one...")
        return False
    
    # Check if unwanted patterns are in .gitignore
    with open(gitignore_path, 'r') as f:
        gitignore_content = f.read()
    
    missing_patterns = []
    for pattern in UNWANTED_PATTERNS:
        if pattern not in gitignore_content:
            missing_patterns.append(pattern)
    
    if missing_patterns:
        print(f"\n⚠️  Adding missing patterns to .gitignore:")
        with open(gitignore_path, 'a') as f:
            f.write("\n# Added by cleanup script\n")
            for pattern in missing_patterns:
                f.write(f"{pattern}\n")
                print(f"  ✅ Added: {pattern}")
        return True
    
    return False


def main():
    """Main cleanup function"""
    print("🧹 Cleanup Before Git Push")
    print("=" * 50)
    
    # Find unwanted files
    unwanted_files = list_unwanted_files()
    
    if not unwanted_files:
        print("✅ No unwanted files found!")
        update_gitignore()
        return 0
    
    # List unwanted files
    print(f"\n📋 Found {len(unwanted_files)} potentially unwanted files:")
    for i, file in enumerate(unwanted_files, 1):
        file_type = "DIR" if file.is_dir() else "FILE"
        print(f"  {i}. [{file_type}] {file}")
    
    # Ask for confirmation
    print("\n⚠️  These files will be PERMANENTLY DELETED")
    response = input("\nRemove these files? (yes/no): ").strip().lower()
    
    if response not in ['yes', 'y']:
        print("❌ Cleanup cancelled")
        return 1
    
    # Remove files
    print("\n🗑️  Removing files...")
    removed = 0
    failed = 0
    
    for file in unwanted_files:
        if remove_file(file):
            removed += 1
        else:
            failed += 1
    
    # Update .gitignore
    print("\n📝 Updating .gitignore...")
    update_gitignore()
    
    # Summary
    print("\n" + "=" * 50)
    print(f"✅ Cleanup complete!")
    print(f"   Removed: {removed} files/directories")
    if failed > 0:
        print(f"   Failed: {failed} files/directories")
    
    print("\n📋 Next steps:")
    print("   1. Review changes: git status")
    print("   2. Stage changes: git add .")
    print("   3. Commit: git commit -m 'Your message'")
    print("   4. Push: git push origin main")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

