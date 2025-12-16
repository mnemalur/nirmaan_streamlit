"""
Check if Git is installed and accessible
Helps diagnose Git installation issues
"""

import subprocess
import sys
import os
from pathlib import Path

def check_git_in_path():
    """Check if git command is available"""
    try:
        result = subprocess.run(
            ["git", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, None
    except FileNotFoundError:
        return False, None
    except Exception as e:
        return False, str(e)

def check_git_installation_paths():
    """Check common Git installation paths on Windows"""
    common_paths = [
        r"C:\Program Files\Git\cmd\git.exe",
        r"C:\Program Files (x86)\Git\cmd\git.exe",
        r"C:\Users\{}\AppData\Local\Programs\Git\cmd\git.exe".format(os.getenv('USERNAME')),
    ]
    
    found_paths = []
    for path in common_paths:
        if Path(path).exists():
            found_paths.append(path)
    
    return found_paths

def main():
    """Main check function"""
    print("🔍 Checking Git Installation")
    print("=" * 50)
    
    # Check if git is in PATH
    print("\n1️⃣ Checking if Git is in PATH...")
    git_available, version = check_git_in_path()
    
    if git_available:
        print(f"  ✅ Git is installed and accessible!")
        print(f"     Version: {version}")
        print("\n📋 You can now run:")
        print("   git status")
        print("   python setup_git.py")
        return 0
    else:
        print("  ❌ Git is NOT found in PATH")
    
    # Check common installation paths
    print("\n2️⃣ Checking common installation paths...")
    found_paths = check_git_installation_paths()
    
    if found_paths:
        print(f"  ⚠️  Found Git installation(s) but not in PATH:")
        for path in found_paths:
            print(f"     {path}")
        print("\n  💡 To fix:")
        print("     1. Add Git to your PATH:")
        print(f"        Add: {Path(found_paths[0]).parent}")
        print("     2. Or restart PowerShell after installing Git")
        print("     3. Or use Git Bash instead of PowerShell")
    else:
        print("  ❌ Git is not installed")
        print("\n  💡 To install Git:")
        print("     1. Download from: https://git-scm.com/download/win")
        print("     2. Run the installer")
        print("     3. Restart PowerShell")
        print("\n  📖 See INSTALL_GIT.md for detailed instructions")
    
    # Check for Git Bash
    print("\n3️⃣ Checking for Git Bash...")
    git_bash_paths = [
        r"C:\Program Files\Git\git-bash.exe",
        r"C:\Program Files\Git\bin\bash.exe",
    ]
    
    git_bash_found = False
    for path in git_bash_paths:
        if Path(path).exists():
            print(f"  ✅ Git Bash found at: {path}")
            print("     You can use Git Bash instead of PowerShell")
            git_bash_found = True
            break
    
    if not git_bash_found:
        print("  ❌ Git Bash not found")
    
    print("\n" + "=" * 50)
    print("📋 Summary:")
    if git_available:
        print("  ✅ Git is ready to use!")
    elif found_paths:
        print("  ⚠️  Git is installed but not in PATH")
        print("     Add Git to PATH or use Git Bash")
    else:
        print("  ❌ Git is not installed")
        print("     Install Git from: https://git-scm.com/download/win")
    
    return 1 if not git_available else 0

if __name__ == "__main__":
    sys.exit(main())

