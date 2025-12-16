# Installing Git on Windows

## Quick Installation

### Option 1: Download Git for Windows (Recommended)

1. **Download Git:**
   - Go to: https://git-scm.com/download/win
   - Download the latest version (64-bit)
   - Run the installer

2. **Installation Settings:**
   - Use default settings (recommended)
   - Make sure "Git from the command line and also from 3rd-party software" is selected
   - Complete the installation

3. **Restart PowerShell:**
   - Close and reopen PowerShell/Command Prompt
   - Git should now be available

4. **Verify Installation:**
   ```powershell
   git --version
   ```
   Should show: `git version 2.x.x`

### Option 2: Install via Winget (Windows Package Manager)

If you have Windows 10/11 with winget:

```powershell
winget install --id Git.Git -e --source winget
```

### Option 3: Install via Chocolatey

If you have Chocolatey installed:

```powershell
choco install git
```

## After Installation

1. **Restart your terminal/PowerShell**
2. **Configure Git (one-time setup):**
   ```powershell
   git config --global user.name "Your Name"
   git config --global user.email "your.email@example.com"
   ```

3. **Verify it works:**
   ```powershell
   git --version
   git status
   ```

## Troubleshooting

### Git still not found after installation

1. **Check if Git is installed:**
   - Look for "Git Bash" in Start Menu
   - If found, Git is installed but not in PATH

2. **Add Git to PATH manually:**
   - Git is usually installed at: `C:\Program Files\Git\cmd\`
   - Add this to your system PATH:
     - Press `Win + X` → System → Advanced system settings
     - Click "Environment Variables"
     - Under "System variables", find "Path" → Edit
     - Add: `C:\Program Files\Git\cmd\`
     - Click OK and restart PowerShell

3. **Or use Git Bash instead:**
   - Open "Git Bash" from Start Menu
   - It has Git pre-configured

## Alternative: Use GitHub Desktop

If you prefer a GUI:
- Download: https://desktop.github.com/
- It includes Git and provides a visual interface
- You can still use command line Git after installing

## Next Steps After Installing Git

Once Git is installed, run:

```powershell
# Navigate to your project
cd C:\Users\sunda\OneDrive\Documents\streamlit_app

# Initialize Git (if not already done)
python setup_git.py

# Or manually:
git init
git add .
git commit -m "Initial commit"
```

