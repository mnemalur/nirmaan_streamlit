"""
Sync script to upload code to Databricks workspace
Run this on your network machine after pulling from Git
"""

import os
import shutil
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

# Configuration
WORKSPACE_PATH = "/Workspace/Users/your-email/streamlit_app"  # Update with your path
LOCAL_PATH = Path(__file__).parent  # Current directory

# Files to upload
FILES_TO_UPLOAD = [
    "app.py",
    "config.py",
    "config_databricks.py",
    "requirements.txt",
    "README.md",
    "DEPLOY_DATABRICKS.md",
]

# Directories to upload
DIRS_TO_UPLOAD = [
    "services",
]


def get_workspace_client():
    """Initialize Databricks workspace client"""
    try:
        # Try to use default profile/credentials
        return WorkspaceClient()
    except Exception as e:
        print(f"Error initializing workspace client: {e}")
        print("Make sure you're authenticated to Databricks")
        print("Options:")
        print("  1. Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables")
        print("  2. Use databricks configure --token")
        print("  3. Use databricks auth login")
        raise


def upload_file(w: WorkspaceClient, local_path: Path, remote_path: str):
    """Upload a single file to Databricks workspace"""
    try:
        with open(local_path, 'rb') as f:
            content = f.read()
        
        # Create parent directory if needed
        parent_dir = os.path.dirname(remote_path)
        if parent_dir:
            try:
                w.workspace.mkdirs(path=parent_dir)
            except:
                pass  # Directory might already exist
        
        # Upload file
        w.workspace.upload(
            path=remote_path,
            content=content,
            format=ImportFormat.AUTO,
            overwrite=True
        )
        print(f"✅ Uploaded: {remote_path}")
        return True
    except Exception as e:
        print(f"❌ Error uploading {remote_path}: {e}")
        return False


def upload_directory(w: WorkspaceClient, local_dir: Path, remote_base: str):
    """Upload a directory recursively to Databricks workspace"""
    remote_dir = f"{remote_base}/{local_dir.name}"
    
    # Create remote directory
    try:
        w.workspace.mkdirs(path=remote_dir)
    except:
        pass
    
    # Upload all files in directory
    for file_path in local_dir.rglob("*"):
        if file_path.is_file():
            # Skip __pycache__ and other ignored files
            if "__pycache__" in str(file_path) or file_path.suffix == ".pyc":
                continue
            
            # Calculate relative path
            rel_path = file_path.relative_to(local_dir)
            remote_path = f"{remote_dir}/{str(rel_path).replace(os.sep, '/')}"
            
            upload_file(w, file_path, remote_path)


def main():
    """Main sync function"""
    print("🔄 Syncing code to Databricks...")
    print(f"Local path: {LOCAL_PATH}")
    print(f"Remote path: {WORKSPACE_PATH}")
    print()
    
    # Verify local files exist
    missing_files = []
    for file in FILES_TO_UPLOAD:
        if not (LOCAL_PATH / file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return
    
    # Initialize workspace client
    try:
        w = get_workspace_client()
    except Exception as e:
        print(f"❌ Failed to connect to Databricks: {e}")
        return
    
    # Create base directory
    try:
        w.workspace.mkdirs(path=WORKSPACE_PATH)
        print(f"✅ Created directory: {WORKSPACE_PATH}")
    except Exception as e:
        print(f"⚠️  Directory might already exist: {e}")
    
    # Upload files
    print("\n📤 Uploading files...")
    uploaded = 0
    failed = 0
    
    for file in FILES_TO_UPLOAD:
        local_file = LOCAL_PATH / file
        remote_file = f"{WORKSPACE_PATH}/{file}"
        if upload_file(w, local_file, remote_file):
            uploaded += 1
        else:
            failed += 1
    
    # Upload directories
    print("\n📤 Uploading directories...")
    for dir_name in DIRS_TO_UPLOAD:
        local_dir = LOCAL_PATH / dir_name
        if local_dir.exists():
            upload_directory(w, local_dir, WORKSPACE_PATH)
            print(f"✅ Uploaded directory: {dir_name}")
        else:
            print(f"⚠️  Directory not found: {dir_name}")
    
    print(f"\n✅ Sync complete!")
    print(f"   Uploaded: {uploaded} files")
    print(f"   Failed: {failed} files")
    print(f"\n📝 Next steps:")
    print(f"   1. Go to Databricks Apps")
    print(f"   2. Create/Update Streamlit app")
    print(f"   3. Point to: {WORKSPACE_PATH}/app.py")


if __name__ == "__main__":
    main()

