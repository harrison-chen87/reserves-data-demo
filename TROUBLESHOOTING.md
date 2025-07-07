# 🔧 Troubleshooting Guide

## Common Issues and Solutions

### ❌ Environment Variable Error

**Error**: `Failed to write file to Databricks volume: Error: Databricks workspace URL must be provided via parameter or DATABRICKS_HOST environment variable`

**Cause**: The app cannot find your Databricks credentials.

**Solutions**:

#### Option 1: Quick Fix with .env File (Recommended)
```bash
# Run the interactive script to create .env file
./create_env.sh
```

#### Option 2: Manual .env File
Create a `.env` file in your project root:
```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
```

#### Option 3: Environment Variables
```bash
# Set environment variables in the same terminal where you run the app
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
python app.py
```

#### Option 4: Full Setup Script
```bash
# Run the comprehensive setup script
./setup_environment.sh
```

### ❌ Authentication Errors

**Error**: `Connection failed: Unauthorized`

**Solutions**:
- Verify your personal access token is correct
- Check that the token hasn't expired
- Ensure you copied the token correctly (no extra spaces)
- Generate a new token if needed

### ❌ Volume Access Errors

**Error**: `Volume does not exist` or `Permission denied`

**Solutions**:
- Verify the volume path format: `/Volumes/catalog/schema/volume/filename.xml`
- Check that the volume exists in your Databricks workspace
- Ensure you have these permissions:
  - `READ VOLUME` on the target volume
  - `WRITE VOLUME` on the target volume
  - `USE CATALOG` on the target catalog
  - `USE SCHEMA` on the target schema

### ❌ Import Errors

**Error**: `ModuleNotFoundError: No module named 'dash'`

**Solutions**:
```bash
# Make sure virtual environment is activated
source valnav-dash-env/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### ❌ Virtual Environment Issues

**Error**: Virtual environment not working

**Solutions**:
```bash
# Recreate virtual environment
rm -rf valnav-dash-env
python -m venv valnav-dash-env
source valnav-dash-env/bin/activate
pip install -r requirements.txt
```

### ❌ Port Already in Use

**Error**: `Address already in use`

**Solutions**:
```bash
# Find process using port 8050
lsof -i :8050

# Kill the process (replace PID with actual process ID)
kill -9 PID

# Or use a different port
python app.py --port 8051
```

### ❌ App Won't Start

**Error**: Various startup errors

**Solutions**:
1. Check that virtual environment is activated
2. Verify all dependencies are installed
3. Check for syntax errors in configuration files
4. Look at the terminal output for specific error messages

### ❌ Configuration Status Shows Warning

**Error**: App shows "⚠️ Not Configured"

**Solutions**:
1. **Check Environment Variables**:
   ```bash
   echo $DATABRICKS_HOST
   echo $DATABRICKS_TOKEN
   ```

2. **Verify .env File**:
   ```bash
   cat .env
   ```

3. **Test Configuration**:
   ```bash
   python -c "
   from databricks_client import validate_databricks_config
   valid, msg = validate_databricks_config()
   print(f'Valid: {valid}, Message: {msg}')
   "
   ```

## 📋 Environment Setup Checklist

- [ ] Virtual environment created and activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Databricks credentials configured (either .env file or environment variables)
- [ ] Configuration shows "✅ Ready" in the UI
- [ ] Volume path is correct format
- [ ] Required permissions on Databricks volume
- [ ] `.gitignore` file is protecting sensitive files from version control

## 🆘 Getting Help

If you're still having issues:

1. **Check the Terminal Output**: Look for specific error messages
2. **Verify Prerequisites**: Ensure all setup steps were completed
3. **Test Components**: Try running individual components to isolate the issue
4. **Check Permissions**: Verify your Databricks access and permissions

## 🔍 Debug Commands

```bash
# Check Python environment
which python
python --version

# Check installed packages
pip list

# Test imports
python -c "
import dash
from databricks_client import validate_databricks_config
print('✅ Imports successful')
"

# Check environment variables
env | grep DATABRICKS

# Test XML generation
python -c "
from xml_generator import generate_synthetic_valnav_xml
xml = generate_synthetic_valnav_xml(num_wells=2, num_facilities=1)
print(f'✅ Generated {len(xml)} characters')
"
```

## 🔒 Security Warning

**⚠️ NEVER commit credentials to version control!**

The project includes a `.gitignore` file that protects:
- `.env` files with your Databricks credentials
- Virtual environment directories
- Python cache files
- Temporary and log files

If you accidentally commit credentials:
1. **Immediately revoke** the exposed tokens in Databricks
2. Generate new personal access tokens
3. Update your local `.env` file with new credentials
4. Consider using `git filter-branch` or similar tools to remove credentials from git history 