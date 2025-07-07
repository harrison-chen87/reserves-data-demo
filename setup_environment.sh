#!/bin/bash
# ValNav Dash App Environment Setup Script

echo "🚀 Setting up ValNav Dash App Environment..."

# Activate virtual environment
echo "📦 Activating virtual environment..."
source valnav-dash-env/bin/activate

# Check if virtual environment is active
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ Virtual environment activated: $VIRTUAL_ENV"
else
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

# Prompt for Databricks configuration
echo ""
echo "🔧 Databricks Configuration Setup"
echo "Please provide your Databricks configuration:"

# Get Databricks host
echo ""
read -p "Enter your Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com): " DATABRICKS_HOST

# Get Databricks token
echo ""
read -s -p "Enter your Databricks personal access token: " DATABRICKS_TOKEN
echo ""

# Validate inputs
if [[ -z "$DATABRICKS_HOST" ]]; then
    echo "❌ Databricks host cannot be empty"
    exit 1
fi

if [[ -z "$DATABRICKS_TOKEN" ]]; then
    echo "❌ Databricks token cannot be empty"
    exit 1
fi

# Export environment variables
export DATABRICKS_HOST="$DATABRICKS_HOST"
export DATABRICKS_TOKEN="$DATABRICKS_TOKEN"

echo ""
echo "✅ Environment variables set successfully!"
echo "📍 DATABRICKS_HOST: $DATABRICKS_HOST"
echo "🔐 DATABRICKS_TOKEN: [hidden]"

# Test the installation
echo ""
echo "🧪 Testing installation..."

# Test Python imports
python -c "
try:
    import dash
    import dash_bootstrap_components as dbc
    from databricks.sdk import WorkspaceClient
    print('✅ All Python packages imported successfully')
    print(f'   - Dash version: {dash.__version__}')
    print(f'   - Dash Bootstrap Components version: {dbc.__version__}')
except ImportError as e:
    print(f'❌ Import error: {e}')
    exit(1)
"

# Test our modules
python -c "
try:
    from xml_generator import generate_synthetic_valnav_xml
    from databricks_client import validate_databricks_config
    print('✅ Custom modules imported successfully')
    
    # Test configuration validation
    is_valid, message = validate_databricks_config()
    if is_valid:
        print('✅ Databricks configuration is valid')
    else:
        print(f'⚠️  Configuration warning: {message}')
except Exception as e:
    print(f'❌ Module error: {e}')
    exit(1)
"

echo ""
echo "🎉 Setup complete! You can now run the application with:"
echo "   python app.py"
echo ""
echo "💡 Tips:"
echo "   - The app will be available at http://localhost:8050"
echo "   - Use 'deactivate' to exit the virtual environment"
echo "   - Re-run this script anytime to set up the environment"
echo ""
echo "🔧 To manually set environment variables in the future:"
echo "   export DATABRICKS_HOST=\"$DATABRICKS_HOST\""
echo "   export DATABRICKS_TOKEN=\"your-token-here\"" 