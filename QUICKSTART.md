# 🚀 Quick Start Guide

Follow these steps to get the ValNav Synthetic Data Generator up and running in a virtual environment.

## Step 1: Create and Activate Virtual Environment

```bash
# Create virtual environment
python -m venv valnav-dash-env

# Activate virtual environment
source valnav-dash-env/bin/activate  # On macOS/Linux
# OR
valnav-dash-env\Scripts\activate     # On Windows
```

## Step 2: Install Dependencies

```bash
# Install all required packages
pip install -r requirements.txt
```

## Step 3: Run the Application

**🔒 Security Enhancement**: Databricks credentials are now entered directly in the web UI for better security!

```bash
# Start the Dash app
python app.py
```

The app will be available at: **http://localhost:8050**

## Step 4: Configure Databricks in the Web UI

1. **Open the app** in your browser at `http://localhost:8050`
2. **Enter your Databricks Workspace URL** in the first field
3. **Enter your Personal Access Token** in the password field
4. **Click "Test Connection"** to verify your credentials
5. **Wait for "✅ Connected" status** before proceeding

## Step 5: Create Schema & Tables (Optional)

1. **Configure Schema Settings**: Enter your catalog name and schema name
2. **Click "Create Schema & Tables"** to create the complete data model
3. **Wait for success confirmation** - this creates 8 tables for your ValNav data
4. **Skip this step** if you want to use an existing schema or create tables manually

## 🔧 Getting Your Databricks Credentials

### Personal Access Token
1. Go to your Databricks workspace
2. Click on your profile icon (top right)
3. Select "User Settings"
4. Go to "Developer" tab
5. Click "Manage" next to "Access tokens"
6. Generate a new token
7. Copy the token and paste it in the web UI

### Workspace URL
Your workspace URL looks like:
- `https://your-workspace.cloud.databricks.com`
- `https://adb-1234567890123456.7.azuredatabricks.net` (Azure)
- `https://dbc-ab123456-789a.cloud.databricks.com` (AWS)

## 🎯 Quick Test

Test that everything is working:

```bash
# Test the installation
python -c "
from xml_generator import generate_synthetic_valnav_xml

# Generate sample XML
xml = generate_synthetic_valnav_xml(num_wells=5, num_facilities=2)
print(f'✅ Generated {len(xml)} characters of XML')
print('✅ App is ready - configure Databricks credentials in the web UI')
"
```

## 🖥️ Web UI Features

The app provides a modern, secure interface with:

- **🔐 Secure Credential Input**: Enter credentials directly in the web UI (not stored)
- **🧪 Connection Testing**: Verify your Databricks connection before generating data
- **🗄️ Schema Creation**: Create Unity Catalog schema and tables with one click
- **📊 Real-time Status**: See connection status and generation progress
- **🔒 Enhanced Security**: Credentials kept only in browser memory during session

### Status Indicators
- **✅ Connected**: Credentials verified, ready to generate data and create schemas
- **❌ Failed**: Connection test failed, check credentials
- **⚠️ Missing Info**: Enter both workspace URL and access token

### Available Actions
- **Test Connection**: Verify your Databricks credentials
- **Create Schema & Tables**: Set up the complete ValNav data model (8 tables)
- **Generate XML & Write to Volume**: Create synthetic data and save to Databricks volume
- **Preview XML**: See a sample of the generated XML structure

## 💡 Tips

- **Virtual Environment**: Always activate the virtual environment before running the app
- **Environment Variables**: Set them in each new terminal session, or add to your shell profile
- **Permissions**: Ensure you have `READ VOLUME` and `WRITE VOLUME` permissions on your Databricks volume
- **Volume Path**: Use format `/Volumes/catalog/schema/volume/filename.xml`
- **Security**: The `.gitignore` file protects your credentials from being committed to version control

## 🔄 Deactivating Virtual Environment

When you're done:
```bash
deactivate
```

## 📁 Project Structure

```
reserves-data-demo/
├── app.py                    # Main Dash application
├── xml_generator.py          # XML generation logic
├── databricks_client.py      # Databricks integration
├── requirements.txt          # Python dependencies
├── .gitignore               # Git ignore file (protects credentials)
├── create_env.sh            # Interactive .env creation script
├── setup_environment.sh      # Interactive setup script
├── env_example.txt          # Template for .env file
├── config_template.txt       # Configuration template
├── README.md                # Full documentation
├── QUICKSTART.md            # This file
├── TROUBLESHOOTING.md       # Troubleshooting guide
└── valnav-dash-env/         # Virtual environment (created)
```

## 🆘 Troubleshooting

**Import errors?**
- Make sure the virtual environment is activated
- Check that all packages installed correctly

**Databricks connection issues?**
- Verify your workspace URL and access token
- Check that your token hasn't expired
- Ensure you have proper volume permissions

**App won't start?**
- Check if port 8050 is already in use
- Look for error messages in the terminal

## 🎉 Ready to Go!

You're all set! Open http://localhost:8050 in your browser and start generating synthetic ValNav data! 