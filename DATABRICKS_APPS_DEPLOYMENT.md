# Databricks Apps Deployment Guide

This guide explains how to deploy the Definitely-Not-ValNav Synthetic Data Generator to Databricks Apps.

## Overview

Databricks Apps is a serverless platform that allows you to deploy web applications directly within your Databricks environment. This application has been optimized to run natively on Databricks Apps with built-in authentication and Unity Catalog integration.

## Prerequisites

1. **Databricks Workspace**: You need access to a Databricks workspace that supports Databricks Apps
2. **Unity Catalog Access**: Your workspace should have Unity Catalog enabled
3. **Permissions**: You need permission to create apps in your Databricks workspace
4. **Databricks CLI** (optional): For command-line deployment

## Deployment Methods

### Method 1: Deploy via Databricks UI (Recommended)

1. **Navigate to Apps**
   - In your Databricks workspace, go to **Compute** → **Apps**
   - Click **Create App**

2. **Choose Custom App**
   - Select **Custom** instead of using a template
   - Name your app (e.g., `definitely-not-valnav-generator`)
   - Add a description

3. **Upload Application Files**
   - Upload all project files to your workspace:
     - `app.py` (main application)
     - `app.yaml` (configuration)
     - `requirements.txt` (dependencies)
     - `xml_generator.py` (XML generation logic)
     - `databricks_client.py` (Databricks integration)

4. **Deploy**
   - Click **Deploy** to start the deployment process
   - Wait for the application to build and start (usually 2-3 minutes)

### Method 2: Deploy via Databricks CLI

1. **Install Databricks CLI**
   ```bash
   pip install databricks-cli
   ```

2. **Configure CLI**
   ```bash
   databricks configure --token
   ```

3. **Create App Directory**
   ```bash
   mkdir my-databricks-app
   cd my-databricks-app
   ```

4. **Copy Files**
   Copy all project files to the app directory

5. **Deploy**
   ```bash
   databricks apps deploy --source-dir . --app-name definitely-not-valnav-generator
   ```

## Configuration Files

### app.yaml
The application uses the following configuration:

```yaml
command: ["python", "app.py"]

env:
  # Dash/Flask specific environment variables
  DASH_ENV: "production"
  
  # Databricks Apps will automatically set DATABRICKS_APP_PORT
  # Our app will read this to bind to the correct port
```

### requirements.txt
Dependencies are pinned to exact versions for stability:

```
# Core Dash dependencies
dash==2.17.1
dash-bootstrap-components==1.5.0
plotly==5.17.0

# Databricks SDK
databricks-sdk==0.20.0
databricks-sql-connector==3.0.0

# Additional dependencies
requests==2.31.0
urllib3==2.0.7

# Optional: For better error handling and logging
python-dotenv==1.0.0
```

## Key Features for Databricks Apps

### 1. Automatic Authentication
- No manual credential input required
- Uses Databricks Apps runtime authentication
- Secure service principal access to Unity Catalog

### 2. Environment Detection
The application automatically detects when running in Databricks Apps:

```python
# Check if running in Databricks Apps environment
in_databricks_apps = os.getenv('DATABRICKS_APP_PORT') is not None
```

### 3. Port Configuration
Automatically binds to the correct port:

```python
port = int(os.environ.get("DATABRICKS_APP_PORT", 8050))
app.run_server(host="0.0.0.0", port=port)
```

## Unity Catalog Setup

The application requires access to Unity Catalog resources:

### Required Permissions
The app's service principal needs these permissions:

1. **Catalog Level**:
   - `USE CATALOG` on your target catalog

2. **Schema Level**:
   - `CREATE TABLE` on your target schema
   - `USE SCHEMA` on your target schema

3. **Volume Level**:
   - `READ VOLUME` on your target volume
   - `WRITE VOLUME` on your target volume

### Default Configuration
The app comes with these default settings (customize as needed):

- **Catalog**: `harrison_chen_catalog`
- **Schema**: `definitely_not_valnav_bronze`
- **Volume Path**: `/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/`

## Application Features

### 1. Synthetic Data Generation
- Generate up to 10,000 wells
- Create facilities, scenarios, price decks
- Configurable data model parameters

### 2. XML File Creation
- Generates XML files in ValNav format
- Writes directly to Unity Catalog volumes
- Automatic file validation

### 3. Schema & Table Creation
- Creates Unity Catalog schema automatically
- Generates 15 core tables for energy data
- Populates tables with generated data

### 4. 1990s UI Theme
- Authentic Windows 95/98 aesthetic
- Enterprise software styling
- Retro color scheme and controls

## Monitoring & Logs

### Viewing Logs
1. Go to your app in the Databricks UI
2. Click on the **Logs** tab
3. Or append `/logz` to your app URL

### Log Output
The application logs key events:
- Startup messages
- Authentication status
- XML generation progress
- Database operations
- Error messages

## Troubleshooting

### Common Issues

1. **App Won't Start**
   - Check requirements.txt for dependency conflicts
   - Verify app.yaml syntax
   - Review startup logs

2. **Permission Errors**
   - Ensure the app's service principal has required Unity Catalog permissions
   - Check catalog/schema/volume accessibility

3. **File Upload Failures**
   - Verify volume path format: `/Volumes/catalog/schema/volume/`
   - Check volume exists and is accessible
   - Ensure sufficient storage quota

### Performance Optimization

1. **Cold Start**: First access may take 2-3 minutes
2. **Compute**: Uses serverless compute for optimal performance
3. **Scaling**: Automatically scales based on usage

## Security

### Built-in Security Features
- **Authentication**: Automatic SSO integration
- **Authorization**: Unity Catalog permissions
- **Network**: Secure Databricks networking
- **Data**: No data leaves your environment

### Access Control
1. **App Access**: Controlled via workspace permissions
2. **Data Access**: Governed by Unity Catalog
3. **Sharing**: Secure URL-based access

## Support

### Documentation
- [Databricks Apps Documentation](https://docs.databricks.com/dev-tools/databricks-apps/)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/)

### Best Practices
1. Pin dependency versions
2. Use meaningful app names
3. Monitor resource usage
4. Regular security reviews
5. Test with sample data first

## Migration from Localhost

If migrating from local development:

1. **Remove Manual Authentication**: No need for workspace URL/token inputs
2. **Update Configuration**: Use default Unity Catalog settings
3. **Test Deployment**: Start with small data sets
4. **Monitor Performance**: Check logs and resource usage

The application will automatically detect the environment and switch between local development mode (requiring credentials) and Databricks Apps mode (using runtime authentication). 