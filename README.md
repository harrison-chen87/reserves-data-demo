# ValNav Synthetic Data Generator

A Dash web application for generating synthetic ValNav well data and writing it directly to Databricks Unity Catalog volumes.

## Features

- **🔐 Secure Credential Management**: Enter Databricks credentials directly in the web UI (no storage)
- **🧪 Connection Testing**: Verify Databricks connectivity before data generation
- **🗄️ Schema & Table Creation**: Create Unity Catalog schema and tables with one click
- **🎛️ Interactive Web UI**: Easy-to-use interface for specifying data generation parameters
- **⚙️ Comprehensive Data Model**: Full ValNav data model with 19+ configurable entity types
- **🔧 Flexible Configuration**: Control count for each entity type (0 to max) including wells, facilities, scenarios, companies, fiscal regimes, meter stations, and more
- **🔗 Databricks Integration**: Direct integration with Databricks Unity Catalog volumes
- **👀 Real-time Preview**: Preview generated XML structure before writing to volume
- **📊 Progress Tracking**: Live progress updates during data generation and upload
- **✅ Validation**: Input validation and error handling for robust operation

## Quick Start

### 1. Set Up Virtual Environment

```bash
python -m venv valnav-dash-env
source valnav-dash-env/bin/activate  # On macOS/Linux
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Application

```bash
python app.py
```

### 4. Configure in Web UI

1. Open `http://localhost:8050` in your browser
2. Enter your Databricks workspace URL and personal access token
3. Click "Test Connection" to verify credentials
4. Generate synthetic data once connected!

## Configuration

### Databricks Setup

#### Personal Access Token
1. Go to your Databricks workspace
2. Click on your profile icon in the top right
3. Select "User Settings"
4. Go to "Developer" tab
5. Click "Manage" next to "Access tokens"
6. Generate a new token
7. Copy the token and set it as `DATABRICKS_TOKEN`

#### Volume Setup
Ensure your Databricks volume exists and you have the required permissions:
- `READ VOLUME` on the target volume
- `WRITE VOLUME` on the target volume
- `USE CATALOG` on the target catalog
- `USE SCHEMA` on the target schema

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | `https://your-workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Your personal access token | `dapi1234567890abcdef...` |

## Usage

### Data Generation Parameters

The application now supports the complete ValNav data model with comprehensive configuration options:

#### 🏗️ Core Entities
- **Wells**: Well entities (0-10,000)
- **Facilities**: Processing facilities (0-1,000)
- **Scenarios**: Economic scenarios (0-50)
- **Price Decks**: Price forecasts (0-20)

#### 💰 Economic & Reference Data
- **Companies**: Operating companies (0-100)
- **Countries**: Countries/regions (0-50)
- **Currencies**: Currency types (0-30)
- **Fiscal Regimes**: Tax/royalty systems (0-20)

#### 🚧 Infrastructure & Operations
- **Meter Stations**: Measurement points (0-200)
- **Transportation Areas**: Transport regions (0-50)
- **Type Wells**: Well templates (0-100)
- **Tax Pools**: Tax calculation pools (0-50)

#### ⚙️ Advanced Configuration
- **Batch Definitions**: Batch processing configs (0-30)
- **Change Record Categories**: Change tracking types (0-20)
- **Custom Data Fields**: User-defined fields (0-50)
- **Hierarchies**: Organizational structures (0-10)

#### 📈 Production & Scheduling
- **Production History**: Months of history per well (1-120)
- **Well Schedule Coverage**: Percentage of wells with schedules (10-100%)
- **Rollups**: Summary aggregations (0-20)

*Note: Setting any entity count to 0 will exclude it from the generated data model*

### Databricks Configuration

- **Volume Path**: Path to your Databricks volume (e.g., `/Volumes/catalog/schema/volume/`)
- **File Name**: Name of the XML file to create
- **Catalog Name**: Unity Catalog name
- **Schema Name**: Schema name for bronze layer data

### Generated XML Structure

The application generates comprehensive XML with the complete ValNav data model structure:

```xml
<ProjectData>
  <PriceDecks>
    <PriceDeck>
      <PriceCommodity>
        <AnnualPrice/>
      </PriceCommodity>
    </PriceDeck>
  </PriceDecks>
  <Scenarios>
    <Scenario>
      <Description/>
    </Scenario>
  </Scenarios>
  <Facilities>
    <Facility>
      <Capacity/>
      <Status/>
    </Facility>
  </Facilities>
  <WellsAndGroups>
    <Well>
      <WellboreData/>
      <ReservoirData/>
    </Well>
    <Group>
      <MemberWellID/>
    </Group>
  </WellsAndGroups>
  <BulkWellSchedules>
    <WellSchedule>
      <ProductionEntry/>
    </WellSchedule>
  </BulkWellSchedules>
  <Companies>
    <Company/>
  </Companies>
  <Currencies>
    <Currency/>
  </Currencies>
  <Countries>
    <Country/>
  </Countries>
  <FiscalRegimes>
    <FiscalRegime/>
  </FiscalRegimes>
  <MeterStations>
    <MeterStation/>
  </MeterStations>
  <TransportationAreas>
    <TransportationArea/>
  </TransportationAreas>
  <TypeWells>
    <TypeWell/>
  </TypeWells>
  <TaxPools>
    <TaxPool/>
  </TaxPools>
  <BatchDefinitions>
    <BatchDefinition/>
  </BatchDefinitions>
  <ChangeRecordCategories>
    <ChangeRecordCategory/>
  </ChangeRecordCategories>
  <CustomDataFields>
    <CustomDataField/>
  </CustomDataFields>
  <Hierarchies>
    <Hierarchy/>
  </Hierarchies>
  <Rollups>
    <Rollup/>
  </Rollups>
</ProjectData>
```

*Note: Only configured entities (count > 0) will be included in the generated XML*

### Schema & Tables Creation

The app can automatically create the Unity Catalog schema and tables with the **"Create Schema & Tables"** button.

#### Tables Created:

The application creates a comprehensive set of tables matching the complete ValNav data model:

##### Core Entity Tables:
1. **`wells`**: Well data with IDs, names, types, facilities, production rates, wellbore and reservoir data
2. **`facilities`**: Facility data with locations, capacities, statuses, and coordinates
3. **`scenarios`**: Scenario metadata with price deck references and descriptions
4. **`price_decks_metadata`**: Price deck information with currency references
5. **`price_decks_annual_prices`**: Annual commodity prices by year and commodity type
6. **`bulk_well_schedules`**: Historical production data with oil, gas, and water rates

##### Reference Data Tables:
7. **`currencies`**: Currency reference data with symbols
8. **`countries`**: Country reference data
9. **`companies`**: Operating companies data
10. **`fiscal_regimes`**: Fiscal regimes and tax structures

##### Infrastructure & Operations Tables:
11. **`meter_stations`**: Meter stations for flow measurement
12. **`transportation_areas`**: Transportation areas and logistics
13. **`type_wells`**: Type well templates and drilling parameters
14. **`tax_pools`**: Tax pools for depletion and depreciation

##### Advanced Configuration Tables:
15. **`batch_definitions`**: Batch processing definitions
16. **`change_record_categories`**: Change record categories for audit trails
17. **`custom_data_fields`**: Custom data field definitions
18. **`hierarchies`**: Organizational hierarchies
19. **`rollups`**: Summary rollup definitions

#### Schema Creation Process:

1. **Enter Credentials**: Input Databricks workspace URL and access token
2. **Test Connection**: Click "Test Connection" to verify credentials  
3. **Configure Schema**: Enter catalog name and schema name
4. **Create Schema**: Click "Create Schema & Tables" to create the schema
5. **Create Tables**: Use the provided notebook (`create_tables_notebook.py`) or SQL commands to create tables
6. **Generate Data**: Use "Generate XML & Write to Volume" to populate the schema

#### Alternative: Use the Databricks Notebook

For complete table creation, use the provided `create_tables_notebook.py` file:

1. **Upload to Databricks**: Import the notebook file into your Databricks workspace
2. **Configure**: Update the `catalog_name` and `schema_name` variables
3. **Run**: Execute all cells to create the complete schema and tables
4. **Verify**: Check that all 8 tables are created successfully

The tables are created with optimized Delta Lake format for performance and include proper data types for all columns.

## File Structure

```
.
├── app.py                      # Main Dash application
├── xml_generator.py            # XML generation logic
├── databricks_client.py        # Databricks volume client
├── create_tables_notebook.py   # Databricks notebook for table creation
├── requirements.txt            # Python dependencies
├── .gitignore                 # Git ignore file (protects credentials)
├── create_env.sh              # Interactive .env creation script
├── setup_environment.sh       # Full environment setup script
├── env_example.txt            # Template for .env file
├── QUICKSTART.md              # Quick setup guide
├── TROUBLESHOOTING.md         # Troubleshooting guide
└── README.md                  # This file
```

## API Reference

### XML Generator

```python
from xml_generator import generate_synthetic_valnav_xml

xml_content = generate_synthetic_valnav_xml(
    num_wells=100,
    num_facilities=10,
    num_scenarios=3,
    num_price_decks=2,
    history_months=24,
    schedule_coverage=0.7
)
```

### Databricks Client

```python
from databricks_client import write_to_databricks_volume

success, message = write_to_databricks_volume(
    xml_content=xml_content,
    volume_path="/Volumes/catalog/schema/volume/data.xml"
)
```

## Troubleshooting

### Common Issues

#### Authentication Errors
- **Error**: `Connection failed: Unauthorized`
- **Solution**: Verify your `DATABRICKS_TOKEN` is correct and hasn't expired

#### Volume Access Errors
- **Error**: `Volume does not exist`
- **Solution**: Check the volume path format and ensure the volume exists

#### Permission Errors
- **Error**: `Permission denied`
- **Solution**: Ensure you have the required permissions on the volume

#### Network Errors
- **Error**: `Connection timeout`
- **Solution**: Check your network connection and workspace URL

### Debugging Tips

1. **Enable Debug Mode**: Set `debug=True` in `app.run_server()` for detailed error logs
2. **Check Logs**: Monitor the console output for detailed error messages
3. **Test Connection**: Use the connection test feature in the UI
4. **Validate Inputs**: Ensure all input parameters are within valid ranges

### Getting Help

If you encounter issues:

1. Check the console output for error messages
2. Verify your Databricks configuration
3. Test with smaller data sets first
4. Check Unity Catalog permissions

## Performance Notes

- **Large Datasets**: Generation time increases with dataset size
- **File Size**: Estimated file sizes are shown in the UI
- **Memory Usage**: Large datasets may require significant memory
- **Network**: Upload time depends on file size and network speed

## 🔒 Security

### Environment Variables Protection

The project includes a comprehensive `.gitignore` file that prevents sensitive information from being committed to version control:

- **`.env` files**: All environment files are ignored to protect Databricks credentials
- **Virtual environments**: The `valnav-dash-env/` directory is ignored
- **Python cache files**: `__pycache__/` and `*.pyc` files are ignored
- **IDE/OS files**: Editor and system-specific files are ignored

### Safe Practices

✅ **DO**:
- Use `.env` files for credentials
- Keep your personal access tokens private
- Regularly rotate your Databricks tokens
- Use the provided scripts for secure setup

❌ **DON'T**:
- Commit `.env` files to version control
- Share your personal access tokens
- Hardcode credentials in source code
- Push sensitive configuration files

### Credential Management

The app uses a secure, UI-based credential system:
1. **Web UI Input** (recommended for all use cases)
   - Enter credentials directly in the web interface
   - Credentials kept only in browser memory during session
   - No storage on disk or in environment variables
   - Real-time connection testing

2. **Legacy Support** (backwards compatibility)
   - Environment variables still supported for automation
   - `.env` files still work for development setups

## License

This project is provided as-is for demonstration purposes.

## Contributing

Feel free to submit issues and enhancement requests! 