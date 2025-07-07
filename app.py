import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
from datetime import datetime
import os
from xml_generator import generate_synthetic_valnav_xml
from databricks_client import write_to_databricks_volume, validate_databricks_config, create_schema_and_tables, DatabricksVolumeClient

# Load environment variables from .env file if it exists (for backwards compatibility)
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("ℹ️ Environment variables loaded (if .env file exists)")
except ImportError:
    print("ℹ️ python-dotenv not available")
except Exception as e:
    print(f"ℹ️ Could not load .env file: {e}")

print("🌐 ValNav Synthetic Data Generator starting...")
print("💡 Databricks credentials will be entered through the web UI for enhanced security")

# Initialize the Dash app with Bootstrap theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("ValNav Synthetic Data Generator", className="text-center mb-4"),
            html.P("Generate synthetic ValNav well data and write to Databricks Unity Catalog", 
                   className="text-center text-muted mb-5")
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("ValNav Data Model Configuration")),
                dbc.CardBody([
                    # Core Entities
                    html.H6("🏗️ Core Entities", className="mb-2"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Wells", html_for="wells-input"),
                            dbc.Input(
                                id="wells-input",
                                type="number",
                                value=250,
                                min=0,
                                max=10000,
                                step=1,
                                placeholder="0-10,000"
                            ),
                            dbc.FormText("Well entities (0-10,000)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Facilities", html_for="facilities-input"),
                            dbc.Input(
                                id="facilities-input",
                                type="number",
                                value=15,
                                min=0,
                                max=1000,
                                step=1,
                                placeholder="0-1,000"
                            ),
                            dbc.FormText("Processing facilities (0-1,000)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Scenarios", html_for="scenarios-input"),
                            dbc.Input(
                                id="scenarios-input",
                                type="number",
                                value=2,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50"
                            ),
                            dbc.FormText("Economic scenarios (0-50)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Price Decks", html_for="price-decks-input"),
                            dbc.Input(
                                id="price-decks-input",
                                type="number",
                                value=3,
                                min=0,
                                max=20,
                                step=1,
                                placeholder="0-20"
                            ),
                            dbc.FormText("Price forecasts (0-20)")
                        ], width=3)
                    ], className="mb-3"),
                    
                    # Economic & Reference Data
                    html.H6("💰 Economic & Reference Data", className="mb-2"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Companies", html_for="companies-input"),
                            dbc.Input(
                                id="companies-input",
                                type="number",
                                value=5,
                                min=0,
                                max=100,
                                step=1,
                                placeholder="0-100"
                            ),
                            dbc.FormText("Operating companies (0-100)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Countries", html_for="countries-input"),
                            dbc.Input(
                                id="countries-input",
                                type="number",
                                value=8,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50"
                            ),
                            dbc.FormText("Countries/regions (0-50)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Currencies", html_for="currencies-input"),
                            dbc.Input(
                                id="currencies-input",
                                type="number",
                                value=8,
                                min=0,
                                max=30,
                                step=1,
                                placeholder="0-30"
                            ),
                            dbc.FormText("Currency types (0-30)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Fiscal Regimes", html_for="fiscal-regimes-input"),
                            dbc.Input(
                                id="fiscal-regimes-input",
                                type="number",
                                value=3,
                                min=0,
                                max=20,
                                step=1,
                                placeholder="0-20"
                            ),
                            dbc.FormText("Tax/royalty systems (0-20)")
                        ], width=3)
                    ], className="mb-3"),
                    
                    # Infrastructure & Operations
                    html.H6("🚧 Infrastructure & Operations", className="mb-2"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Meter Stations", html_for="meter-stations-input"),
                            dbc.Input(
                                id="meter-stations-input",
                                type="number",
                                value=0,
                                min=0,
                                max=200,
                                step=1,
                                placeholder="0-200"
                            ),
                            dbc.FormText("Measurement points (0-200)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Transportation Areas", html_for="transportation-areas-input"),
                            dbc.Input(
                                id="transportation-areas-input",
                                type="number",
                                value=0,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50"
                            ),
                            dbc.FormText("Transport regions (0-50)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Type Wells", html_for="type-wells-input"),
                            dbc.Input(
                                id="type-wells-input",
                                type="number",
                                value=0,
                                min=0,
                                max=100,
                                step=1,
                                placeholder="0-100"
                            ),
                            dbc.FormText("Well templates (0-100)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Tax Pools", html_for="tax-pools-input"),
                            dbc.Input(
                                id="tax-pools-input",
                                type="number",
                                value=0,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50"
                            ),
                            dbc.FormText("Tax calculation pools (0-50)")
                        ], width=3)
                    ], className="mb-3"),
                    
                    # Advanced Configuration
                    html.H6("⚙️ Advanced Configuration", className="mb-2"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Batch Definitions", html_for="batch-definitions-input"),
                            dbc.Input(
                                id="batch-definitions-input",
                                type="number",
                                value=0,
                                min=0,
                                max=30,
                                step=1,
                                placeholder="0-30"
                            ),
                            dbc.FormText("Batch processing configs (0-30)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Change Record Categories", html_for="change-record-categories-input"),
                            dbc.Input(
                                id="change-record-categories-input",
                                type="number",
                                value=0,
                                min=0,
                                max=20,
                                step=1,
                                placeholder="0-20"
                            ),
                            dbc.FormText("Change tracking types (0-20)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Custom Data Fields", html_for="custom-data-fields-input"),
                            dbc.Input(
                                id="custom-data-fields-input",
                                type="number",
                                value=0,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50"
                            ),
                            dbc.FormText("User-defined fields (0-50)")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Label("Hierarchies", html_for="hierarchies-input"),
                            dbc.Input(
                                id="hierarchies-input",
                                type="number",
                                value=0,
                                min=0,
                                max=10,
                                step=1,
                                placeholder="0-10"
                            ),
                            dbc.FormText("Organizational structures (0-10)")
                        ], width=3)
                    ], className="mb-3"),
                    
                    # Production & Scheduling
                    html.H6("📈 Production & Scheduling", className="mb-2"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Production History (Months)", html_for="history-input"),
                            dbc.Input(
                                id="history-input",
                                type="number",
                                value=24,
                                min=1,
                                max=120,
                                step=1,
                                placeholder="1-120"
                            ),
                            dbc.FormText("Months of history per well (1-120)")
                        ], width=4),
                        
                        dbc.Col([
                            dbc.Label("Well Schedule Coverage (%)", html_for="schedule-coverage-input"),
                            dbc.Input(
                                id="schedule-coverage-input",
                                type="number",
                                value=70,
                                min=10,
                                max=100,
                                step=5,
                                placeholder="10-100"
                            ),
                            dbc.FormText("% of wells with schedules (10-100%)")
                        ], width=4),
                        
                        dbc.Col([
                            dbc.Label("Rollups", html_for="rollups-input"),
                            dbc.Input(
                                id="rollups-input",
                                type="number",
                                value=0,
                                min=0,
                                max=20,
                                step=1,
                                placeholder="0-20"
                            ),
                            dbc.FormText("Summary aggregations (0-20)")
                        ], width=4)
                    ], className="mb-4")
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Databricks Configuration"),
                    dbc.Badge("Not Connected", id="config-status-badge", color="secondary", className="ms-2")
                ]),
                dbc.CardBody([
                    # Databricks Credentials Section
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Databricks Workspace URL", html_for="workspace-url-input"),
                            dbc.Input(
                                id="workspace-url-input",
                                type="text",
                                placeholder="https://your-workspace.cloud.databricks.com",
                                value="",
                                className="mb-2"
                            ),
                            dbc.FormText("Your Databricks workspace URL", className="mb-3")
                        ], width=8),
                        
                        dbc.Col([
                            dbc.Label("Test Connection", html_for="test-connection-btn"),
                            html.Br(),
                            dbc.Button(
                                "Test Connection",
                                id="test-connection-btn",
                                color="outline-primary",
                                size="sm",
                                className="w-100"
                            ),
                            dbc.FormText("Verify credentials", className="text-center")
                        ], width=4)
                    ], className="mb-3"),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Personal Access Token", html_for="access-token-input"),
                            dbc.Input(
                                id="access-token-input",
                                type="password",
                                placeholder="Enter your Databricks personal access token",
                                value="",
                                className="mb-2"
                            ),
                            dbc.FormText([
                                "Your Databricks personal access token (kept secure in browser memory only). ",
                                html.A("Get token", href="#", id="token-help-link", className="text-primary")
                            ])
                        ], width=12)
                    ], className="mb-3"),
                    
                    # Connection status
                    dbc.Alert(
                        id="connection-status-alert",
                        is_open=False,
                        dismissable=True,
                        className="mb-3"
                    ),
                    
                    html.Hr(),
                    
                    # Volume and File Configuration
                    html.H5("Volume & File Configuration", className="mb-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Databricks Volume Path", html_for="volume-path-input"),
                            dbc.Input(
                                id="volume-path-input",
                                type="text",
                                value="/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/",
                                placeholder="Enter Databricks volume path"
                            ),
                            dbc.FormText("Path to Databricks volume where XML file will be written")
                        ], width=8),
                        
                        dbc.Col([
                            dbc.Label("File Name", html_for="filename-input"),
                            dbc.Input(
                                id="filename-input",
                                type="text",
                                value="synthetic-valnav-data.xml",
                                placeholder="Enter filename"
                            ),
                            dbc.FormText("Name of the XML file to create")
                        ], width=4)
                    ], className="mb-3"),
                    
                    # Unity Catalog Configuration
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Catalog Name", html_for="catalog-input"),
                            dbc.Input(
                                id="catalog-input",
                                type="text",
                                value="harrison_chen_catalog",
                                placeholder="Enter catalog name"
                            ),
                            dbc.FormText("Unity Catalog name")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Label("Schema Name", html_for="schema-input"),
                            dbc.Input(
                                id="schema-input",
                                type="text",
                                value="valnav_bronze",
                                placeholder="Enter schema name"
                            ),
                            dbc.FormText("Schema name for bronze layer data")
                        ], width=6)
                    ], className="mb-4")
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Alert([
                        html.H6("📋 Workflow:", className="mb-2"),
                        html.Ol([
                            html.Li("Configure your data model parameters above"),
                            html.Li("Generate XML & Write to Volume (creates the XML file)"),
                            html.Li("Create Schema & Tables from XML (reads the XML and creates matching tables)")
                        ], className="mb-0")
                    ], color="info", className="mb-3"),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                "Generate XML & Write to Volume",
                                id="generate-button",
                                color="primary",
                                size="lg",
                                className="w-100",
                                disabled=True
                            )
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Button(
                                "Create Schema & Tables from XML",
                                id="create-schema-button",
                                color="success",
                                size="lg",
                                className="w-100",
                                disabled=True
                            ),
                            dbc.FormText("Creates tables based on your generated XML file", className="text-center mt-2")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Button(
                                "Preview XML Structure",
                                id="preview-button",
                                color="outline-secondary",
                                size="lg",
                                className="w-100"
                            )
                        ], width=6)
                    ])
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    # Progress and Status Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("Generation Status")),
                dbc.CardBody([
                    dbc.Progress(id="progress-bar", value=0, className="mb-3"),
                    html.Div(id="status-output", className="text-muted"),
                    html.Hr(),
                    html.Div(id="result-output")
                ])
            ])
        ], width=12)
    ], className="mb-4"),
    
    # Preview Section
    dbc.Row([
        dbc.Col([
            dbc.Collapse([
                dbc.Card([
                    dbc.CardHeader(html.H4("XML Preview")),
                    dbc.CardBody([
                        html.Pre(id="xml-preview", className="bg-light p-3", style={"max-height": "400px", "overflow": "auto"})
                    ])
                ])
            ], id="preview-collapse", is_open=False)
        ], width=12)
    ])
], fluid=True)

# Callback for testing Databricks connection
@app.callback(
    [Output("config-status-badge", "children"),
     Output("config-status-badge", "color"),
     Output("connection-status-alert", "children"),
     Output("connection-status-alert", "color"),
     Output("connection-status-alert", "is_open"),
     Output("generate-button", "disabled"),
     Output("create-schema-button", "disabled")],
    [Input("test-connection-btn", "n_clicks")],
    [State("workspace-url-input", "value"),
     State("access-token-input", "value")],
    prevent_initial_call=True
)
def test_databricks_connection(n_clicks, workspace_url, access_token):
    if not n_clicks:
        return "Not Connected", "secondary", "", "info", False, True, True
    
    try:
        if not workspace_url or not access_token:
            return ("⚠️ Missing Info", "warning", 
                    "Please enter both workspace URL and access token", 
                    "warning", True, True, True)
        
        # Test the connection using the provided credentials
        from databricks_client import DatabricksVolumeClient
        
        client = DatabricksVolumeClient(workspace_url=workspace_url, token=access_token)
        success, message = client.test_connection()
        
        if success:
            return ("✅ Connected", "success", 
                    f"Successfully connected! {message}", 
                    "success", True, False, False)
        else:
            return ("❌ Failed", "danger", 
                    f"Connection failed: {message}", 
                    "danger", True, True, True)
            
    except Exception as e:
        return ("❌ Error", "danger", 
                f"Error testing connection: {str(e)}", 
                "danger", True, True, True)

# Callback for creating schema and tables from XML
@app.callback(
    [Output("result-output", "children", allow_duplicate=True),
     Output("status-output", "children", allow_duplicate=True)],
    [Input("create-schema-button", "n_clicks")],
    [State("workspace-url-input", "value"),
     State("access-token-input", "value"),
     State("catalog-input", "value"),
     State("schema-input", "value"),
     State("volume-path-input", "value"),
     State("filename-input", "value")],
    prevent_initial_call=True
)
def create_schema_and_tables_callback(n_clicks, workspace_url, access_token, catalog_name, schema_name, volume_path, filename):
    if not n_clicks:
        return "", ""
    
    try:
        if not workspace_url or not access_token:
            return (dbc.Alert("Please enter Databricks credentials and test connection", color="danger"), 
                    "Error: Missing credentials")
        
        if not catalog_name or not schema_name:
            return (dbc.Alert("Please enter both catalog name and schema name", color="danger"), 
                    "Error: Missing catalog or schema name")
        
        if not volume_path or not filename:
            return (dbc.Alert("Please enter volume path and filename. Make sure to generate XML data first!", color="warning"), 
                    "Error: Missing volume path or filename")
        
        # Construct full path to XML file
        if not volume_path.endswith('/'):
            volume_path += '/'
        xml_file_path = volume_path + filename
        
        # Create schema and tables based on XML content
        try:
            client = DatabricksVolumeClient(workspace_url=workspace_url, token=access_token)
            
            # Test connection first
            connected, conn_message = client.test_connection()
            if not connected:
                return (
                    dbc.Alert(f"❌ Connection failed: {conn_message}", color="danger"),
                    f"Connection failed: {conn_message}"
                )
            
            # Create schema and tables from XML
            success, message = client.create_valnav_schema_from_xml(
                catalog_name=catalog_name,
                schema_name=schema_name,
                volume_path=xml_file_path
            )
            
            if success:
                result_children = [
                    html.H4("✅ Schema & Tables Created from XML!", className="alert-heading"),
                    html.P("Successfully created schema and tables based on your generated XML file."),
                    html.P(f"XML File: {xml_file_path}"),
                    html.P(f"Catalog: {catalog_name}"),
                    html.P(f"Schema: {schema_name}"),
                    html.P(f"Created at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"),
                    html.Hr(),
                    html.Pre(message, style={'background-color': '#f8f9fa', 'padding': '10px', 'border-radius': '5px', 'font-size': '12px', 'white-space': 'pre-wrap'})
                ]
                
                result = dbc.Alert(result_children, color="success")
                status = "Schema and tables created from XML successfully"
            else:
                result = dbc.Alert([
                    html.H4("❌ Schema Creation from XML Failed", className="alert-heading"),
                    html.P(f"Error: {message}"),
                    html.P("Make sure the XML file exists in the volume and you have proper permissions.")
                ], color="danger")
                status = f"Schema creation from XML failed: {message}"
            
            return result, status
            
        except Exception as e:
            logger.error(f"Error creating schema from XML: {e}")
            return (
                dbc.Alert([
                    html.H4("❌ Error Creating Schema from XML", className="alert-heading"),
                    html.P(f"Error: {str(e)}")
                ], color="danger"),
                f"Error creating schema from XML: {str(e)}"
            )
        
    except Exception as e:
        return (dbc.Alert([
            html.H4("Error!", className="alert-heading"),
            html.P(f"An error occurred: {str(e)}")
        ], color="danger"), f"Error: {str(e)}")

# Callback for generating XML and writing to volume
@app.callback(
    [Output("progress-bar", "value"),
     Output("status-output", "children"),
     Output("result-output", "children")],
    [Input("generate-button", "n_clicks")],
    [State("wells-input", "value"),
     State("facilities-input", "value"),
     State("scenarios-input", "value"),
     State("price-decks-input", "value"),
     State("companies-input", "value"),
     State("countries-input", "value"),
     State("currencies-input", "value"),
     State("fiscal-regimes-input", "value"),
     State("meter-stations-input", "value"),
     State("transportation-areas-input", "value"),
     State("type-wells-input", "value"),
     State("tax-pools-input", "value"),
     State("batch-definitions-input", "value"),
     State("change-record-categories-input", "value"),
     State("custom-data-fields-input", "value"),
     State("hierarchies-input", "value"),
     State("rollups-input", "value"),
     State("history-input", "value"),
     State("schedule-coverage-input", "value"),
     State("workspace-url-input", "value"),
     State("access-token-input", "value"),
     State("volume-path-input", "value"),
     State("filename-input", "value"),
     State("catalog-input", "value"),
     State("schema-input", "value")],
    prevent_initial_call=True
)
def generate_and_write_xml(n_clicks, num_wells, num_facilities, num_scenarios, 
                          num_price_decks, num_companies, num_countries, num_currencies,
                          num_fiscal_regimes, num_meter_stations, num_transportation_areas,
                          num_type_wells, num_tax_pools, num_batch_definitions, 
                          num_change_record_categories, num_custom_data_fields,
                          num_hierarchies, num_rollups, history_months, schedule_coverage,
                          workspace_url, access_token, volume_path, filename, 
                          catalog_name, schema_name):
    if not n_clicks:
        return 0, "", ""
    
    try:
        # Validate inputs
        if not all([num_wells, num_facilities, num_scenarios, num_price_decks]):
            return 0, "Error: Please fill in all required fields", dbc.Alert("Invalid input parameters", color="danger")
        
        if not workspace_url or not access_token:
            return 0, "Error: Please enter Databricks credentials and test connection", dbc.Alert("Missing Databricks credentials", color="danger")
        
        # Update progress
        progress = 10
        status = "Generating synthetic XML data..."
        
        # Generate XML
        xml_data = generate_synthetic_valnav_xml(
            num_wells=num_wells,
            num_facilities=num_facilities,
            num_scenarios=num_scenarios,
            num_price_decks=num_price_decks,
            num_companies=num_companies,
            num_countries=num_countries,
            num_currencies=num_currencies,
            num_fiscal_regimes=num_fiscal_regimes,
            num_meter_stations=num_meter_stations,
            num_transportation_areas=num_transportation_areas,
            num_type_wells=num_type_wells,
            num_tax_pools=num_tax_pools,
            num_batch_definitions=num_batch_definitions,
            num_change_record_categories=num_change_record_categories,
            num_custom_data_fields=num_custom_data_fields,
            num_hierarchies=num_hierarchies,
            num_rollups=num_rollups,
            history_months=history_months,
            schedule_coverage=schedule_coverage/100
        )
        
        progress = 50
        status = "XML generation complete. Writing to Databricks volume..."
        
        # Construct full file path
        full_path = f"{volume_path.rstrip('/')}/{filename}"
        
        # Write to Databricks volume using UI credentials
        success, message = write_to_databricks_volume(
            xml_content=xml_data, 
            volume_path=full_path,
            workspace_url=workspace_url,
            token=access_token
        )
        
        if success:
            progress = 100
            status = "Successfully completed!"
            result = dbc.Alert([
                html.H4("Success!", className="alert-heading"),
                html.P(f"Generated XML with {num_wells} wells, {num_facilities} facilities, {num_scenarios} scenarios, and {num_price_decks} price decks."),
                html.P(f"File written to: {full_path}"),
                html.P(f"File size: {len(xml_data):,} bytes"),
                html.P(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            ], color="success")
        else:
            progress = 0
            status = f"Error writing to volume: {message}"
            result = dbc.Alert([
                html.H4("Error!", className="alert-heading"),
                html.P(f"Failed to write file to Databricks volume: {message}")
            ], color="danger")
        
        return progress, status, result
        
    except Exception as e:
        return 0, f"Error: {str(e)}", dbc.Alert([
            html.H4("Error!", className="alert-heading"),
            html.P(f"An error occurred: {str(e)}")
        ], color="danger")

# Callback for XML preview
@app.callback(
    [Output("xml-preview", "children"),
     Output("preview-collapse", "is_open")],
    [Input("preview-button", "n_clicks")],
    [State("wells-input", "value"),
     State("facilities-input", "value"),
     State("scenarios-input", "value"),
     State("price-decks-input", "value"),
     State("companies-input", "value"),
     State("countries-input", "value"),
     State("currencies-input", "value"),
     State("fiscal-regimes-input", "value"),
     State("meter-stations-input", "value"),
     State("transportation-areas-input", "value"),
     State("type-wells-input", "value"),
     State("tax-pools-input", "value"),
     State("batch-definitions-input", "value"),
     State("change-record-categories-input", "value"),
     State("custom-data-fields-input", "value"),
     State("hierarchies-input", "value"),
     State("rollups-input", "value"),
     State("history-input", "value"),
     State("schedule-coverage-input", "value")],
    prevent_initial_call=True
)
def preview_xml(n_clicks, num_wells, num_facilities, num_scenarios, 
                num_price_decks, num_companies, num_countries, num_currencies,
                num_fiscal_regimes, num_meter_stations, num_transportation_areas,
                num_type_wells, num_tax_pools, num_batch_definitions, 
                num_change_record_categories, num_custom_data_fields,
                num_hierarchies, num_rollups, history_months, schedule_coverage):
    if not n_clicks:
        return "", False
    
    try:
        # Generate a small sample for preview
        sample_xml = generate_synthetic_valnav_xml(
            num_wells=min(3, num_wells or 3),
            num_facilities=min(2, num_facilities or 2),
            num_scenarios=min(2, num_scenarios or 2),
            num_price_decks=min(2, num_price_decks or 2),
            num_companies=min(2, num_companies or 2),
            num_countries=min(3, num_countries or 3),
            num_currencies=min(3, num_currencies or 3),
            num_fiscal_regimes=min(1, num_fiscal_regimes or 1),
            num_meter_stations=min(1, num_meter_stations or 0),
            num_transportation_areas=min(1, num_transportation_areas or 0),
            num_type_wells=min(1, num_type_wells or 0),
            num_tax_pools=min(1, num_tax_pools or 0),
            num_batch_definitions=min(1, num_batch_definitions or 0),
            num_change_record_categories=min(1, num_change_record_categories or 0),
            num_custom_data_fields=min(2, num_custom_data_fields or 0),
            num_hierarchies=min(1, num_hierarchies or 0),
            num_rollups=min(1, num_rollups or 0),
            history_months=min(6, history_months or 6),
            schedule_coverage=0.5
        )
        
        # Truncate for preview
        preview_text = sample_xml[:2000] + "\n\n... (truncated for preview)\n\n" + sample_xml[-500:]
        
        return preview_text, True
        
    except Exception as e:
        return f"Error generating preview: {str(e)}", True

# Callback for token help modal/tooltip
@app.callback(
    Output("token-help-link", "href"),
    [Input("token-help-link", "n_clicks")],
    prevent_initial_call=True
)
def show_token_help(n_clicks):
    # Return a JavaScript that opens the help in a modal or new tab
    return "javascript:alert('To get your Databricks Personal Access Token:\\n\\n1. Go to your Databricks workspace\\n2. Click your profile icon (top right)\\n3. Select User Settings\\n4. Go to Developer tab\\n5. Click Manage next to Access tokens\\n6. Generate a new token\\n7. Copy and paste it here\\n\\nNote: Keep this token secure and never share it!');"

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050) 