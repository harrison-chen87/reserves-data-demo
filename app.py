import dash
from dash import dcc, html, Input, Output, State
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

print("🌐 Definitely-Not-ValNav Synthetic Data Generator starting...")
print("💡 Databricks credentials will be entered through the web UI for enhanced security")

# Initialize the Dash app without Bootstrap theme
app = dash.Dash(__name__)

# Custom CSS for 1990s Windows 95/98 aesthetic
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            * {
                box-sizing: border-box;
            }
            
            body {
                font-family: "MS Sans Serif", Arial, sans-serif;
                font-size: 11px;
                background-color: #C0C0C0;
                color: #000000;
                margin: 0;
                padding: 8px;
                background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='4' height='4' viewBox='0 0 4 4'%3E%3Cpath fill='%23808080' fill-opacity='0.3' d='M1,3h1v1H1V3zm2-2h1v1H3V1z'/%3E%3C/svg%3E");
            }
            
            .container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 0;
            }
            
            .window-frame {
                background-color: #C0C0C0;
                border: 2px outset #C0C0C0;
                margin: 4px;
                padding: 4px;
                box-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            }
            
            .window-header {
                background: linear-gradient(90deg, #0080FF 0%, #004080 100%);
                color: white;
                padding: 2px 4px;
                font-weight: bold;
                font-size: 11px;
                border: 1px outset #C0C0C0;
                margin: -4px -4px 4px -4px;
            }
            
            .window-content {
                background-color: #C0C0C0;
                padding: 8px;
            }
            
            .group-box {
                border: 2px inset #C0C0C0;
                margin: 8px 0;
                padding: 12px 8px 8px 8px;
                position: relative;
                background-color: #C0C0C0;
            }
            
            .group-box-title {
                position: absolute;
                top: -6px;
                left: 8px;
                background-color: #C0C0C0;
                padding: 0 4px;
                font-weight: bold;
                font-size: 11px;
                color: #000000;
            }
            
            .button-90s {
                background-color: #C0C0C0;
                border: 2px outset #C0C0C0;
                color: #000000;
                padding: 4px 16px;
                font-family: "MS Sans Serif", Arial, sans-serif;
                font-size: 11px;
                cursor: pointer;
                margin: 2px;
                min-height: 24px;
                font-weight: normal;
            }
            
            .button-90s:hover {
                background-color: #D0D0D0;
            }
            
            .button-90s:active {
                border: 2px inset #C0C0C0;
                background-color: #A0A0A0;
            }
            
            .button-90s:disabled {
                color: #808080;
                background-color: #C0C0C0;
                border: 2px outset #C0C0C0;
                cursor: not-allowed;
            }
            
            .button-primary {
                background-color: #0080FF;
                color: white;
                border: 2px outset #0080FF;
                font-weight: bold;
            }
            
            .button-primary:hover {
                background-color: #4090FF;
            }
            
            .button-primary:active {
                border: 2px inset #0080FF;
                background-color: #0060C0;
            }
            
            .button-success {
                background-color: #008000;
                color: white;
                border: 2px outset #008000;
                font-weight: bold;
            }
            
            .button-success:hover {
                background-color: #00A000;
            }
            
            .button-success:active {
                border: 2px inset #008000;
                background-color: #006000;
            }
            
            .input-90s {
                background-color: white;
                border: 2px inset #C0C0C0;
                color: #000000;
                padding: 2px 4px;
                font-family: "MS Sans Serif", Arial, sans-serif;
                font-size: 11px;
                margin: 2px;
                height: 20px;
            }
            
            .input-90s:focus {
                outline: none;
                border: 2px inset #0080FF;
            }
            
            .label-90s {
                font-family: "MS Sans Serif", Arial, sans-serif;
                font-size: 11px;
                color: #000000;
                margin: 2px 0;
                display: block;
            }
            
            .form-text-90s {
                font-family: "MS Sans Serif", Arial, sans-serif;
                font-size: 10px;
                color: #606060;
                margin: 1px 0 4px 0;
            }
            
            .status-bar {
                background-color: #C0C0C0;
                border: 1px inset #C0C0C0;
                padding: 2px 4px;
                font-size: 10px;
                color: #000000;
                margin: 4px 0;
            }
            
            .progress-bar-90s {
                background-color: white;
                border: 2px inset #C0C0C0;
                height: 20px;
                margin: 4px 0;
                position: relative;
                overflow: hidden;
            }
            
            .progress-bar-fill {
                background: linear-gradient(90deg, #0080FF 0%, #004080 100%);
                height: 100%;
                transition: width 0.3s ease;
            }
            
            .alert-90s {
                border: 2px inset #C0C0C0;
                padding: 8px;
                margin: 4px 0;
                background-color: #FFFFE0;
                font-size: 11px;
            }
            
            .alert-success {
                background-color: #E0FFE0;
                color: #008000;
            }
            
            .alert-error {
                background-color: #FFE0E0;
                color: #800000;
            }
            
            .alert-warning {
                background-color: #FFF0E0;
                color: #804000;
            }
            
            .badge-90s {
                background-color: #808080;
                color: white;
                padding: 2px 6px;
                font-size: 10px;
                border: 1px outset #808080;
                margin: 2px;
            }
            
            .badge-connected {
                background-color: #008000;
                border: 1px outset #008000;
            }
            
            .badge-error {
                background-color: #800000;
                border: 1px outset #800000;
            }
            
            .badge-warning {
                background-color: #FF8000;
                border: 1px outset #FF8000;
            }
            
            .row-90s {
                display: flex;
                flex-wrap: wrap;
                margin: 4px 0;
            }
            
            .col-90s {
                flex: 1;
                margin: 0 4px;
                min-width: 120px;
            }
            
            .col-90s-6 {
                flex: 0 0 48%;
                margin: 0 1%;
            }
            
            .col-90s-4 {
                flex: 0 0 31%;
                margin: 0 1%;
            }
            
            .col-90s-3 {
                flex: 0 0 23%;
                margin: 0 1%;
            }
            
            .text-center {
                text-align: center;
            }
            
            .text-bold {
                font-weight: bold;
            }
            
            .mb-2 {
                margin-bottom: 8px;
            }
            
            .mb-4 {
                margin-bottom: 16px;
            }
            
            .w-100 {
                width: 100%;
            }
            
            .textarea-90s {
                background-color: white;
                border: 2px inset #C0C0C0;
                color: #000000;
                padding: 4px;
                font-family: "Courier New", monospace;
                font-size: 10px;
                margin: 2px;
                resize: vertical;
            }
            
            .code-preview {
                background-color: white;
                border: 2px inset #C0C0C0;
                padding: 8px;
                font-family: "Courier New", monospace;
                font-size: 10px;
                color: #000000;
                max-height: 400px;
                overflow: auto;
                white-space: pre-wrap;
            }
            
            h1 {
                font-family: "MS Sans Serif", Arial, sans-serif;
                font-size: 18px;
                color: #000000;
                margin: 8px 0;
                font-weight: bold;
            }
            
            h2, h3, h4, h5, h6 {
                font-family: "MS Sans Serif", Arial, sans-serif;
                font-size: 12px;
                color: #000000;
                margin: 4px 0;
                font-weight: bold;
            }
            
            ol, ul {
                font-size: 11px;
                margin: 4px 0;
                padding-left: 20px;
            }
            
            hr {
                border: 1px inset #C0C0C0;
                margin: 8px 0;
            }
            
            .collapse-90s {
                border: 2px inset #C0C0C0;
                margin: 4px 0;
                background-color: #C0C0C0;
            }
            
            .collapse-90s.open {
                display: block;
            }
            
            .collapse-90s.closed {
                display: none;
            }
            
            /* Custom scrollbar for 90s feel */
            ::-webkit-scrollbar {
                width: 16px;
            }
            
            ::-webkit-scrollbar-track {
                background: #C0C0C0;
                border: 1px inset #C0C0C0;
            }
            
            ::-webkit-scrollbar-thumb {
                background: #808080;
                border: 1px outset #808080;
            }
            
            ::-webkit-scrollbar-thumb:hover {
                background: #606060;
            }
            
            /* Responsive adjustments */
            @media (max-width: 768px) {
                .col-90s-6, .col-90s-4, .col-90s-3 {
                    flex: 0 0 100%;
                    margin: 2px 0;
                }
                
                .row-90s {
                    flex-direction: column;
                }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Define the app layout with 1990s styling
app.layout = html.Div([
    html.Div([
        # Main Title Window
        html.Div([
            html.Div("Definitely-Not-ValNav Synthetic Data Generator", className="window-header"),
            html.Div([
                html.H1("🖥️ Definitely-Not-ValNav Synthetic Data Generator", className="text-center"),
                html.P("Generate synthetic Definitely-Not-ValNav well data and write to Databricks Unity Catalog", 
                       className="text-center", style={"color": "#606060", "font-size": "12px"})
            ], className="window-content text-center")
        ], className="window-frame"),
        
        # Data Model Configuration Window
        html.Div([
            html.Div("Definitely-Not-ValNav Data Model Configuration", className="window-header"),
            html.Div([
                # Core Entities
                html.Div([
                    html.Div("🏗️ Core Entities", className="group-box-title"),
                    html.Div([
                        html.Div([
                            html.Label("Wells", className="label-90s"),
                            dcc.Input(
                                id="wells-input",
                                type="number",
                                value=250,
                                min=0,
                                max=10000,
                                step=1,
                                placeholder="0-10,000",
                                className="input-90s w-100"
                            ),
                            html.Div("Well entities (0-10,000)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Facilities", className="label-90s"),
                            dcc.Input(
                                id="facilities-input",
                                type="number",
                                value=15,
                                min=0,
                                max=1000,
                                step=1,
                                placeholder="0-1,000",
                                className="input-90s w-100"
                            ),
                            html.Div("Processing facilities (0-1,000)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Scenarios", className="label-90s"),
                            dcc.Input(
                                id="scenarios-input",
                                type="number",
                                value=2,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50",
                                className="input-90s w-100"
                            ),
                            html.Div("Economic scenarios (0-50)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Price Decks", className="label-90s"),
                            dcc.Input(
                                id="price-decks-input",
                                type="number",
                                value=3,
                                min=0,
                                max=20,
                                step=1,
                                placeholder="0-20",
                                className="input-90s w-100"
                            ),
                            html.Div("Price forecasts (0-20)", className="form-text-90s")
                        ], className="col-90s-3")
                    ], className="row-90s")
                ], className="group-box"),
                
                # Economic & Reference Data
                html.Div([
                    html.Div("💰 Economic & Reference Data", className="group-box-title"),
                    html.Div([
                        html.Div([
                            html.Label("Companies", className="label-90s"),
                            dcc.Input(
                                id="companies-input",
                                type="number",
                                value=5,
                                min=0,
                                max=100,
                                step=1,
                                placeholder="0-100",
                                className="input-90s w-100"
                            ),
                            html.Div("Operating companies (0-100)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Countries", className="label-90s"),
                            dcc.Input(
                                id="countries-input",
                                type="number",
                                value=8,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50",
                                className="input-90s w-100"
                            ),
                            html.Div("Countries/regions (0-50)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Currencies", className="label-90s"),
                            dcc.Input(
                                id="currencies-input",
                                type="number",
                                value=8,
                                min=0,
                                max=30,
                                step=1,
                                placeholder="0-30",
                                className="input-90s w-100"
                            ),
                            html.Div("Currency types (0-30)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Fiscal Regimes", className="label-90s"),
                            dcc.Input(
                                id="fiscal-regimes-input",
                                type="number",
                                value=3,
                                min=0,
                                max=20,
                                step=1,
                                placeholder="0-20",
                                className="input-90s w-100"
                            ),
                            html.Div("Tax/royalty systems (0-20)", className="form-text-90s")
                        ], className="col-90s-3")
                    ], className="row-90s")
                ], className="group-box"),
                
                # Infrastructure & Operations
                html.Div([
                    html.Div("🚧 Infrastructure & Operations", className="group-box-title"),
                    html.Div([
                        html.Div([
                            html.Label("Meter Stations", className="label-90s"),
                            dcc.Input(
                                id="meter-stations-input",
                                type="number",
                                value=0,
                                min=0,
                                max=200,
                                step=1,
                                placeholder="0-200",
                                className="input-90s w-100"
                            ),
                            html.Div("Measurement points (0-200)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Transportation Areas", className="label-90s"),
                            dcc.Input(
                                id="transportation-areas-input",
                                type="number",
                                value=0,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50",
                                className="input-90s w-100"
                            ),
                            html.Div("Transport regions (0-50)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Type Wells", className="label-90s"),
                            dcc.Input(
                                id="type-wells-input",
                                type="number",
                                value=0,
                                min=0,
                                max=100,
                                step=1,
                                placeholder="0-100",
                                className="input-90s w-100"
                            ),
                            html.Div("Well templates (0-100)", className="form-text-90s")
                        ], className="col-90s-3"),
                        
                        html.Div([
                            html.Label("Tax Pools", className="label-90s"),
                            dcc.Input(
                                id="tax-pools-input",
                                type="number",
                                value=0,
                                min=0,
                                max=50,
                                step=1,
                                placeholder="0-50",
                                className="input-90s w-100"
                            ),
                            html.Div("Tax calculation pools (0-50)", className="form-text-90s")
                        ], className="col-90s-3")
                    ], className="row-90s")
                ], className="group-box"),
                
                # Production & Scheduling
                html.Div([
                    html.Div("📈 Production & Scheduling", className="group-box-title"),
                    html.Div([
                        html.Div([
                            html.Label("Production History (Months)", className="label-90s"),
                            dcc.Input(
                                id="history-input",
                                type="number",
                                value=24,
                                min=1,
                                max=120,
                                step=1,
                                placeholder="1-120",
                                className="input-90s w-100"
                            ),
                            html.Div("Months of history per well (1-120)", className="form-text-90s")
                        ], className="col-90s-4"),
                        
                        html.Div([
                            html.Label("Well Schedule Coverage (%)", className="label-90s"),
                            dcc.Input(
                                id="schedule-coverage-input",
                                type="number",
                                value=70,
                                min=10,
                                max=100,
                                step=5,
                                placeholder="10-100",
                                className="input-90s w-100"
                            ),
                            html.Div("% of wells with schedules (10-100%)", className="form-text-90s")
                        ], className="col-90s-4"),
                        
                        html.Div([
                            html.Label("Rollups", className="label-90s"),
                            dcc.Input(
                                id="rollups-input",
                                type="number",
                                value=0,
                                min=0,
                                max=20,
                                step=1,
                                placeholder="0-20",
                                className="input-90s w-100"
                            ),
                            html.Div("Summary aggregations (0-20)", className="form-text-90s")
                        ], className="col-90s-4")
                    ], className="row-90s")
                ], className="group-box")
            ], className="window-content")
        ], className="window-frame"),
        
        # Databricks Configuration Window
        html.Div([
            html.Div([
                "Databricks Configuration",
                html.Span(id="config-status-badge", children="✅ Connected", className="badge-90s badge-connected", style={"float": "right"})
            ], className="window-header"),
            html.Div([
                # Databricks Runtime Information
                html.Div([
                    html.Div("Runtime Environment", className="group-box-title"),
                    html.Div([
                        html.Div([
                            html.Div("🔒 Authentication", className="text-bold mb-2"),
                            html.P("Authenticated via Databricks Apps runtime", className="form-text-90s"),
                            html.P("✅ Built-in Unity Catalog integration", className="form-text-90s"),
                            html.P("🛡️ Secure service principal access", className="form-text-90s")
                        ], className="col-90s-6"),
                        
                        html.Div([
                            html.Div("🚀 Ready to Use", className="text-bold mb-2"),
                            html.P("No manual configuration required", className="form-text-90s"),
                            html.P("Automatic workspace detection", className="form-text-90s"),
                            html.P("Native Databricks Apps deployment", className="form-text-90s")
                        ], className="col-90s-6")
                    ], className="row-90s")
                ], className="group-box"),
                
                # Volume and File Configuration
                html.Div([
                    html.Div("Volume & File Configuration", className="group-box-title"),
                    html.Div([
                        html.Div([
                            html.Label("Databricks Volume Path", className="label-90s"),
                            dcc.Input(
                                id="volume-path-input",
                                type="text",
                                value="/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/",
                                placeholder="Enter Databricks volume path",
                                className="input-90s w-100"
                            ),
                            html.Div("Path to Databricks volume where XML file will be written", className="form-text-90s")
                        ], className="col-90s-6"),
                        
                        html.Div([
                            html.Label("File Name", className="label-90s"),
                            dcc.Input(
                                id="filename-input",
                                type="text",
                                value="synthetic-definitely-not-valnav-data.xml",
                                placeholder="Enter filename",
                                className="input-90s w-100"
                            ),
                            html.Div("Name of the XML file to create", className="form-text-90s")
                        ], className="col-90s-6")
                    ], className="row-90s"),
                    
                    # Unity Catalog Configuration
                    html.Div([
                        html.Div([
                            html.Label("Catalog Name", className="label-90s"),
                            dcc.Input(
                                id="catalog-input",
                                type="text",
                                value="harrison_chen_catalog",
                                placeholder="Enter catalog name",
                                className="input-90s w-100"
                            ),
                            html.Div("Unity Catalog name", className="form-text-90s")
                        ], className="col-90s-6"),
                        
                        html.Div([
                            html.Label("Schema Name", className="label-90s"),
                            dcc.Input(
                                id="schema-input",
                                type="text",
                                value="definitely_not_valnav_bronze",
                                placeholder="Enter schema name",
                                className="input-90s w-100"
                            ),
                            html.Div("Schema name for bronze layer data", className="form-text-90s")
                        ], className="col-90s-6")
                    ], className="row-90s")
                ], className="group-box")
            ], className="window-content")
        ], className="window-frame"),
        
        # Action Buttons Window
        html.Div([
            html.Div("System Operations", className="window-header"),
            html.Div([
                html.Div([
                    html.Div("📋 Workflow:", className="text-bold mb-2"),
                    html.Ol([
                        html.Li("Configure your data model parameters above"),
                        html.Li("Generate XML & Write to Volume (creates the XML file)"),
                        html.Li("Create Schema & Tables from XML (reads the XML and creates matching tables)")
                    ], style={"margin": "4px 0"})
                ], className="alert-90s mb-4"),
                
                html.Div([
                    html.Div([
                        html.Button(
                            "🔄 Generate XML & Write to Volume",
                            id="generate-button",
                            className="button-90s button-primary w-100",
                            disabled=False,
                            style={"padding": "8px 16px", "font-size": "12px"}
                        )
                    ], className="col-90s-6"),
                    
                    html.Div([
                        html.Button(
                            "📊 Create Schema & Tables from XML",
                            id="create-schema-button",
                            className="button-90s button-success w-100",
                            disabled=False,
                            style={"padding": "8px 16px", "font-size": "12px"}
                        ),
                        html.Div("Creates tables based on your generated XML file", className="form-text-90s text-center")
                    ], className="col-90s-6")
                ], className="row-90s"),
                
                html.Div([
                    html.Button(
                        "🔍 Preview XML Structure",
                        id="preview-button",
                        className="button-90s w-100",
                        style={"padding": "8px 16px", "font-size": "12px"}
                    )
                ], className="text-center", style={"margin": "8px 0"})
            ], className="window-content")
        ], className="window-frame"),
        
        # Progress and Status Window
        html.Div([
            html.Div("Generation Status", className="window-header"),
            html.Div([
                html.Div([
                    html.Div(id="progress-bar", className="progress-bar-90s"),
                    html.Div(id="status-output", className="status-bar"),
                    html.Hr(),
                    html.Div(id="result-output")
                ])
            ], className="window-content")
        ], className="window-frame"),
        
        # Preview Window
        html.Div([
            html.Div(id="preview-collapse", className="collapse-90s closed", children=[
                html.Div([
                    html.Div("XML Preview", className="window-header"),
                    html.Div([
                        html.Pre(id="xml-preview", className="code-preview")
                    ], className="window-content")
                ], className="window-frame")
            ])
        ])
    ], className="container")
])

# Note: Connection testing is not needed in Databricks Apps environment
# Authentication is handled automatically by the Databricks Apps runtime

# Callback for creating schema and tables from XML
@app.callback(
    [Output("result-output", "children", allow_duplicate=True),
     Output("status-output", "children", allow_duplicate=True)],
    [Input("create-schema-button", "n_clicks")],
    [State("catalog-input", "value"),
     State("schema-input", "value"),
     State("volume-path-input", "value"),
     State("filename-input", "value")],
    prevent_initial_call=True
)
def create_schema_and_tables_callback(n_clicks, catalog_name, schema_name, volume_path, filename):
    if not n_clicks:
        return "", ""
    
    try:
        if not catalog_name or not schema_name:
            return (html.Div("Please enter both catalog name and schema name", className="alert-90s alert-error"), 
                    "Error: Missing catalog or schema name")
        
        if not volume_path or not filename:
            return (html.Div("Please enter volume path and filename. Make sure to generate XML data first!", className="alert-90s alert-warning"), 
                    "Error: Missing volume path or filename")
        
        # Construct full path to XML file
        if not volume_path.endswith('/'):
            volume_path += '/'
        xml_file_path = volume_path + filename
        
        # Create schema and tables based on XML content
        try:
            # Use Databricks runtime context - no credentials needed
            client = DatabricksVolumeClient()
            
            # Test connection first
            connected, conn_message = client.test_connection()
            if not connected:
                return (
                    html.Div(f"❌ Connection failed: {conn_message}", className="alert-90s alert-error"),
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
                    html.H4("✅ Schema & Tables Created from XML!", className="text-bold"),
                    html.P("Successfully created schema and tables based on your generated XML file."),
                    html.P(f"XML File: {xml_file_path}"),
                    html.P(f"Catalog: {catalog_name}"),
                    html.P(f"Schema: {schema_name}"),
                    html.P(f"Created at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"),
                    html.Hr(),
                    html.Pre(message, className="code-preview", style={'height': '150px'})
                ]
                
                result = html.Div(result_children, className="alert-90s alert-success")
                status = "Schema and tables created from XML successfully"
            else:
                result = html.Div([
                    html.H4("❌ Schema Creation from XML Failed", className="text-bold"),
                    html.P(f"Error: {message}"),
                    html.P("Make sure the XML file exists in the volume and you have proper permissions.")
                ], className="alert-90s alert-error")
                status = f"Schema creation from XML failed: {message}"
            
            return result, status
            
        except Exception as e:
            return (
                html.Div([
                    html.H4("❌ Error Creating Schema from XML", className="text-bold"),
                    html.P(f"Error: {str(e)}")
                ], className="alert-90s alert-error"),
                f"Error creating schema from XML: {str(e)}"
            )
        
    except Exception as e:
        return (html.Div([
            html.H4("❌ Error!", className="text-bold"),
            html.P(f"An error occurred: {str(e)}")
        ], className="alert-90s alert-error"), f"Error: {str(e)}")

# Callback for generating XML and writing to volume
@app.callback(
    [Output("progress-bar", "children"),
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
     State("rollups-input", "value"),
     State("history-input", "value"),
     State("schedule-coverage-input", "value"),
     State("volume-path-input", "value"),
     State("filename-input", "value"),
     State("catalog-input", "value"),
     State("schema-input", "value")],
    prevent_initial_call=True
)
def generate_and_write_xml(n_clicks, num_wells, num_facilities, num_scenarios, 
                          num_price_decks, num_companies, num_countries, num_currencies,
                          num_fiscal_regimes, num_meter_stations, num_transportation_areas,
                          num_type_wells, num_tax_pools, num_rollups, history_months, schedule_coverage,
                          volume_path, filename, catalog_name, schema_name):
    if not n_clicks:
        return html.Div(className="progress-bar-fill", style={"width": "0%"}), "", ""
    
    try:
        # Validate inputs
        if not all([num_wells, num_facilities, num_scenarios, num_price_decks]):
            return (html.Div(className="progress-bar-fill", style={"width": "0%"}), 
                    "Error: Please fill in all required fields", 
                    html.Div("Invalid input parameters", className="alert-90s alert-error"))
        
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
            num_rollups=num_rollups,
            history_months=history_months,
            schedule_coverage=schedule_coverage/100
        )
        
        progress = 50
        status = "XML generation complete. Writing to Databricks volume..."
        
        # Construct full file path
        full_path = f"{volume_path.rstrip('/')}/{filename}"
        
        # Write to Databricks volume using runtime context
        success, message = write_to_databricks_volume(
            xml_content=xml_data, 
            volume_path=full_path
        )
        
        if success:
            progress = 100
            status = "Successfully completed!"
            result = html.Div([
                html.H4("✅ Success!", className="text-bold"),
                html.P(f"Generated XML with {num_wells} wells, {num_facilities} facilities, {num_scenarios} scenarios, and {num_price_decks} price decks."),
                html.P(f"File written to: {full_path}"),
                html.P(f"File size: {len(xml_data):,} bytes"),
                html.P(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            ], className="alert-90s alert-success")
        else:
            progress = 0
            status = f"Error writing to volume: {message}"
            result = html.Div([
                html.H4("❌ Error!", className="text-bold"),
                html.P(f"Failed to write file to Databricks volume: {message}")
            ], className="alert-90s alert-error")
        
        return html.Div(className="progress-bar-fill", style={"width": f"{progress}%"}), status, result
        
    except Exception as e:
        return (html.Div(className="progress-bar-fill", style={"width": "0%"}), 
                f"Error: {str(e)}", 
                html.Div([
                    html.H4("❌ Error!", className="text-bold"),
                    html.P(f"An error occurred: {str(e)}")
                ], className="alert-90s alert-error"))

# Callback for XML preview
@app.callback(
    [Output("xml-preview", "children"),
     Output("preview-collapse", "className")],
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
     State("rollups-input", "value"),
     State("history-input", "value"),
     State("schedule-coverage-input", "value")],
    prevent_initial_call=True
)
def preview_xml(n_clicks, num_wells, num_facilities, num_scenarios, 
                num_price_decks, num_companies, num_countries, num_currencies,
                num_fiscal_regimes, num_meter_stations, num_transportation_areas,
                num_type_wells, num_tax_pools, num_rollups, history_months, schedule_coverage):
    if not n_clicks:
        return "", "collapse-90s closed"
    
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
            num_rollups=min(1, num_rollups or 0),
            history_months=min(6, history_months or 6),
            schedule_coverage=0.5
        )
        
        # Truncate for preview
        preview_text = sample_xml[:2000] + "\n\n... (truncated for preview)\n\n" + sample_xml[-500:]
        
        return preview_text, "collapse-90s open"
        
    except Exception as e:
        return f"Error generating preview: {str(e)}", "collapse-90s open"

# Token help is not needed in Databricks Apps environment

if __name__ == "__main__":
    import os
    import sys
    
    # For Databricks Apps deployment
    port = int(os.environ.get("DATABRICKS_APP_PORT", 8050))
    
    # Add logging for debugging
    print(f"🌐 Definitely-Not-ValNav Synthetic Data Generator starting on port {port}...")
    print("💡 Running in Databricks Apps environment with built-in authentication")
    
    # For Databricks Apps, always bind to 0.0.0.0 with the specified port
    app.run_server(
        debug=False,  # Set to False for production in Databricks Apps
        host="0.0.0.0", 
        port=port,
        dev_tools_hot_reload=False  # Disable hot reload in production
    ) 