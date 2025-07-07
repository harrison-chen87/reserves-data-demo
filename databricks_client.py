import os
import logging
from typing import Tuple, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
import requests
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabricksVolumeClient:
    """
    Client for interacting with Databricks Unity Catalog Volumes.
    """
    
    def __init__(self, workspace_url: Optional[str] = None, token: Optional[str] = None):
        """
        Initialize the Databricks client.
        
        Args:
            workspace_url: Databricks workspace URL (optional, can be set via env var)
            token: Databricks access token (optional, can be set via env var)
        """
        self.workspace_url = workspace_url or os.getenv('DATABRICKS_HOST')
        self.token = token or os.getenv('DATABRICKS_TOKEN')
        
        if not self.workspace_url:
            raise ValueError("Databricks workspace URL must be provided via parameter or DATABRICKS_HOST environment variable")
        
        if not self.token:
            raise ValueError("Databricks access token must be provided via parameter or DATABRICKS_TOKEN environment variable")
        
        # Initialize the Databricks SDK client
        try:
            self.client = WorkspaceClient(
                host=self.workspace_url,
                token=self.token
            )
            logger.info("Successfully initialized Databricks client")
        except Exception as e:
            logger.error(f"Failed to initialize Databricks client: {e}")
            raise
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the connection to Databricks.
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Try to get current user info as a connection test
            current_user = self.client.current_user.me()
            logger.info(f"Successfully connected to Databricks as user: {current_user.user_name}")
            return True, f"Connected as {current_user.user_name}"
        except DatabricksError as e:
            logger.error(f"Connection test failed: {e}")
            return False, f"Connection failed: {e}"
        except Exception as e:
            logger.error(f"Unexpected error during connection test: {e}")
            return False, f"Unexpected error: {e}"
    
    def write_file_to_volume(self, file_content: str, volume_path: str) -> Tuple[bool, str]:
        """
        Write file content to a Databricks volume using the Files API.
        
        Args:
            file_content: The content to write to the file
            volume_path: The full path to the file in the volume (e.g., /Volumes/catalog/schema/volume/file.xml)
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Validate volume path format
            if not volume_path.startswith('/Volumes/'):
                return False, "Volume path must start with '/Volumes/'"
            
            # Use the Files API to write the file
            self.client.files.upload(
                file_path=volume_path,
                contents=file_content.encode('utf-8'),
                overwrite=True
            )
            
            logger.info(f"Successfully wrote file to volume: {volume_path}")
            return True, f"File successfully written to {volume_path}"
            
        except DatabricksError as e:
            logger.error(f"Failed to write file to volume: {e}")
            return False, f"Databricks API error: {e}"
        except Exception as e:
            logger.error(f"Unexpected error writing file to volume: {e}")
            return False, f"Unexpected error: {e}"
    
    def check_volume_exists(self, volume_path: str) -> Tuple[bool, str]:
        """
        Check if a volume path exists and is accessible.
        
        Args:
            volume_path: The volume path to check
        
        Returns:
            Tuple[bool, str]: (exists, message)
        """
        try:
            # Extract volume components from path
            path_parts = volume_path.strip('/').split('/')
            if len(path_parts) < 4 or path_parts[0] != 'Volumes':
                return False, "Invalid volume path format"
            
            catalog_name = path_parts[1]
            schema_name = path_parts[2]
            volume_name = path_parts[3]
            
            # Check if the volume exists
            try:
                volume_info = self.client.volumes.read(
                    full_name=f"{catalog_name}.{schema_name}.{volume_name}"
                )
                logger.info(f"Volume exists: {volume_info.full_name}")
                return True, f"Volume {volume_info.full_name} is accessible"
            except DatabricksError as e:
                if "does not exist" in str(e).lower():
                    return False, f"Volume {catalog_name}.{schema_name}.{volume_name} does not exist"
                else:
                    return False, f"Error accessing volume: {e}"
                    
        except Exception as e:
            logger.error(f"Error checking volume existence: {e}")
            return False, f"Error checking volume: {e}"
    
    def list_volume_contents(self, volume_path: str) -> Tuple[bool, list]:
        """
        List contents of a volume directory.
        
        Args:
            volume_path: The volume directory path
        
        Returns:
            Tuple[bool, list]: (success, list_of_files)
        """
        try:
            files = self.client.files.list_directory_contents(directory_path=volume_path)
            file_list = []
            for file_info in files:
                file_list.append({
                    'name': file_info.name,
                    'path': file_info.path,
                    'is_directory': file_info.is_directory,
                    'file_size': file_info.file_size
                })
            
            logger.info(f"Listed {len(file_list)} items in {volume_path}")
            return True, file_list
            
        except DatabricksError as e:
            logger.error(f"Failed to list volume contents: {e}")
            return False, []
        except Exception as e:
            logger.error(f"Unexpected error listing volume contents: {e}")
            return False, []

    def create_valnav_schema_from_xml(self, catalog_name: str, schema_name: str, volume_path: str) -> Tuple[bool, str]:
        """
        Create the schema and tables for the ValNav data model based on XML content in volume.
        
        Args:
            catalog_name: Name of the Unity Catalog
            schema_name: Name of the schema to create
            volume_path: Path to the XML file in the volume
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Use SDK approach to read XML and create schema
            return self._create_valnav_schema_from_xml_sdk(catalog_name, schema_name, volume_path)
            
        except Exception as e:
            logger.error(f"Error creating schema and tables from XML: {e}")
            return False, f"Error creating schema from XML: {e}"
    
    def create_valnav_schema(self, catalog_name: str, schema_name: str) -> Tuple[bool, str]:
        """
        Create the schema and tables for the ValNav data model (legacy method).
        
        Args:
            catalog_name: Name of the Unity Catalog
            schema_name: Name of the schema to create
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Use SDK approach first (more reliable for schema operations)
            return self._create_valnav_schema_sdk(catalog_name, schema_name)
            
        except Exception as e:
            logger.error(f"Error creating schema and tables: {e}")
            return False, f"Error creating schema: {e}"
    
    def _create_valnav_schema_from_xml_sdk(self, catalog_name: str, schema_name: str, volume_path: str) -> Tuple[bool, str]:
        """
        Create schema and tables using Databricks SDK based on XML content.
        """
        try:
            schema_full_name = f"{catalog_name}.{schema_name}"
            
            # Step 1: Read XML file from volume
            logger.info(f"Reading XML file from volume: {volume_path}")
            xml_content = self._read_file_from_volume(volume_path)
            if not xml_content:
                return False, f"Failed to read XML file from volume: {volume_path}"
            
            # Step 2: Parse XML to determine which entities are present
            logger.info("Parsing XML to determine entity structure...")
            entities_present = self._parse_xml_entities(xml_content)
            logger.info(f"Found entities in XML: {list(entities_present.keys())}")
            
            # Step 3: Create the schema
            try:
                logger.info(f"Creating schema: {schema_full_name}")
                self.client.schemas.create(
                    name=schema_name,
                    catalog_name=catalog_name,
                    comment=f"ValNav synthetic data schema created from XML file"
                )
                logger.info(f"✅ Created schema: {schema_full_name}")
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info(f"✅ Schema {schema_full_name} already exists")
                else:
                    logger.error(f"❌ Error creating schema: {e}")
                    return False, f"Failed to create schema: {e}"
            
            # Step 4: Create tables only for entities present in XML
            logger.info("Creating tables based on XML content...")
            table_definitions = self._get_table_definitions_for_entities(catalog_name, schema_name, entities_present)
            
            created_tables = []
            failed_tables = []
            
            # Check if SQL execution is available
            try:
                warehouse_id = self._get_default_warehouse_id()
                logger.info(f"Using warehouse ID: {warehouse_id}")
                sql_execution_available = True
            except Exception as e:
                logger.warning(f"SQL execution not available: {e}")
                sql_execution_available = False
            
            if sql_execution_available and table_definitions:
                # Create tables using SQL execution
                for entity_name, table_def in table_definitions.items():
                    try:
                        logger.info(f"Creating table for entity: {entity_name}")
                        
                        # Execute SQL using statement execution API
                        result = self.client.statement_execution.execute_statement(
                            statement=table_def,
                            warehouse_id=warehouse_id
                        )
                        
                        # Wait for completion
                        self._wait_for_statement_completion(result.statement_id)
                        
                        table_name = f"{catalog_name}.{schema_name}.{entity_name}"
                        logger.info(f"✅ Created table: {table_name}")
                        created_tables.append(entity_name)
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to create table for {entity_name}: {e}")
                        failed_tables.append(f"{entity_name}: {str(e)}")
            
            # Step 5: Optionally populate tables with XML data
            populated_tables = []
            if sql_execution_available and created_tables:
                logger.info("Populating tables with XML data...")
                for entity_name in created_tables:
                    try:
                        if entity_name in entities_present:
                            insert_statements = self._generate_insert_statements(
                                catalog_name, schema_name, entity_name, entities_present[entity_name]
                            )
                            
                            for insert_sql in insert_statements:
                                result = self.client.statement_execution.execute_statement(
                                    statement=insert_sql,
                                    warehouse_id=warehouse_id
                                )
                                self._wait_for_statement_completion(result.statement_id)
                            
                            populated_tables.append(entity_name)
                            logger.info(f"✅ Populated table: {entity_name}")
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to populate table {entity_name}: {e}")
            
            # Prepare success message
            success_message = f"✅ Schema {schema_full_name} created from XML!"
            
            if sql_execution_available:
                if created_tables:
                    success_message += f"\n\n📋 Successfully created {len(created_tables)} tables based on XML content:"
                    for table in created_tables:
                        success_message += f"\n   • {table}"
                
                if populated_tables:
                    success_message += f"\n\n📊 Successfully populated {len(populated_tables)} tables with XML data:"
                    for table in populated_tables:
                        success_message += f"\n   • {table}"
                
                if failed_tables:
                    success_message += f"\n\n⚠️ Failed to create {len(failed_tables)} tables:"
                    for table in failed_tables:
                        success_message += f"\n   • {table}"
                        
                success_message += f"\n\n💡 Tables created match exactly what's in your generated XML file!"
            else:
                success_message += f"\n\n💡 SQL execution not available. Use the notebook to create tables for: {list(entities_present.keys())}"
            
            return True, success_message
            
        except Exception as e:
            logger.error(f"Error in XML-based schema creation: {e}")
            return False, f"Error creating schema from XML: {e}"
    
    def _create_valnav_schema_sdk(self, catalog_name: str, schema_name: str) -> Tuple[bool, str]:
        """
        Create schema and tables using Databricks SDK.
        """
        try:
            schema_full_name = f"{catalog_name}.{schema_name}"
            
            # Step 1: Create the schema
            try:
                logger.info(f"Creating schema: {schema_full_name}")
                self.client.schemas.create(
                    name=schema_name,
                    catalog_name=catalog_name,
                    comment=f"ValNav synthetic data schema created by Dash app"
                )
                logger.info(f"✅ Created schema: {schema_full_name}")
                schema_created = True
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info(f"✅ Schema {schema_full_name} already exists")
                    schema_created = True
                else:
                    logger.error(f"❌ Error creating schema: {e}")
                    return False, f"Failed to create schema: {e}"
            
            # Step 2: Create tables using SQL execution
            logger.info("Creating tables using SQL execution...")
            table_definitions = self._get_table_definitions(catalog_name, schema_name)
            
            created_tables = []
            failed_tables = []
            
            # Check if SQL execution is available
            try:
                warehouse_id = self._get_default_warehouse_id()
                logger.info(f"Using warehouse ID: {warehouse_id}")
                sql_execution_available = True
            except Exception as e:
                logger.warning(f"SQL execution not available: {e}")
                sql_execution_available = False
            
            if sql_execution_available:
                # Create tables using SQL execution
                for table_def in table_definitions:
                    try:
                        # Extract table name from SQL for logging
                        table_name = self._extract_table_name_from_sql(table_def)
                        logger.info(f"Creating table: {table_name}")
                        
                        # Execute SQL using statement execution API
                        result = self.client.statement_execution.execute_statement(
                            statement=table_def,
                            warehouse_id=warehouse_id
                        )
                        
                        # Wait for completion
                        self._wait_for_statement_completion(result.statement_id)
                        
                        logger.info(f"✅ Created table: {table_name}")
                        created_tables.append(table_name)
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to create table {table_name}: {e}")
                        failed_tables.append(f"{table_name}: {str(e)}")
            
            # Prepare success message
            success_message = f"✅ Schema {schema_full_name} created successfully!"
            
            if sql_execution_available:
                if created_tables:
                    success_message += f"\n\n📋 Successfully created {len(created_tables)} tables:"
                    for table in created_tables:
                        success_message += f"\n   • {table}"
                
                if failed_tables:
                    success_message += f"\n\n⚠️ Failed to create {len(failed_tables)} tables:"
                    for table in failed_tables:
                        success_message += f"\n   • {table}"
                    success_message += f"\n\n💡 Use the provided notebook file 'create_tables_notebook.py' to create remaining tables manually."
            else:
                # SQL execution not available, provide manual instructions
                success_message += f"\n\n💡 SQL execution not available (no running warehouses)."
                success_message += f"\n\n📋 To create tables, use one of these methods:"
                success_message += f"\n   1. Use the provided notebook file 'create_tables_notebook.py'"
                success_message += f"\n   2. Start a SQL warehouse and run this button again"
                success_message += f"\n   3. Run SQL commands manually in Databricks SQL Editor"
                success_message += f"\n\nTotal tables to create: {len(table_definitions)}"
            
            return True, success_message
            
        except Exception as e:
            logger.error(f"Error in SDK schema creation: {e}")
            return False, f"Error creating schema: {e}"
    
    def _get_warehouses_info(self) -> str:
        """Get available SQL warehouses for the user."""
        try:
            warehouses = self.client.warehouses.list()
            available_warehouses = []
            
            for warehouse in warehouses:
                if warehouse.state and warehouse.state.value == "RUNNING":
                    available_warehouses.append(f"{warehouse.name} (ID: {warehouse.id})")
            
            if available_warehouses:
                return f"Available SQL Warehouses: {', '.join(available_warehouses[:3])}"
            else:
                return "No running SQL warehouses found. Please start a warehouse in your Databricks workspace."
                
        except Exception as e:
            logger.info(f"Could not retrieve warehouse info: {e}")
            return "Use any available SQL warehouse or cluster"
    
    def _get_default_warehouse_id(self) -> str:
        """Get the first available SQL warehouse ID."""
        try:
            warehouses = self.client.warehouses.list()
            
            # Try to find a running warehouse first
            for warehouse in warehouses:
                if warehouse.state and warehouse.state.value == "RUNNING":
                    logger.info(f"Using running warehouse: {warehouse.name} (ID: {warehouse.id})")
                    return warehouse.id
            
            # If no running warehouse, use the first available one
            for warehouse in warehouses:
                if warehouse.state and warehouse.state.value in ["STOPPED", "STARTING"]:
                    logger.info(f"Using warehouse: {warehouse.name} (ID: {warehouse.id})")
                    return warehouse.id
            
            # If no warehouses available, raise an error
            raise Exception("No SQL warehouses available")
            
        except Exception as e:
            logger.error(f"Could not get warehouse ID: {e}")
            raise Exception(f"Failed to get warehouse ID: {e}")
    
    def _extract_table_name_from_sql(self, sql_statement: str) -> str:
        """Extract table name from CREATE TABLE SQL statement."""
        try:
            # Find the table name after "CREATE TABLE IF NOT EXISTS"
            import re
            match = re.search(r'CREATE TABLE IF NOT EXISTS\s+([^\s(]+)', sql_statement, re.IGNORECASE)
            if match:
                return match.group(1)
            else:
                return "unknown_table"
        except Exception:
            return "unknown_table"
    
    def _wait_for_statement_completion(self, statement_id: str, timeout: int = 300) -> None:
        """Wait for SQL statement to complete execution."""
        import time
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                result = self.client.statement_execution.get_statement(statement_id)
                
                if result.status and result.status.state:
                    state = result.status.state.value
                    
                    if state == "SUCCEEDED":
                        logger.info(f"Statement {statement_id} completed successfully")
                        return
                    elif state in ["FAILED", "CANCELED"]:
                        error_msg = result.status.error if result.status.error else "Unknown error"
                        raise Exception(f"Statement failed: {error_msg}")
                    elif state in ["PENDING", "RUNNING"]:
                        # Still running, wait and check again
                        time.sleep(2)
                        continue
                    else:
                        raise Exception(f"Unknown statement state: {state}")
                else:
                    # No status available, wait and check again
                    time.sleep(2)
                    continue
                    
            except Exception as e:
                if "Statement failed" in str(e):
                    raise  # Re-raise statement execution errors
                else:
                    # Other errors (like API errors), wait and retry
                    time.sleep(2)
                    continue
        
        raise Exception(f"Statement execution timeout after {timeout} seconds")
    
    def _read_file_from_volume(self, volume_path: str) -> str:
        """Read file content from Databricks volume."""
        try:
            logger.info(f"Reading file from volume: {volume_path}")
            response = self.client.files.download(file_path=volume_path)
            
            # The DownloadResponse object has a 'contents' attribute which is a StreamingResponse
            if hasattr(response, 'contents'):
                contents = response.contents
                
                # Handle different types of content
                if isinstance(contents, bytes):
                    return contents.decode('utf-8')
                elif isinstance(contents, str):
                    return contents
                else:
                    # It's likely a StreamingResponse object
                    # Try different ways to read the streaming response
                    if hasattr(contents, 'content'):
                        # If it has a content attribute
                        content_data = contents.content
                        if isinstance(content_data, bytes):
                            return content_data.decode('utf-8')
                        elif isinstance(content_data, str):
                            return content_data
                    
                    if hasattr(contents, 'read'):
                        # If it has a read method
                        content_data = contents.read()
                        if isinstance(content_data, bytes):
                            return content_data.decode('utf-8')
                        elif isinstance(content_data, str):
                            return content_data
                    
                    if hasattr(contents, 'iter_content'):
                        # If it supports iteration
                        content_bytes = b""
                        for chunk in contents.iter_content(chunk_size=8192):
                            content_bytes += chunk
                        return content_bytes.decode('utf-8')
                    
                    # Try to iterate directly
                    try:
                        content_bytes = b""
                        for chunk in contents:
                            content_bytes += chunk
                        return content_bytes.decode('utf-8')
                    except:
                        pass
                    
                    # Last resort: try to get text directly
                    if hasattr(contents, 'text'):
                        return contents.text
                    
                    logger.error(f"Could not read StreamingResponse content. Type: {type(contents)}, Attributes: {dir(contents)}")
                    return ""
            else:
                logger.error(f"DownloadResponse object does not have 'contents' attribute")
                return ""
                
        except Exception as e:
            logger.error(f"Failed to read file from volume: {e}")
            logger.error(f"Response type: {type(response)}")
            if hasattr(response, 'contents'):
                logger.error(f"Contents type: {type(response.contents)}")
                logger.error(f"Contents attributes: {dir(response.contents)}")
            return ""
    
    def _parse_xml_entities(self, xml_content: str) -> dict:
        """Parse XML to determine which entities are present and extract their data."""
        try:
            import xml.etree.ElementTree as ET
            
            # Parse the XML
            root = ET.fromstring(xml_content)
            entities_present = {}
            
            # Define entity mappings
            entity_mappings = {
                'PriceDecks': 'price_decks_metadata',
                'Scenarios': 'scenarios', 
                'Facilities': 'facilities',
                'WellsAndGroups': 'wells',
                'BulkWellSchedules': 'bulk_well_schedules',
                'Currencies': 'currencies',
                'Countries': 'countries',
                'Companies': 'companies',
                'FiscalRegimes': 'fiscal_regimes',
                'MeterStations': 'meter_stations',
                'TransportationAreas': 'transportation_areas',
                'TypeWells': 'type_wells',
                'TaxPools': 'tax_pools',
                'BatchDefinitions': 'batch_definitions',
                'ChangeRecordCategories': 'change_record_categories',
                'CustomDataFields': 'custom_data_fields',
                'Hierarchies': 'hierarchies',
                'Rollups': 'rollups'
            }
            
            # Check which entities are present in the XML
            for xml_element, table_name in entity_mappings.items():
                element = root.find(xml_element)
                if element is not None and len(element) > 0:
                    entities_present[table_name] = self._extract_entity_data(element, xml_element)
                    logger.info(f"Found {len(entities_present[table_name])} records for {table_name}")
            
            # Handle special cases
            
            # PriceDecks also creates price_decks_annual_prices table
            if 'price_decks_metadata' in entities_present:
                price_deck_element = root.find('PriceDecks')
                annual_prices = self._extract_annual_prices_data(price_deck_element)
                if annual_prices:
                    entities_present['price_decks_annual_prices'] = annual_prices
            
            return entities_present
            
        except Exception as e:
            logger.error(f"Failed to parse XML entities: {e}")
            return {}
    
    def _extract_entity_data(self, element, entity_type: str) -> list:
        """Extract data from XML element into list of dictionaries."""
        data = []
        
        try:
            if entity_type == 'WellsAndGroups':
                # Handle wells specifically
                for well in element.findall('Well'):
                    well_data = dict(well.attrib)
                    # Add nested data
                    wellbore = well.find('WellboreData')
                    if wellbore is not None:
                        well_data.update({f"Wellbore{k}": v for k, v in wellbore.attrib.items()})
                    reservoir = well.find('ReservoirData')
                    if reservoir is not None:
                        well_data.update({f"Reservoir{k}": v for k, v in reservoir.attrib.items()})
                    data.append(well_data)
            elif entity_type == 'BulkWellSchedules':
                # Handle bulk well schedules specifically
                for schedule in element.findall('WellSchedule'):
                    schedule_data = dict(schedule.attrib)
                    data.append(schedule_data)
            else:
                # Handle other entities
                for child in element:
                    if child.tag in ['Well', 'Group']:  # Skip groups for now
                        continue
                    child_data = dict(child.attrib)
                    data.append(child_data)
        
        except Exception as e:
            logger.warning(f"Failed to extract data for {entity_type}: {e}")
        
        return data
    
    def _extract_annual_prices_data(self, price_decks_element) -> list:
        """Extract annual prices data from PriceDecks element."""
        annual_prices = []
        
        try:
            for price_deck in price_decks_element.findall('PriceDeck'):
                price_deck_id = price_deck.get('ID', '')
                
                for commodity in price_deck.findall('PriceCommodity'):
                    commodity_name = commodity.get('Name', '')
                    unit = commodity.get('Unit', '')
                    
                    for annual_price in commodity.findall('AnnualPrice'):
                        price_data = {
                            'PriceDeckID': price_deck_id,
                            'Commodity': commodity_name,
                            'Unit': unit,
                            'Year': annual_price.get('Year', ''),
                            'Value': annual_price.get('Value', '')
                        }
                        annual_prices.append(price_data)
        
        except Exception as e:
            logger.warning(f"Failed to extract annual prices data: {e}")
        
        return annual_prices
    
    def _get_table_definitions_for_entities(self, catalog_name: str, schema_name: str, entities_present: dict) -> dict:
        """Get table definitions only for entities present in XML."""
        all_table_definitions = {
            'wells': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.wells (WellID STRING, WellName STRING, WellType STRING, FacilityID STRING, SpudDate DATE, Status STRING, CurrentOilRate DOUBLE, CurrentGasRate DOUBLE, WellboreDepth INTEGER, WellboreTrajectory STRING, ReservoirFormation STRING, ReservoirFluidType STRING) USING DELTA",
            'facilities': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.facilities (FacilityID STRING, FacilityName STRING, FacilityType STRING, Location STRING, Latitude DOUBLE, Longitude DOUBLE, Capacity DOUBLE, CapacityUnit STRING, Status STRING) USING DELTA",
            'scenarios': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.scenarios (ScenarioID STRING, ScenarioName STRING, PriceDeckID STRING, Type STRING, Status STRING, Description STRING) USING DELTA",
            'price_decks_metadata': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.price_decks_metadata (PriceDeckID STRING, PriceDeckName STRING, CurrencyID STRING) USING DELTA",
            'price_decks_annual_prices': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.price_decks_annual_prices (PriceDeckID STRING, Commodity STRING, Unit STRING, Year INTEGER, Value DOUBLE) USING DELTA",
            'bulk_well_schedules': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.bulk_well_schedules (WellID STRING, ScenarioID STRING, ProductionDate DATE, OilRate DOUBLE, GasRate DOUBLE, WaterRate DOUBLE) USING DELTA",
            'currencies': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.currencies (CurrencyID STRING, CurrencyName STRING, Symbol STRING) USING DELTA",
            'countries': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.countries (CountryID STRING, CountryName STRING) USING DELTA",
            'companies': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.companies (CompanyID STRING, CompanyName STRING, CompanyType STRING, Founded INTEGER) USING DELTA",
            'fiscal_regimes': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.fiscal_regimes (FiscalRegimeID STRING, FiscalRegimeName STRING, FiscalRegimeType STRING, TaxRate DOUBLE, RoyaltyRate DOUBLE) USING DELTA",
            'meter_stations': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.meter_stations (MeterStationID STRING, MeterStationName STRING, MeterType STRING, FacilityID STRING, Accuracy DOUBLE) USING DELTA",
            'transportation_areas': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.transportation_areas (TransportationAreaID STRING, TransportationAreaName STRING, TransportType STRING, Capacity INTEGER) USING DELTA",
            'type_wells': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.type_wells (TypeWellID STRING, TypeWellName STRING, Category STRING, DrillingDays INTEGER, CompletionCost INTEGER) USING DELTA",
            'tax_pools': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.tax_pools (TaxPoolID STRING, TaxPoolName STRING, TaxPoolType STRING, Balance DOUBLE) USING DELTA",
            'batch_definitions': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.batch_definitions (BatchDefinitionID STRING, BatchDefinitionName STRING, BatchType STRING, Frequency STRING) USING DELTA",
            'change_record_categories': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.change_record_categories (CategoryID STRING, CategoryName STRING, ChangeType STRING, ApprovalRequired BOOLEAN) USING DELTA",
            'custom_data_fields': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.custom_data_fields (CustomDataFieldID STRING, CustomDataFieldName STRING, FieldType STRING, Required BOOLEAN) USING DELTA",
            'hierarchies': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.hierarchies (HierarchyID STRING, HierarchyName STRING, HierarchyType STRING, Levels INTEGER) USING DELTA",
            'rollups': f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.rollups (RollupID STRING, RollupName STRING, RollupType STRING, Frequency STRING) USING DELTA"
        }
        
        # Return only definitions for entities present in XML
        return {entity: all_table_definitions[entity] for entity in entities_present.keys() if entity in all_table_definitions}
    
    def _generate_insert_statements(self, catalog_name: str, schema_name: str, entity_name: str, entity_data: list) -> list:
        """Generate INSERT statements to populate tables with XML data."""
        insert_statements = []
        
        if not entity_data:
            return insert_statements
        
        try:
            table_name = f"{catalog_name}.{schema_name}.{entity_name}"
            
            # Process data in batches to avoid overly large statements
            batch_size = 100
            for i in range(0, len(entity_data), batch_size):
                batch = entity_data[i:i + batch_size]
                
                # Build VALUES clause
                values_clauses = []
                for record in batch:
                    # Format values based on entity type
                    values = self._format_values_for_entity(entity_name, record)
                    if values:
                        values_clauses.append(f"({values})")
                
                if values_clauses:
                    columns = self._get_column_names_for_entity(entity_name)
                    if columns:
                        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES {', '.join(values_clauses)}"
                    else:
                        insert_sql = f"INSERT INTO {table_name} VALUES {', '.join(values_clauses)}"
                    insert_statements.append(insert_sql)
        
        except Exception as e:
            logger.warning(f"Failed to generate insert statements for {entity_name}: {e}")
        
        return insert_statements
    
    def _format_values_for_entity(self, entity_name: str, record: dict) -> str:
        """Format values for INSERT statement based on entity type."""
        try:
            # Handle each entity type with proper formatting
            if entity_name == 'wells':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', '{record.get('FacilityID', '')}', '{record.get('SpudDate', '')}', '{record.get('Status', '')}', {record.get('CurrentOilRate', 0)}, {record.get('CurrentGasRate', 0)}, {record.get('WellboreDepth', 0)}, '{record.get('WellboreTrajectory', '')}', '{record.get('ReservoirFormation', '')}', '{record.get('ReservoirFluidType', '')}'"
            
            elif entity_name == 'facilities':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', '{record.get('Location', '')}', {record.get('Latitude', 0)}, {record.get('Longitude', 0)}, {record.get('Capacity', 0)}, '{record.get('CapacityUnit', '')}', '{record.get('Status', '')}'"
            
            elif entity_name == 'scenarios':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('PriceDeckID', '')}', '{record.get('Type', '')}', '{record.get('Status', '')}', '{record.get('Description', '')}'"
            
            elif entity_name == 'price_decks_metadata':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('CurrencyID', '')}'"
            
            elif entity_name == 'price_decks_annual_prices':
                commodity = record.get('Commodity', '')
                if commodity is None:
                    commodity = 'NULL'
                else:
                    commodity = f"'{commodity}'"
                return f"'{record.get('PriceDeckID', '')}', {commodity}, '{record.get('Unit', '')}', '{record.get('Year', '')}', '{record.get('Value', 0)}'"
            
            elif entity_name == 'bulk_well_schedules':
                return f"'{record.get('WellID', '')}', '{record.get('ScenarioID', '')}', '{record.get('ProductionDate', '')}', {record.get('OilRate', 0)}, {record.get('GasRate', 0)}, {record.get('WaterRate', 0)}"
            
            elif entity_name == 'currencies':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Symbol', '')}'"
            
            elif entity_name == 'countries':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}'"
            
            elif entity_name == 'companies':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', '{record.get('Founded', '')}'"
            
            elif entity_name == 'fiscal_regimes':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', '{record.get('TaxRate', 0)}', '{record.get('RoyaltyRate', 0)}'"
            
            elif entity_name == 'meter_stations':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', '{record.get('FacilityID', '')}', {record.get('Accuracy', 0)}"
            
            elif entity_name == 'transportation_areas':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', {record.get('Capacity', 0)}"
            
            elif entity_name == 'type_wells':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Category', '')}', {record.get('DrillingDays', 0)}, {record.get('CompletionCost', 0)}"
            
            elif entity_name == 'tax_pools':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', {record.get('Balance', 0)}"
            
            elif entity_name == 'batch_definitions':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', '{record.get('Frequency', '')}'"
            
            elif entity_name == 'change_record_categories':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', {record.get('ApprovalRequired', False)}"
            
            elif entity_name == 'custom_data_fields':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', {record.get('Required', False)}"
            
            elif entity_name == 'hierarchies':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', {record.get('Levels', 0)}"
            
            elif entity_name == 'rollups':
                return f"'{record.get('ID', '')}', '{record.get('Name', '')}', '{record.get('Type', '')}', '{record.get('Frequency', '')}'"
            
            else:
                # Generic formatting for unknown entities - escape strings and handle nulls
                values = []
                for key, value in record.items():
                    if value is None or value == '':
                        values.append('NULL')
                    elif isinstance(value, str):
                        escaped_value = value.replace("'", "''")  # Escape single quotes
                        values.append(f"'{escaped_value}'")
                    elif isinstance(value, bool):
                        values.append('TRUE' if value else 'FALSE')
                    else:
                        values.append(str(value))
                return ', '.join(values)
        except Exception as e:
            logger.warning(f"Failed to format values for {entity_name}: {e}")
            return ""
    
    def _get_column_names_for_entity(self, entity_name: str) -> str:
        """Get column names for entity table."""
        column_mappings = {
            'wells': 'WellID, WellName, WellType, FacilityID, SpudDate, Status, CurrentOilRate, CurrentGasRate, WellboreDepth, WellboreTrajectory, ReservoirFormation, ReservoirFluidType',
            'facilities': 'FacilityID, FacilityName, FacilityType, Location, Latitude, Longitude, Capacity, CapacityUnit, Status',
            'scenarios': 'ScenarioID, ScenarioName, PriceDeckID, Type, Status, Description',
            'price_decks_metadata': 'PriceDeckID, PriceDeckName, CurrencyID',
            'price_decks_annual_prices': 'PriceDeckID, Commodity, Unit, Year, Value',
            'bulk_well_schedules': 'WellID, ScenarioID, ProductionDate, OilRate, GasRate, WaterRate',
            'currencies': 'CurrencyID, CurrencyName, Symbol',
            'countries': 'CountryID, CountryName',
            'companies': 'CompanyID, CompanyName, CompanyType, Founded',
            'fiscal_regimes': 'FiscalRegimeID, FiscalRegimeName, FiscalRegimeType, TaxRate, RoyaltyRate',
            'meter_stations': 'MeterStationID, MeterStationName, MeterType, FacilityID, Accuracy',
            'transportation_areas': 'TransportationAreaID, TransportationAreaName, TransportType, Capacity',
            'type_wells': 'TypeWellID, TypeWellName, Category, DrillingDays, CompletionCost',
            'tax_pools': 'TaxPoolID, TaxPoolName, TaxPoolType, Balance',
            'batch_definitions': 'BatchDefinitionID, BatchDefinitionName, BatchType, Frequency',
            'change_record_categories': 'CategoryID, CategoryName, ChangeType, ApprovalRequired',
            'custom_data_fields': 'CustomDataFieldID, CustomDataFieldName, FieldType, Required',
            'hierarchies': 'HierarchyID, HierarchyName, HierarchyType, Levels',
            'rollups': 'RollupID, RollupName, RollupType, Frequency'
        }
        return column_mappings.get(entity_name, '')
    
    def _get_table_definitions(self, catalog_name: str, schema_name: str) -> list:
        """Get SQL table definition statements."""
        return [
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.wells (WellID STRING, WellName STRING, WellType STRING, FacilityID STRING, SpudDate DATE, Status STRING, CurrentOilRate DOUBLE, CurrentGasRate DOUBLE, WellboreDepth INTEGER, WellboreTrajectory STRING, ReservoirFormation STRING, ReservoirFluidType STRING) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.facilities (FacilityID STRING, FacilityName STRING, FacilityType STRING, Location STRING, Latitude DOUBLE, Longitude DOUBLE, Capacity DOUBLE, CapacityUnit STRING, Status STRING) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.scenarios (ScenarioID STRING, ScenarioName STRING, PriceDeckID STRING, Type STRING, Status STRING, Description STRING) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.price_decks_metadata (PriceDeckID STRING, PriceDeckName STRING, CurrencyID STRING) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.price_decks_annual_prices (PriceDeckID STRING, Commodity STRING, Unit STRING, Year INTEGER, Value DOUBLE) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.bulk_well_schedules (WellID STRING, ScenarioID STRING, ProductionDate DATE, OilRate DOUBLE, GasRate DOUBLE, WaterRate DOUBLE) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.currencies (CurrencyID STRING, CurrencyName STRING, Symbol STRING) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.countries (CountryID STRING, CountryName STRING) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.companies (CompanyID STRING, CompanyName STRING, CompanyType STRING, Founded INTEGER) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.fiscal_regimes (FiscalRegimeID STRING, FiscalRegimeName STRING, FiscalRegimeType STRING, TaxRate DOUBLE, RoyaltyRate DOUBLE) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.meter_stations (MeterStationID STRING, MeterStationName STRING, MeterType STRING, FacilityID STRING, Accuracy DOUBLE) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.transportation_areas (TransportationAreaID STRING, TransportationAreaName STRING, TransportType STRING, Capacity INTEGER) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.type_wells (TypeWellID STRING, TypeWellName STRING, Category STRING, DrillingDays INTEGER, CompletionCost INTEGER) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.tax_pools (TaxPoolID STRING, TaxPoolName STRING, TaxPoolType STRING, Balance DOUBLE) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.batch_definitions (BatchDefinitionID STRING, BatchDefinitionName STRING, BatchType STRING, Frequency STRING) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.change_record_categories (CategoryID STRING, CategoryName STRING, ChangeType STRING, ApprovalRequired BOOLEAN) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.custom_data_fields (CustomDataFieldID STRING, CustomDataFieldName STRING, FieldType STRING, Required BOOLEAN) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.hierarchies (HierarchyID STRING, HierarchyName STRING, HierarchyType STRING, Levels INTEGER) USING DELTA",
            
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.rollups (RollupID STRING, RollupName STRING, RollupType STRING, Frequency STRING) USING DELTA"
        ]
    



# Convenience functions for the Dash app
def write_to_databricks_volume(xml_content: str, volume_path: str, 
                              workspace_url: Optional[str] = None, 
                              token: Optional[str] = None) -> Tuple[bool, str]:
    """
    Convenience function to write XML content to a Databricks volume.
    
    Args:
        xml_content: The XML content to write
        volume_path: The full path to the file in the volume
        workspace_url: Databricks workspace URL (optional)
        token: Databricks access token (optional)
    
    Returns:
        Tuple[bool, str]: (success, message)
    """
    try:
        client = DatabricksVolumeClient(workspace_url=workspace_url, token=token)
        
        # Test connection first
        connected, conn_message = client.test_connection()
        if not connected:
            return False, f"Connection failed: {conn_message}"
        
        # Write the file
        success, message = client.write_file_to_volume(xml_content, volume_path)
        return success, message
        
    except Exception as e:
        logger.error(f"Error in write_to_databricks_volume: {e}")
        return False, f"Error: {e}"


def validate_databricks_config(workspace_url: Optional[str] = None, 
                              token: Optional[str] = None) -> Tuple[bool, str]:
    """
    Validate Databricks configuration without making API calls.
    
    Args:
        workspace_url: Databricks workspace URL (optional)
        token: Databricks access token (optional)
    
    Returns:
        Tuple[bool, str]: (is_valid, message)
    """
    workspace_url = workspace_url or os.getenv('DATABRICKS_HOST')
    token = token or os.getenv('DATABRICKS_TOKEN')
    
    errors = []
    
    if not workspace_url:
        errors.append("Databricks workspace URL not provided")
    elif not workspace_url.startswith('https://'):
        errors.append("Workspace URL must start with 'https://'")
    
    if not token:
        errors.append("Databricks access token not provided")
    elif len(token) < 10:
        errors.append("Access token appears to be too short")
    
    if errors:
        return False, "; ".join(errors)
    
    return True, "Configuration appears valid"


def create_schema_and_tables(catalog_name: str, schema_name: str,
                           workspace_url: Optional[str] = None, 
                           token: Optional[str] = None) -> Tuple[bool, str]:
    """
    Create the schema and tables for the ValNav data model in Unity Catalog.
    
    Args:
        catalog_name: Name of the Unity Catalog
        schema_name: Name of the schema to create
        workspace_url: Databricks workspace URL (optional)
        token: Databricks access token (optional)
    
    Returns:
        Tuple[bool, str]: (success, message)
    """
    try:
        client = DatabricksVolumeClient(workspace_url=workspace_url, token=token)
        
        # Test connection first
        connected, conn_message = client.test_connection()
        if not connected:
            return False, f"Connection failed: {conn_message}"
        
        # Create schema
        success, message = client.create_valnav_schema(catalog_name, schema_name)
        return success, message
        
    except Exception as e:
        logger.error(f"Error in create_schema_and_tables: {e}")
        return False, f"Error: {e}"


def get_databricks_config_help() -> str:
    """
    Get help text for configuring Databricks connection.
    
    Returns:
        str: Help text with configuration instructions
    """
    return """
    Databricks Configuration Help:
    
    1. Environment Variables (Recommended):
       - Set DATABRICKS_HOST to your workspace URL (e.g., https://your-workspace.cloud.databricks.com)
       - Set DATABRICKS_TOKEN to your personal access token
    
    2. Personal Access Token:
       - Go to your Databricks workspace
       - Click on your profile icon in the top right
       - Select "User Settings"
       - Go to "Developer" tab
       - Click "Manage" next to "Access tokens"
       - Generate a new token
    
    3. Volume Path Format:
       - Must start with /Volumes/
       - Format: /Volumes/catalog_name/schema_name/volume_name/file.xml
       - Example: /Volumes/my_catalog/my_schema/my_volume/data.xml
    
    4. Required Permissions:
       - READ VOLUME on the target volume
       - WRITE VOLUME on the target volume
       - USE CATALOG on the target catalog
       - USE SCHEMA on the target schema
    """ 