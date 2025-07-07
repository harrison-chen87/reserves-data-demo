# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # ValNav Data Tables Creation
# MAGIC 
# MAGIC This notebook creates the complete set of tables for the ValNav synthetic data model.
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Catalog must exist and you must have CREATE TABLE privileges
# MAGIC - Schema should be created first (or will be created automatically)
# MAGIC 
# MAGIC **Usage:**
# MAGIC 1. Update the catalog_name and schema_name variables below
# MAGIC 2. Run all cells in order
# MAGIC 3. Tables will be created in your Unity Catalog

# COMMAND ----------
# Configuration - UPDATE THESE VALUES
catalog_name = "harrison_chen_catalog"  # Replace with your catalog name
schema_name = "valnav_bronze"           # Replace with your schema name

print(f"Creating tables in: {catalog_name}.{schema_name}")

# COMMAND ----------
# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
print(f"✅ Schema {catalog_name}.{schema_name} is ready")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table Creation

# COMMAND ----------
# Wells table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.wells (
    WellID STRING,
    WellName STRING,
    WellType STRING,
    FacilityID STRING,
    SpudDate DATE,
    Status STRING,
    CurrentOilRate DOUBLE,
    CurrentGasRate DOUBLE,
    WellboreDepth INTEGER,
    WellboreTrajectory STRING,
    ReservoirFormation STRING,
    ReservoirFluidType STRING
) USING DELTA
COMMENT 'Wells data including production rates and geological information'
""")
print("✅ Wells table created/verified")

# COMMAND ----------
# Facilities table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.facilities (
    FacilityID STRING,
    FacilityName STRING,
    FacilityType STRING,
    Location STRING,
    Latitude DOUBLE,
    Longitude DOUBLE,
    Capacity DOUBLE,
    CapacityUnit STRING,
    Status STRING
) USING DELTA
COMMENT 'Facilities data including locations and capacities'
""")
print("✅ Facilities table created/verified")

# COMMAND ----------
# Scenarios table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.scenarios (
    ScenarioID STRING,
    ScenarioName STRING,
    PriceDeckID STRING,
    Type STRING,
    Status STRING,
    Description STRING
) USING DELTA
COMMENT 'Economic scenarios with price deck references'
""")
print("✅ Scenarios table created/verified")

# COMMAND ----------
# Price decks metadata table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.price_decks_metadata (
    PriceDeckID STRING,
    PriceDeckName STRING,
    CurrencyID STRING
) USING DELTA
COMMENT 'Price deck metadata and currency information'
""")
print("✅ Price decks metadata table created/verified")

# COMMAND ----------
# Price decks annual prices table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.price_decks_annual_prices (
    PriceDeckID STRING,
    Commodity STRING,
    Unit STRING,
    Year INTEGER,
    Value DOUBLE
) USING DELTA
COMMENT 'Annual commodity prices by price deck'
""")
print("✅ Price decks annual prices table created/verified")

# COMMAND ----------
# Bulk well schedules table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.bulk_well_schedules (
    WellID STRING,
    ScenarioID STRING,
    ProductionDate DATE,
    OilRate DOUBLE,
    GasRate DOUBLE,
    WaterRate DOUBLE
) USING DELTA
COMMENT 'Historical production data for wells by scenario'
""")
print("✅ Bulk well schedules table created/verified")

# COMMAND ----------
# Currencies table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.currencies (
    CurrencyID STRING,
    CurrencyName STRING,
    Symbol STRING
) USING DELTA
COMMENT 'Currency reference data'
""")
print("✅ Currencies table created/verified")

# COMMAND ----------
# Countries table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.countries (
    CountryID STRING,
    CountryName STRING
) USING DELTA
COMMENT 'Country reference data'
""")
print("✅ Countries table created/verified")

# COMMAND ----------
# Companies table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.companies (
    CompanyID STRING,
    CompanyName STRING,
    CompanyType STRING,
    Founded INTEGER
) USING DELTA
COMMENT 'Operating companies data'
""")
print("✅ Companies table created/verified")

# COMMAND ----------
# Fiscal regimes table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.fiscal_regimes (
    FiscalRegimeID STRING,
    FiscalRegimeName STRING,
    FiscalRegimeType STRING,
    TaxRate DOUBLE,
    RoyaltyRate DOUBLE
) USING DELTA
COMMENT 'Fiscal regimes and tax structures'
""")
print("✅ Fiscal regimes table created/verified")

# COMMAND ----------
# Meter stations table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.meter_stations (
    MeterStationID STRING,
    MeterStationName STRING,
    MeterType STRING,
    FacilityID STRING,
    Accuracy DOUBLE
) USING DELTA
COMMENT 'Meter stations for flow measurement'
""")
print("✅ Meter stations table created/verified")

# COMMAND ----------
# Transportation areas table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.transportation_areas (
    TransportationAreaID STRING,
    TransportationAreaName STRING,
    TransportType STRING,
    Capacity INTEGER
) USING DELTA
COMMENT 'Transportation areas and logistics'
""")
print("✅ Transportation areas table created/verified")

# COMMAND ----------
# Type wells table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.type_wells (
    TypeWellID STRING,
    TypeWellName STRING,
    Category STRING,
    DrillingDays INTEGER,
    CompletionCost INTEGER
) USING DELTA
COMMENT 'Type well templates and drilling parameters'
""")
print("✅ Type wells table created/verified")

# COMMAND ----------
# Tax pools table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.tax_pools (
    TaxPoolID STRING,
    TaxPoolName STRING,
    TaxPoolType STRING,
    Balance DOUBLE
) USING DELTA
COMMENT 'Tax pools for depletion and depreciation'
""")
print("✅ Tax pools table created/verified")

# COMMAND ----------
# Rollups table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.rollups (
    RollupID STRING,
    RollupName STRING,
    RollupType STRING,
    Frequency STRING
) USING DELTA
COMMENT 'Summary rollup definitions'
""")
print("✅ Rollups table created/verified")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------
# Show all tables created
tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
print(f"\n🎉 Successfully created {len(tables)} tables in {catalog_name}.{schema_name}:")
for table in tables:
    print(f"   • {table.tableName}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Generate XML Data**: Use the Dash app to generate synthetic ValNav XML data
# MAGIC 2. **Load Data**: Process the XML file to populate these tables
# MAGIC 3. **Query Data**: Use SQL to analyze the synthetic energy data
# MAGIC 
# MAGIC The tables are now ready to receive data from your ValNav XML files! 