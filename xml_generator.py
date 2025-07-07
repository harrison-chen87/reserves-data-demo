import random
from datetime import datetime, timedelta

def generate_synthetic_valnav_xml(num_wells=250, num_facilities=15, num_scenarios=2, 
                                 num_price_decks=3, num_companies=5, num_countries=8, 
                                 num_currencies=8, num_fiscal_regimes=3, num_meter_stations=0,
                                 num_transportation_areas=0, num_type_wells=0, num_tax_pools=0,
                                 num_rollups=0, history_months=24, schedule_coverage=0.7):
    """
    Generates a synthetic XML dataset replicating oil reserve evaluation data.

    Args:
        num_wells (int): The number of unique well IDs to generate.
        num_facilities (int): The number of unique facility IDs to generate.
        num_scenarios (int): The number of unique scenario IDs to generate.
        num_price_decks (int): The number of unique price deck IDs to generate.
        num_companies (int): The number of operating companies to generate.
        num_countries (int): The number of countries/regions to generate.
        num_currencies (int): The number of currency types to generate.
        num_fiscal_regimes (int): The number of tax/royalty systems to generate.
        num_meter_stations (int): The number of measurement points to generate.
        num_transportation_areas (int): The number of transport regions to generate.
        num_type_wells (int): The number of well templates to generate.
        num_tax_pools (int): The number of tax calculation pools to generate.
        num_rollups (int): The number of summary aggregations to generate.
        history_months (int): Number of months of production history to generate.
        schedule_coverage (float): Percentage of wells with production schedules (0.0-1.0).

    Returns:
        str: A well-formed XML string containing the synthetic data.
    """
    current_date = datetime.now()
    xml_parts = []

    # XML Declaration and Root
    xml_parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    xml_parts.append(f'<ProjectData generatedBy="DashSynthGen" exportDate="{current_date.isoformat()}Z">')

    # --- Generate Price Decks ---
    price_deck_ids = []
    commodities = [
        {"name": "Oil", "unit": "USD/bbl", "base_price": 70.0, "volatility": 15.0},
        {"name": "Gas", "unit": "USD/Mcf", "base_price": 3.0, "volatility": 1.0},
        {"name": "NGLs", "unit": "USD/bbl", "base_price": 35.0, "volatility": 8.0}
    ]
    
    price_deck_names = [
        "Conservative Base Case", "Optimistic Forecast", "Pessimistic Scenario",
        "High Volatility Case", "Low Price Environment", "Recovery Scenario",
        "Stress Test Case", "Economic Cycle Base", "Long Term Outlook", "Short Term Forecast"
    ]
    
    xml_parts.append('  <PriceDecks>')
    for i in range(1, num_price_decks + 1):
        deck_id = f"PD-{i:03d}"
        price_deck_ids.append(deck_id)
        deck_name = price_deck_names[i-1] if i <= len(price_deck_names) else f"Price Deck {i}"
        
        xml_parts.append(f'    <PriceDeck ID="{deck_id}" Name="{deck_name}" CurrencyID="USD">')
        
        # Generate price commodities
        for commodity in commodities:
            xml_parts.append(f'      <PriceCommodity Commodity="{commodity["name"]}" Unit="{commodity["unit"]}">')
            
            # Generate annual prices with some trend and volatility
            base_price = commodity["base_price"]
            trend = random.uniform(-0.05, 0.05)  # Annual trend
            
            for year_offset in range(6):  # 6 years of price forecasts
                year = 2025 + year_offset
                # Apply trend and random volatility
                price = base_price * (1 + trend * year_offset) * random.uniform(0.8, 1.2)
                xml_parts.append(f'        <AnnualPrice Year="{year}" Value="{price:.2f}"/>')
            
            xml_parts.append('      </PriceCommodity>')
        
        xml_parts.append('    </PriceDeck>')
    xml_parts.append('  </PriceDecks>')

    # --- Generate Scenarios ---
    scenario_ids = []
    scenario_types = ["Deterministic", "Probabilistic", "Monte Carlo", "Sensitivity"]
    scenario_statuses = ["Active", "Under Review", "Approved", "Archived"]
    
    xml_parts.append('  <Scenarios>')
    for i in range(1, num_scenarios + 1):
        scenario_id = f"SCN-{i:03d}"
        scenario_ids.append(scenario_id)
        scenario_name = f"Scenario {i} - {random.choice(['P50', 'P10', 'P90', 'Base', 'Upside', 'Downside'])}"
        price_deck_id = random.choice(price_deck_ids)
        scenario_type = random.choice(scenario_types)
        status = random.choice(scenario_statuses)
        
        description = f"{scenario_name} economic forecast using {price_deck_id} pricing."
        xml_parts.append(f'    <Scenario ID="{scenario_id}" Name="{scenario_name}" PriceDeckID="{price_deck_id}" Type="{scenario_type}" Status="{status}" Description="{description}"/>')
    xml_parts.append('  </Scenarios>')

    # --- Generate Facilities ---
    facility_ids = []
    facility_types = ["Processing Unit", "Gathering Station", "Gas Plant", "Terminal", "Compression Station", "Pumping Station"]
    facility_statuses = ["Operational", "Under Maintenance", "Under Construction", "Planned", "Shut Down"]
    
    xml_parts.append('  <Facilities>')
    for i in range(1, num_facilities + 1):
        fac_id = f"FAC-{i:03d}"
        facility_ids.append(fac_id)
        fac_name = f"Facility {chr(64 + random.randint(1, 26))}{i:02d}"
        fac_type = random.choice(facility_types)
        location_name = f"Field {chr(64 + random.randint(1, 15))}"
        latitude = round(random.uniform(25.0, 60.0), 4)
        longitude = round(random.uniform(-125.0, -60.0), 4)
        capacity = random.randint(1000, 500000)
        status = random.choice(facility_statuses)

        xml_parts.append(f'    <Facility ID="{fac_id}" Name="{fac_name}" Type="{fac_type}" Location="{location_name}" Latitude="{latitude}" Longitude="{longitude}" Capacity="{capacity}" CapacityUnit="Bbl/day" Status="{status}"/>')
    xml_parts.append('  </Facilities>')

    # --- Generate Wells ---
    xml_parts.append('  <WellsAndGroups>')
    well_ids = []
    well_types = ["Oil Producer", "Gas Producer", "Injector", "Dual Producer", "Exploration"]
    well_statuses = ["Producing", "Shut-in", "Drilling", "Abandoned", "Completed", "Testing"]
    trajectories = ["Horizontal", "Vertical", "Long Reach", "Deviated", "Multilateral"]
    formations = ["Wolfcamp", "Bakken", "Montney", "Eagle Ford", "Permian", "Marcellus", "Utica", "Niobrara"]
    fluid_types = ["Oil", "Gas", "Condensate", "Water", "Mixed"]

    for i in range(1, num_wells + 1):
        well_id = f"WELL-{i:04d}"
        well_ids.append(well_id)
        well_name = f"Well {random.choice(['Alpha', 'Beta', 'Gamma', 'Delta', 'Echo', 'Foxtrot'])}-{i:04d}"
        facility_id = random.choice(facility_ids)
        
        # Generate spud date (up to 20 years old)
        spud_date = (current_date - timedelta(days=random.randint(30, 365*20))).strftime('%Y-%m-%d')
        status = random.choice(well_statuses)
        well_type = random.choice(well_types)

        # Generate production rates based on well type
        if "Oil" in well_type:
            current_oil_rate = round(random.uniform(0, 1000), 2)
            current_gas_rate = round(random.uniform(0, 3), 2)
        elif "Gas" in well_type:
            current_oil_rate = round(random.uniform(0, 50), 2)
            current_gas_rate = round(random.uniform(0, 10), 2)
        elif well_type == "Injector":
            current_oil_rate = 0
            current_gas_rate = 0
        else:  # Dual Producer, Exploration
            current_oil_rate = round(random.uniform(0, 300), 2)
            current_gas_rate = round(random.uniform(0, 5), 2)

        wellbore_depth = random.randint(3000, 25000)
        trajectory = random.choice(trajectories)
        reservoir_formation = random.choice(formations)
        reservoir_fluid_type = random.choice(fluid_types)

        xml_parts.append(f'    <Well ID="{well_id}" Name="{well_name}" Type="{well_type}" FacilityID="{facility_id}" SpudDate="{spud_date}" Status="{status}" CurrentOilRate="{current_oil_rate}" CurrentGasRate="{current_gas_rate}">')
        xml_parts.append(f'      <WellboreData Depth="{wellbore_depth}" Trajectory="{trajectory}"/>')
        xml_parts.append(f'      <ReservoirData Formation="{reservoir_formation}" FluidType="{reservoir_fluid_type}"/>')
        xml_parts.append('    </Well>')

    # Generate well groups
    num_groups = min(5, max(1, num_wells // 50))  # Create groups for large well counts
    for g in range(1, num_groups + 1):
        group_id = f"GRP-{g:03d}"
        group_name = f"Well Group {g}"
        group_type = random.choice(["Development", "Exploration", "Production", "Injection"])
        
        xml_parts.append(f'    <Group ID="{group_id}" Name="{group_name}" Type="{group_type}">')
        
        # Add random wells to the group
        max_wells_in_group = min(20, max(1, len(well_ids) // num_groups))
        num_wells_to_sample = min(max_wells_in_group, len(well_ids))
        group_wells = random.sample(well_ids, num_wells_to_sample)
        for well_id in group_wells:
            xml_parts.append(f'      <MemberWellID>{well_id}</MemberWellID>')
        
        xml_parts.append('    </Group>')

    xml_parts.append('  </WellsAndGroups>')

    # --- Generate Bulk Well Schedules ---
    xml_parts.append('  <BulkWellSchedules>')
    
    # Generate schedules for the specified percentage of wells
    num_scheduled_wells = int(num_wells * schedule_coverage)
    scheduled_wells = random.sample(well_ids, num_scheduled_wells)
    
    for well_id in scheduled_wells:
        # Randomly assign to scenarios
        scenario_id = random.choice(scenario_ids)
        
        # Generate production entries
        for month_offset in range(history_months):
            prod_date = (current_date - timedelta(days=30*month_offset)).strftime('%Y-%m-%d')
            
            # Generate declining production with some volatility
            decline_factor = (1 - month_offset * 0.02) * random.uniform(0.8, 1.2)
            decline_factor = max(0.1, decline_factor)  # Minimum 10% of initial
            
            oil_rate = round(random.uniform(5, 400) * decline_factor, 2)
            gas_rate = round(random.uniform(0.1, 5) * decline_factor, 2)
            water_rate = round(random.uniform(0, 100) * (1 + month_offset * 0.05), 2)  # Water increases over time
            
            xml_parts.append(f'    <WellSchedule WellID="{well_id}" ScenarioID="{scenario_id}" ProductionDate="{prod_date}" OilRate="{oil_rate}" GasRate="{gas_rate}" WaterRate="{water_rate}"/>')
    
    xml_parts.append('  </BulkWellSchedules>')

    # --- Generate Companies ---
    if num_companies > 0:
        xml_parts.append('  <Companies>')
        company_names = ["ExxonMobil", "Shell", "BP", "Chevron", "TotalEnergies", "ConocoPhillips", "ENI", "Equinor", "Suncor", "Imperial Oil", "Husky Energy", "Encana", "Devon Energy", "Pioneer Natural Resources", "EOG Resources"]
        company_types = ["Major", "Independent", "National", "Service"]
        
        for i in range(1, num_companies + 1):
            company_id = f"COMP-{i:03d}"
            company_name = random.choice(company_names) if i <= len(company_names) else f"Company {i}"
            company_type = random.choice(company_types)
            founded_year = random.randint(1950, 2020)
            
            xml_parts.append(f'    <Company ID="{company_id}" Name="{company_name}" Type="{company_type}" Founded="{founded_year}"/>')
        xml_parts.append('  </Companies>')

    # --- Generate Currencies ---
    if num_currencies > 0:
        xml_parts.append('  <Currencies>')
        all_currencies = [
            {"ID": "USD", "Name": "United States Dollar", "Symbol": "$"},
            {"ID": "CAD", "Name": "Canadian Dollar", "Symbol": "C$"},
            {"ID": "EUR", "Name": "Euro", "Symbol": "€"},
            {"ID": "GBP", "Name": "British Pound", "Symbol": "£"},
            {"ID": "JPY", "Name": "Japanese Yen", "Symbol": "¥"},
            {"ID": "AUD", "Name": "Australian Dollar", "Symbol": "A$"},
            {"ID": "CNY", "Name": "Chinese Yuan", "Symbol": "¥"},
            {"ID": "BRL", "Name": "Brazilian Real", "Symbol": "R$"},
            {"ID": "MXN", "Name": "Mexican Peso", "Symbol": "$"},
            {"ID": "NOK", "Name": "Norwegian Krone", "Symbol": "kr"},
            {"ID": "RUB", "Name": "Russian Ruble", "Symbol": "₽"},
            {"ID": "INR", "Name": "Indian Rupee", "Symbol": "₹"},
            {"ID": "SAR", "Name": "Saudi Riyal", "Symbol": "SR"},
            {"ID": "AED", "Name": "UAE Dirham", "Symbol": "AED"},
            {"ID": "KWD", "Name": "Kuwaiti Dinar", "Symbol": "KD"}
        ]
        
        selected_currencies = all_currencies[:num_currencies]
        for currency in selected_currencies:
            xml_parts.append(f'    <Currency ID="{currency["ID"]}" Name="{currency["Name"]}" Symbol="{currency["Symbol"]}"/>')
        xml_parts.append('  </Currencies>')

    # --- Generate Countries ---
    if num_countries > 0:
        xml_parts.append('  <Countries>')
        all_countries = [
            {"ID": "US", "Name": "United States"},
            {"ID": "CA", "Name": "Canada"},
            {"ID": "UK", "Name": "United Kingdom"},
            {"ID": "NO", "Name": "Norway"},
            {"ID": "AU", "Name": "Australia"},
            {"ID": "BR", "Name": "Brazil"},
            {"ID": "MX", "Name": "Mexico"},
            {"ID": "AR", "Name": "Argentina"},
            {"ID": "SA", "Name": "Saudi Arabia"},
            {"ID": "RU", "Name": "Russia"},
            {"ID": "IN", "Name": "India"},
            {"ID": "AE", "Name": "United Arab Emirates"},
            {"ID": "KW", "Name": "Kuwait"},
            {"ID": "QA", "Name": "Qatar"},
            {"ID": "NG", "Name": "Nigeria"}
        ]
        
        selected_countries = all_countries[:num_countries]
        for country in selected_countries:
            xml_parts.append(f'    <Country ID="{country["ID"]}" Name="{country["Name"]}"/>')
        xml_parts.append('  </Countries>')

    # --- Generate Fiscal Regimes ---
    if num_fiscal_regimes > 0:
        xml_parts.append('  <FiscalRegimes>')
        regime_types = ["Royalty", "Production Sharing", "Service Contract", "Tax/Royalty"]
        
        for i in range(1, num_fiscal_regimes + 1):
            regime_id = f"FISC-{i:03d}"
            regime_name = f"Fiscal Regime {i}"
            regime_type = random.choice(regime_types)
            tax_rate = round(random.uniform(10, 50), 2)
            royalty_rate = round(random.uniform(5, 25), 2)
            
            xml_parts.append(f'    <FiscalRegime ID="{regime_id}" Name="{regime_name}" Type="{regime_type}" TaxRate="{tax_rate}" RoyaltyRate="{royalty_rate}"/>')
        xml_parts.append('  </FiscalRegimes>')

    # --- Generate Meter Stations ---
    if num_meter_stations > 0:
        xml_parts.append('  <MeterStations>')
        meter_types = ["Orifice", "Turbine", "Ultrasonic", "Coriolis", "Venturi"]
        
        for i in range(1, num_meter_stations + 1):
            station_id = f"METER-{i:03d}"
            station_name = f"Meter Station {i}"
            meter_type = random.choice(meter_types)
            facility_id = random.choice(facility_ids) if facility_ids else f"FAC-001"
            accuracy = round(random.uniform(0.1, 2.0), 2)
            
            xml_parts.append(f'    <MeterStation ID="{station_id}" Name="{station_name}" Type="{meter_type}" FacilityID="{facility_id}" Accuracy="{accuracy}"/>')
        xml_parts.append('  </MeterStations>')

    # --- Generate Transportation Areas ---
    if num_transportation_areas > 0:
        xml_parts.append('  <TransportationAreas>')
        transport_types = ["Pipeline", "Truck", "Rail", "Marine", "Helicopter"]
        
        for i in range(1, num_transportation_areas + 1):
            area_id = f"TRANS-{i:03d}"
            area_name = f"Transportation Area {i}"
            transport_type = random.choice(transport_types)
            capacity = random.randint(1000, 50000)
            
            xml_parts.append(f'    <TransportationArea ID="{area_id}" Name="{area_name}" Type="{transport_type}" Capacity="{capacity}"/>')
        xml_parts.append('  </TransportationAreas>')

    # --- Generate Type Wells ---
    if num_type_wells > 0:
        xml_parts.append('  <TypeWells>')
        well_categories = ["Vertical Oil", "Horizontal Oil", "Vertical Gas", "Horizontal Gas", "Multilateral"]
        
        for i in range(1, num_type_wells + 1):
            type_id = f"TYPE-{i:03d}"
            type_name = f"Type Well {i}"
            category = random.choice(well_categories)
            drilling_days = random.randint(10, 120)
            completion_cost = random.randint(500000, 5000000)
            
            xml_parts.append(f'    <TypeWell ID="{type_id}" Name="{type_name}" Category="{category}" DrillingDays="{drilling_days}" CompletionCost="{completion_cost}"/>')
        xml_parts.append('  </TypeWells>')

    # --- Generate Tax Pools ---
    if num_tax_pools > 0:
        xml_parts.append('  <TaxPools>')
        pool_types = ["Depletion", "Depreciation", "Exploration", "Development", "Production"]
        
        for i in range(1, num_tax_pools + 1):
            pool_id = f"POOL-{i:03d}"
            pool_name = f"Tax Pool {i}"
            pool_type = random.choice(pool_types)
            balance = round(random.uniform(100000, 10000000), 2)
            
            xml_parts.append(f'    <TaxPool ID="{pool_id}" Name="{pool_name}" Type="{pool_type}" Balance="{balance}"/>')
        xml_parts.append('  </TaxPools>')

    # --- Generate Rollups ---
    if num_rollups > 0:
        xml_parts.append('  <Rollups>')
        rollup_types = ["Production", "Reserves", "Economics", "Facility", "Well"]
        
        for i in range(1, num_rollups + 1):
            rollup_id = f"ROLLUP-{i:03d}"
            rollup_name = f"Rollup {i}"
            rollup_type = random.choice(rollup_types)
            frequency = random.choice(["Daily", "Weekly", "Monthly", "Quarterly", "Annual"])
            
            xml_parts.append(f'    <Rollup ID="{rollup_id}" Name="{rollup_name}" Type="{rollup_type}" Frequency="{frequency}"/>')
        xml_parts.append('  </Rollups>')

    # --- End Root ---
    xml_parts.append('</ProjectData>')

    return "\n".join(xml_parts)


def validate_inputs(num_wells, num_facilities, num_scenarios, num_price_decks, 
                   history_months, schedule_coverage):
    """
    Validates user inputs for the XML generation function.
    
    Returns:
        tuple: (is_valid, error_message)
    """
    errors = []
    
    if not (1 <= num_wells <= 10000):
        errors.append("Number of wells must be between 1 and 10,000")
    
    if not (1 <= num_facilities <= 1000):
        errors.append("Number of facilities must be between 1 and 1,000")
    
    if not (1 <= num_scenarios <= 50):
        errors.append("Number of scenarios must be between 1 and 50")
    
    if not (1 <= num_price_decks <= 20):
        errors.append("Number of price decks must be between 1 and 20")
    
    if not (1 <= history_months <= 120):
        errors.append("Production history must be between 1 and 120 months")
    
    if not (0.1 <= schedule_coverage <= 1.0):
        errors.append("Schedule coverage must be between 10% and 100%")
    
    if num_facilities > num_wells:
        errors.append("Number of facilities cannot exceed number of wells")
    
    if errors:
        return False, "; ".join(errors)
    
    return True, ""


def estimate_file_size(num_wells, num_facilities, num_scenarios, num_price_decks, 
                      history_months, schedule_coverage):
    """
    Estimates the approximate file size of the generated XML.
    
    Returns:
        int: Estimated file size in bytes
    """
    # Base XML structure
    base_size = 2000
    
    # Price decks: ~500 bytes per deck
    price_deck_size = num_price_decks * 500
    
    # Scenarios: ~200 bytes per scenario
    scenario_size = num_scenarios * 200
    
    # Facilities: ~300 bytes per facility
    facility_size = num_facilities * 300
    
    # Wells: ~400 bytes per well
    well_size = num_wells * 400
    
    # Production schedules: ~80 bytes per entry
    schedule_entries = int(num_wells * schedule_coverage * history_months)
    schedule_size = schedule_entries * 80
    
    # Fixed sections (currencies, countries)
    fixed_size = 3000
    
    estimated_size = (base_size + price_deck_size + scenario_size + 
                     facility_size + well_size + schedule_size + fixed_size)
    
    return estimated_size 