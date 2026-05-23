-- ============================================
-- Скрипт для создания схемы и таблиц в Trino/Iceberg
-- Запуск: trino --catalog iceberg --file create_tables.sql
-- ============================================

-- 1. Создаём схему (namespace) 
CREATE SCHEMA IF NOT EXISTS iceberg.mine;

-- USE iceberg.mine; -- убрано: не поддерживается в Trino через JDBC

-- ============================================
-- 3. Создание всех таблиц
-- ============================================

-- affected_areas - зоны поражения
CREATE TABLE IF NOT EXISTS iceberg.mine.affected_areas (
    area_id VARCHAR,
    incident_id VARCHAR,
    premise_id VARCHAR,
    damage_type VARCHAR,
    damage_description VARCHAR,
    is_primary_blast_zone BOOLEAN,  
    length_m DOUBLE,              
    geo_meta VARCHAR,               
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- air_analysis - анализ воздуха
CREATE TABLE IF NOT EXISTS iceberg.mine.air_analysis (
    sample_id VARCHAR,
    incident_id VARCHAR,
    sample_point VARCHAR,
    sample_dttm VARCHAR,
    co2_percent DOUBLE,
    o2_percent DOUBLE,
    ch4_percent DOUBLE,
    co_percent DOUBLE,
    h2_percent DOUBLE,
    analyst VARCHAR,
    analyst_laboratory VARCHAR,
    r1_o2_co2_ratio DOUBLE,       -- R1 = ΔO₂/ΔCO₂ 
    r2_co_o2_ratio DOUBLE,
    r3_co_co2_ratio DOUBLE,
    delta_o2 DOUBLE,                  -- ΔO₂  = O2_ATM − o2_percent
    delta_co2 DOUBLE,                 -- ΔCO₂ = co2_percent − CO2_ATM
    delta_co DOUBLE,                  -- ΔCO  = co_percent − CO_ATM
    is_oxidation BOOLEAN,             -- R1 < 2.5
    conclusion VARCHAR,
    source_file VARCHAR
    -- _notice_num убран: внутреннее поле парсера, не должно быть в схеме
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- chronology_incident - хронология
CREATE TABLE IF NOT EXISTS iceberg.mine.chronology_incident (
    event_id VARCHAR,
    incident_id VARCHAR,
    event_dttm VARCHAR,
    event_type VARCHAR,
    action_description VARCHAR,
    temperature_c DOUBLE,
    pressure_mmHg DOUBLE,
    humidity_percent DOUBLE,
    source VARCHAR,
    location VARCHAR,
    persons VARCHAR,
    speaker VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- company_description - описание компании
CREATE TABLE IF NOT EXISTS iceberg.mine.company_description (
    company_id VARCHAR,
    company_name VARCHAR,
    mine_name VARCHAR,
    year_commissioned INTEGER,
    annual_production_tons DOUBLE,
    actual_production_tons DOUBLE,
    depth_m DOUBLE,
    employees_count INTEGER,
    gas_hazard_category VARCHAR,
    outburst_hazard VARCHAR,
    source_file VARCHAR,
    description VARCHAR,
    avg_daily_load_lava_48 DOUBLE,
    avg_daily_load_lava_42 DOUBLE,
    production_9months VARCHAR
) WITH (format = 'PARQUET');

-- employee - сотрудники
CREATE TABLE IF NOT EXISTS iceberg.mine.employee (
    employee_id VARCHAR,
    lastname VARCHAR,
    firstname VARCHAR,
    middlename VARCHAR,
    birth_date VARCHAR,
    position VARCHAR,
    department VARCHAR,
    status_at_incident VARCHAR,
    injury_type VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- equipment - оборудование
CREATE TABLE IF NOT EXISTS iceberg.mine.equipment (
    equipment_id VARCHAR,
    equipment_type VARCHAR,
    model VARCHAR,
    serial_number VARCHAR,
    manufacturer VARCHAR,
    year_of_manufacture INTEGER,
    technical_condition VARCHAR,
    spark_safety_class VARCHAR,
    location_at_incident VARCHAR,
    manufacturer_requirements VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- equipment_certificate - сертификаты
CREATE TABLE IF NOT EXISTS iceberg.mine.equipment_certificate (
    certificate_id VARCHAR,
    equipment_id VARCHAR,
    certificate_number VARCHAR,
    certificate_type VARCHAR,
    issuing_body VARCHAR,
    issue_date VARCHAR,
    expiry_date VARCHAR,
    is_valid_at_incident BOOLEAN, 
    product_name VARCHAR,        
    serial_number VARCHAR,         
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- equipment_issue_log - журнал выдачи
CREATE TABLE IF NOT EXISTS iceberg.mine.equipment_issue_log (
    issue_id VARCHAR,
    incident_id VARCHAR,
    equipment_name VARCHAR,
    equipment_type VARCHAR,
    inventory_number VARCHAR,
    quantity INTEGER,
    unit VARCHAR,
    issued_to VARCHAR,
    employee_id VARCHAR,
    position VARCHAR,
    issue_date VARCHAR,
    shift VARCHAR,
    return_date VARCHAR,
    is_returned BOOLEAN,  
    issued_by VARCHAR,
    purpose VARCHAR,
    notes VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- equipment_maintenance - обслуживание
CREATE TABLE IF NOT EXISTS iceberg.mine.equipment_maintenance (
    maintenance_id VARCHAR,
    equipment_id VARCHAR,
    maintenance_date VARCHAR,
    shift VARCHAR,
    operation_type VARCHAR,
    performed_by VARCHAR,
    anomaly_notes VARCHAR,
    is_completed BOOLEAN,
    source_file VARCHAR
    -- _equipment_model убран: внутреннее поле парсера
) WITH (format = 'PARQUET');

-- expert_dictionary - словарь экспертов
CREATE TABLE IF NOT EXISTS iceberg.mine.expert_dictionary (
    expert_id VARCHAR,
    expert_lastname VARCHAR,
    expert_firstname VARCHAR,
    expert_middlename VARCHAR,
    profession VARCHAR,
    category VARCHAR,
    expiry_date VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- gas_analysis - анализ газа
CREATE TABLE IF NOT EXISTS iceberg.mine.gas_analysis (
    measurement_id VARCHAR,
    incident_id VARCHAR,
    location VARCHAR,
    measurement_dttm VARCHAR,
    ch4_percent DOUBLE,
    air_velocity_mps DOUBLE,
    measurement_height_cm DOUBLE,
    is_anomaly BOOLEAN, 
    note VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- geological_structure - геология
CREATE TABLE IF NOT EXISTS iceberg.mine.geological_structure (
    structure_id VARCHAR,
    mine_name VARCHAR,
    panel_name VARCHAR,
    layer_name VARCHAR,
    depth_from_m DOUBLE,
    depth_to_m DOUBLE,
    rock_type VARCHAR,
    thickness_m DOUBLE,
    gas_content_m3_per_ton DOUBLE,
    description VARCHAR,
    is_working_layer BOOLEAN,  
    is_satellite BOOLEAN,     
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- hypotesis_prove_facts - факты гипотез
CREATE TABLE IF NOT EXISTS iceberg.mine.hypotesis_prove_facts (
    fact_id VARCHAR,
    hypotesis_id VARCHAR,
    source_name VARCHAR,
    is_prove BOOLEAN,  -- исправлено INTEGER → BOOLEAN
    fact_text VARCHAR,
    match_type VARCHAR,
    keyword VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- incident_description - описание инцидента
CREATE TABLE IF NOT EXISTS iceberg.mine.incident_description (
    incident_id VARCHAR,
    incident_date VARCHAR,
    incident_time VARCHAR,
    mine_name VARCHAR,
    incident_type VARCHAR,
    fatalities INTEGER,
    injuries INTEGER,
    material_damage VARCHAR,
    brief_description VARCHAR,
    extracted_locations VARCHAR,
    extracted_entities VARCHAR,
    -- добавлены: нужны для расчёта incident_metrics в DAG
    blast_pressure_mpa DOUBLE,
    blast_wave_speed_mps DOUBLE,
    economic_damage DOUBLE,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- inspection_description - описание осмотров
CREATE TABLE IF NOT EXISTS iceberg.mine.inspection_description (
    fact_id VARCHAR,
    incident_id VARCHAR,
    inspection_date VARCHAR,
    fact_description VARCHAR,
    inspector_name VARCHAR,
    location VARCHAR,
    condition_description VARCHAR,
    violations_found VARCHAR,
    equipment_name VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- premise - выработки
CREATE TABLE IF NOT EXISTS iceberg.mine.premise (
    premise_id VARCHAR,
    premise_name VARCHAR,
    premise_type VARCHAR,
    x_coordinate DOUBLE,
    y_coordinate DOUBLE,
    level_m DOUBLE,
    length_m DOUBLE,
    cross_section_m2 DOUBLE,
    company_id VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- premise_parameters - параметры выработок
CREATE TABLE IF NOT EXISTS iceberg.mine.premise_parameters (
    param_id VARCHAR,
    incident_id VARCHAR,
    location VARCHAR,
    measurement_date VARCHAR,
    param_type VARCHAR,
    inert_dust_applied_kg DOUBLE,
    noncombustible_content_percent DOUBLE,
    normative_noncombustible_percent DOUBLE,
    is_compliant BOOLEAN, 
    dust_removal_water_m3 DOUBLE,
    dust_removal_frequency VARCHAR,
    water_spray_present BOOLEAN, 
    water_spray_flow_l_min DOUBLE,
    air_flow_m3_min DOUBLE,
    air_velocity_mps DOUBLE,
    cross_section_m2 DOUBLE,
    ch4_concentration_percent DOUBLE,
    leakage_coefficient DOUBLE,
    distribution_coefficient DOUBLE,
    gas_flow_m3_min DOUBLE,
    ch4_flow_m3_min DOUBLE,
    vacuum_pressure_mmH2O DOUBLE,
    degassing_efficiency_percent DOUBLE,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- regulatory_document - нормативные документы
CREATE TABLE IF NOT EXISTS iceberg.mine.regulatory_document (
    doc_id VARCHAR,
    doc_name VARCHAR,
    doc_number VARCHAR,
    issue_date VARCHAR,
    effective_date VARCHAR,
    normative_value VARCHAR,
    section VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- seismic_event - сейсмика
CREATE TABLE IF NOT EXISTS iceberg.mine.seismic_event (
    event_id VARCHAR,
    event_dttm VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    depth_km DOUBLE,
    magnitude DOUBLE,
    energy_class DOUBLE,
    source VARCHAR,
    source_file VARCHAR,
    distance_to_mine_km DOUBLE
) WITH (format = 'PARQUET');

-- sensor_record - показания датчиков
CREATE TABLE IF NOT EXISTS iceberg.mine.sensor_record (
    record_id VARCHAR,
    incident_id VARCHAR,
    sensor_id VARCHAR,
    sensor_type VARCHAR,
    record_dttm VARCHAR,
    value DOUBLE,
    unit VARCHAR,
    is_critical BOOLEAN,    
    data_quality_flag INTEGER,  -- код качества (0=ok,1=warn,2=err) 
    x_coordinate DOUBLE,
    y_coordinate DOUBLE,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- sensor_reestr - реестр датчиков
CREATE TABLE IF NOT EXISTS iceberg.mine.sensor_reestr (
    sensor_id VARCHAR,
    sensor_type VARCHAR,
    model VARCHAR,
    location VARCHAR,
    measurement_range VARCHAR,
    unit VARCHAR,
    last_calibration_date VARCHAR,
    battery_life_hours DOUBLE,
    actual_battery_time_at_incident DOUBLE,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- witness_statement - показания свидетелей
CREATE TABLE IF NOT EXISTS iceberg.mine.witness_statement (
    statement_id VARCHAR,
    incident_id VARCHAR,
    witness_name VARCHAR,
    statement_datetime VARCHAR,
    testimony_text VARCHAR,
    source_file VARCHAR,
    extracted_persons VARCHAR,
    extracted_locations VARCHAR,
    extracted_organizations VARCHAR,
    extracted_facts VARCHAR,
    extracted_events VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- ============================================
-- 4. Дополнительные таблицы для метрик
-- ============================================

-- expert_conclusion - заключения экспертов
CREATE TABLE IF NOT EXISTS iceberg.mine.expert_conclusion (
    conclusion_id VARCHAR,
    incident_id VARCHAR,
    conclusion_date VARCHAR,
    expert_id VARCHAR,
    fire_origin_location VARCHAR,
    blast_epicenter_location VARCHAR,
    scenario_description VARCHAR,
    root_cause VARCHAR,
    contributing_factors VARCHAR,
    is_confirmed BOOLEAN,  
    report_id VARCHAR,
    confirmed_hypotesis_id VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- hypotesis - гипотезы
CREATE TABLE IF NOT EXISTS iceberg.mine.hypotesis (
    hypothesis_id VARCHAR,
    incident_id VARCHAR,
    hypothesis_type VARCHAR,
    description VARCHAR,
    confidence_score DOUBLE,
    is_confirmed BOOLEAN,  
    actual_flag BOOLEAN,   
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- recommended_actions - рекомендуемые меры
CREATE TABLE IF NOT EXISTS iceberg.mine.recommended_actions (
    measure_id VARCHAR,
    conclusion_id VARCHAR,
    measure_type VARCHAR,
    description VARCHAR,
    responsible_party VARCHAR,
    implementation_deadline VARCHAR,
    status VARCHAR,
    source_file VARCHAR
) WITH (format = 'PARQUET');

-- fire_metrics - метрики пожара (заполняются Spark)
CREATE TABLE IF NOT EXISTS iceberg.mine.fire_metrics (
    metric_id VARCHAR,
    incident_id VARCHAR,
    r1_o2_co2_ratio DOUBLE,       -- R1 = ΔO₂/ΔCO₂ (исправлено: было r1_co2_o2_ratio)
    r2_co_o2_ratio DOUBLE,
    r3_co_co2_ratio DOUBLE,
    delta_o2 DOUBLE,
    delta_co2 DOUBLE,
    delta_co DOUBLE,
    critical_r1_threshold DOUBLE,
    is_oxidation_detected BOOLEAN,
    fire_duration_minutes INTEGER,
    fire_spread_speed_mps DOUBLE,
    max_co_ppm DOUBLE,
    calculation_date TIMESTAMP,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- ventilation_metrics - метрики вентиляции
CREATE TABLE IF NOT EXISTS iceberg.mine.ventilation_metrics (
    metric_id VARCHAR,
    incident_id VARCHAR,
    premise_id VARCHAR,                 -- добавлено: локация замера
    measurement_dttm VARCHAR,           -- добавлено: дата замера
    air_velocity_fact_mps DOUBLE,       -- ν_fact (факт)
    air_velocity_min_mps DOUBLE,
    air_velocity_norm_mps DOUBLE,       -- ν_norm (норматив)
    velocity_deficit_percent DOUBLE,    -- δ_v = max(0,(ν_norm−ν_fact)/ν_norm×100%)
    airflow_in_m3min DOUBLE,            -- Q_in
    airflow_out_m3min DOUBLE,           -- Q_out
    airflow_total_m3min DOUBLE,         -- Q_total
    airflow_lava_m3min DOUBLE,          -- Q_lava
    leakage_coefficient DOUBLE,         -- k_leak = (Q_out−Q_in)/Q_in
    distribution_coefficient DOUBLE,    -- K_distr = Q_lava/Q_total
    is_ventilation_valid BOOLEAN,
    calculation_date TIMESTAMP,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- incident_metrics - общие метрики
CREATE TABLE IF NOT EXISTS iceberg.mine.incident_metrics (
    metric_id VARCHAR,
    incident_id VARCHAR,
    total_victims INTEGER,           
    fatalities_count INTEGER,
    injuries_count INTEGER,
    blast_pressure_mpa DOUBLE,
    blast_wave_speed_mps DOUBLE,
    affected_area_length_m DOUBLE, 
    affected_areas_count INTEGER,
    destroyed_structures_count INTEGER,
    economic_damage DOUBLE,
    is_seismic_event BOOLEAN,    
    calculation_date TIMESTAMP,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- incident_metrics_summary — сводная таблица S(uᵢ) (заполняется Spark)
CREATE TABLE IF NOT EXISTS iceberg.mine.incident_metrics_summary (
    incident_id VARCHAR,
    -- M_inc
    total_victims INTEGER,
    fatalities INTEGER,
    injuries INTEGER,
    affected_area_length_m DOUBLE,
    affected_areas_count INTEGER,
    blast_pressure_mpa DOUBLE,
    blast_wave_speed_mps DOUBLE,
    is_seismic_event BOOLEAN,
    -- M_vent (агрегаты по инциденту)
    avg_delta_v_pct DOUBLE,
    avg_k_leak DOUBLE,
    avg_k_distr DOUBLE,
    ventilation_compliant BOOLEAN,
    -- M_fire (агрегаты по инциденту)
    min_r1 DOUBLE,
    max_r2 DOUBLE,
    oxidation_detected BOOLEAN,
    oxidation_sample_count BIGINT,
    calculated_at TIMESTAMP
) WITH (format = 'PARQUET');

-- degassing_metrics — показатели дегазации (заполняются Spark)
CREATE TABLE IF NOT EXISTS iceberg.mine.degassing_metrics (
    incident_id VARCHAR,
    premise_id VARCHAR,
    measurement_dttm VARCHAR,
    airflow_mix_m3min DOUBLE,
    ch4_percent DOUBLE,
    negative_pressure_mm DOUBLE,
    q_gas_ch4_m3min DOUBLE,         -- Q_газ_CH₄ = расход × CH4/100
    ch4_total_m3min DOUBLE,         -- Q_общ_CH₄
    eta_deg_pct DOUBLE,             -- η_deg = Q_газ_CH₄/Q_общ_CH₄ × 100%
    calculated_at TIMESTAMP,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- dust_metrics — показатели пылевзрывозащиты (заполняются Spark)
CREATE TABLE IF NOT EXISTS iceberg.mine.dust_metrics (
    incident_id VARCHAR,
    premise_id VARCHAR,
    measurement_dttm VARCHAR,
    dust_type VARCHAR,
    c_noncomb_pct DOUBLE,           -- содержание негорючих, %
    c_norm_pct DOUBLE,              -- норматив = 85%
    is_compliant_dust BOOLEAN,      -- C_noncomb ≥ 85%
    cross_section_m2 DOUBLE,
    length_m DOUBLE,
    calculated_at TIMESTAMP,
    source_file VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['incident_id']);

-- ============================================
-- 5. Проверка создания таблиц
-- ============================================

-- SHOW TABLES FROM iceberg.mine; -- убрано: выполняется отдельно через show_tables()