from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

# 1. Cambiamos el import al decorador correcto
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

# 2. Usamos el decorador @data_loader
@data_loader
def execute_dimensional_model(*args, **kwargs):
    """
    Ejecuta las transformaciones SQL para poblar hechos y dimensiones
    escribiendo los resultados en el esquema clean.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    query = """
    -- 1. Preparamos el terreno
    CREATE SCHEMA IF NOT EXISTS clean;

    -- Borramos si ya existen para evitar errores al reejecutar (idempotencia)
    DROP TABLE IF EXISTS clean.fact_trips;
    DROP TABLE IF EXISTS clean.dim_pickup_location;
    DROP TABLE IF EXISTS clean.dim_dropoff_location;
    DROP TABLE IF EXISTS clean.dim_vendor;
    DROP TABLE IF EXISTS clean.dim_payment_type;

    -- 2. Creamos la Fact Table (con el doble cast para los enteros)
    CREATE TABLE clean.fact_trips AS
    SELECT 
        vendorid::numeric::int AS vendor_id,
        pulocationid::numeric::int AS pu_location_id,
        dolocationid::numeric::int AS do_location_id,
        payment_type::numeric::int,
        ratecodeid::numeric::int AS rate_code_id,
        tpep_pickup_datetime::timestamp,
        tpep_dropoff_datetime::timestamp,
        passenger_count::numeric::int,
        trip_distance::numeric,
        fare_amount::numeric,
        extra::numeric,
        mta_tax::numeric,
        tip_amount::numeric,
        tolls_amount::numeric,
        improvement_surcharge::numeric,
        total_amount::numeric,
        -- Calculamos la duración directamente en SQL
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime::timestamp - tpep_pickup_datetime::timestamp)) AS trip_duration_seconds
    FROM raw.ny_taxi_data
    WHERE trip_distance::numeric > 0 
      AND passenger_count::numeric > 0;

    -- 3. Creamos Dimensiones de Ubicación (con el doble cast)
    CREATE TABLE clean.dim_pickup_location AS 
    SELECT DISTINCT pulocationid::numeric::int AS location_id FROM raw.ny_taxi_data;

    CREATE TABLE clean.dim_dropoff_location AS 
    SELECT DISTINCT dolocationid::numeric::int AS location_id FROM raw.ny_taxi_data;

    -- 4. Crear las dimensiones maestras de texto
    CREATE TABLE clean.dim_vendor (vendor_id int, vendor_name text);
    INSERT INTO clean.dim_vendor VALUES (1, 'Creative Mobile'), (2, 'VeriFone');

    CREATE TABLE clean.dim_payment_type (payment_type int, payment_type_name text);
    INSERT INTO clean.dim_payment_type VALUES 
    (1, 'Credit Card'), (2, 'Cash'), (3, 'No Charge'), (4, 'Dispute');
    """

# Ejecutamos el query usando los secrets configurados
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as db:
        db.execute(query)
        db.commit()
        print("✅ Transformación dimensional completada. Resultados escritos en el esquema clean.")
        
    return []