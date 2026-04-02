if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd
from datetime import datetime

@data_loader
def generate_urls_to_load(*args, **kwargs):
    """
    Carga Incremental Dinámica: 
    Lee tu io_config.yaml con variables de entorno, busca la fecha máxima 
    en Postgres y genera URLs hasta la fecha actual menos 2 meses de delay.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    # Como en tu config ya definiste POSTGRES_SCHEMA: raw, puedes llamar 
    # a la tabla como raw.ny_taxi_data o simplemente ny_taxi_data.
    query = """
        SELECT MAX(tpep_pickup_datetime) AS max_date
        FROM raw.ny_taxi_data
    """
    
    start_year = 2023
    start_month = 1
    
    # 1. Intentamos la conexión a Postgres usando tu io_config.yaml
    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            df_existing = loader.load(query)
            
        if not df_existing.empty and pd.notna(df_existing['max_date'].iloc[0]):
            max_date = pd.to_datetime(df_existing['max_date'].iloc[0])
            print(f"📊 Última fecha encontrada en DB: {max_date}")
            
            if max_date.month == 12:
                start_year = max_date.year + 1
                start_month = 1
            else:
                start_year = max_date.year
                start_month = max_date.month + 1
        else:
            print("⚠️ La tabla existe pero está vacía. Empezando desde 2019.")
            
    except Exception as e:
        # AHORA SÍ VEREMOS EL ERROR REAL
        print("⚠️ FALLO EN LA CONEXIÓN O CONSULTA A POSTGRES ⚠️")
        print(f"🛑 DETALLE DEL ERROR: {str(e)}")
        print("👉 Si dice 'relation does not exist', es normal: la tabla aún no existe.")
        print("👉 Si dice 'authentication failed' o algo de 'NoneType', Mage no está leyendo tu archivo .env.")

    hoy = datetime.now()
    limit_month = hoy.month - 2
    limit_year = hoy.year
    
    if limit_month <= 0:
        limit_month += 12
        limit_year -= 1

    print(f"\n🎯 Calculando URLs desde {start_year}-{start_month:02d} hasta {limit_year}-{limit_month:02d}")

    # 3. Generamos las URLs
    urls_to_load = []
    current_year = start_year
    current_month = start_month
    
    while current_year < limit_year or (current_year == limit_year and current_month <= limit_month):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_year}-{current_month:02d}.parquet'
        urls_to_load.append(url)
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    print(f"🔥 Se generaron {len(urls_to_load)} URLs incrementales.")
    if urls_to_load:
        print(f"Primera URL: {urls_to_load[0]}")
        print(f"Última URL: {urls_to_load[-1]}")
    
    return urls_to_load