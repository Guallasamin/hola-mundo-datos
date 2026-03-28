from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Exporta el dataframe a la base de datos PostgreSQL, 
    dentro del esquema 'raw'.
    """
    schema_name = 'raw'  # Capa cruda requerida
    table_name = 'ny_taxi_data'
    
    # Cargamos las credenciales de forma segura (secrets/env variables)
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Requisito: crear automáticamente el esquema raw si no existe
        loader.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        
        # Cargar los datos a PostgreSQL
        loader.export(
            df,
            schema_name,
            table_name,
            index=False, # No subimos el índice de Pandas
            if_exists='replace', # 'replace' nos sirve para esta prueba inicial
        )