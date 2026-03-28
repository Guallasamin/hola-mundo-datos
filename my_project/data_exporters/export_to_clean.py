from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(data_dict, **kwargs) -> None:
    """
    Exporta el modelo dimensional (hechos y dimensiones)
    al esquema 'clean' en PostgreSQL.
    """
    # Verificamos que estamos recibiendo el diccionario correcto
    if not isinstance(data_dict, dict):
        raise ValueError("El exporter esperaba un diccionario con DataFrames")

    schema_name = 'clean' # Ahora apuntamos a la capa analítica
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Requisito: crear el esquema clean si no existe
        loader.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        
        # Iteramos sobre el diccionario (ej: nombre_tabla='dim_vendor', df=DataFrame)
        for table_name, df in data_dict.items():
            print(f"Exportando tabla: {schema_name}.{table_name} con {len(df)} registros...")
            
            loader.export(
                df,
                schema_name,
                table_name,
                index=False,
                if_exists='replace', # Usamos replace para esta prueba local
                chunksize=10000
            )
    print("¡Exportación a capa clean completada exitosamente!")