import pandas as pd
import requests
import io

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Descarga un mes de datos de NY Taxi para prueba, 
    esquivando el bloqueo 403 del servidor.
    """
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    
    # 1. Agregamos un 'User-Agent' para simular que somos un navegador web común
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    # 2. Descargamos el archivo usando requests
    response = requests.get(url, headers=headers)
    response.raise_for_status() # Lanza un error claro si la descarga falla
    
    # 3. Leemos los bytes descargados directamente en Pandas
    df = pd.read_parquet(io.BytesIO(response.content))
    
    # ENVIAMOS SOLO 10 MIL REGISTROS PARA LA PRUEBA LOCAL
    return df.head(10000)