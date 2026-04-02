if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def generate_urls_to_load(*args, **kwargs):
    """
    Fuerza bruta: Genera todas las URLs desde 2019 hasta 2026.
    Cero consultas a la base de datos.
    """
    urls_to_load = []
    
    # Rango estricto solicitado: 2019 a 2026
    start_year = 2013
    end_year = 2025
    
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
            urls_to_load.append(url)
            
    print(f"🔥 Se generaron {len(urls_to_load)} URLs estáticas.")
    print(f"Primera URL: {urls_to_load[0]}")
    print(f"Última URL: {urls_to_load[-1]}")
    
    return urls_to_load