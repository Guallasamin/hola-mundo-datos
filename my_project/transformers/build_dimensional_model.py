import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs):
    """
    Limpia los datos y construye el modelo dimensional.
    """
    # 'data' es el DataFrame que viene de la consulta SQL anterior
    # Mage convierte los nombres de las columnas a minúsculas por defecto en SQL
    df = data.copy()

    # --- 1. LIMPIEZA DE DATOS (Capa Clean) ---
    # Eliminamos duplicados
    df = df.drop_duplicates().reset_index(drop=True)
    
    # Eliminamos filas donde métricas críticas sean nulas (ej. pasajeros o distancia)
    df = df.dropna(subset=['passenger_count', 'trip_distance'])

    # --- 2. CONSTRUCCIÓN DE DIMENSIONES ---
    
    # Dimensión Vendor
    dim_vendor = df[['vendorid']].drop_duplicates().reset_index(drop=True)
    dim_vendor['vendor_name'] = dim_vendor['vendorid'].map({1: 'Creative Mobile', 2: 'VeriFone'})
    
    # Dimensión Payment Type
    dim_payment_type = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_map = {1: 'Credit card', 2: 'Cash', 3: 'No charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}
    dim_payment_type['payment_name'] = dim_payment_type['payment_type'].map(payment_map)

    # --- 3. TABLA DE HECHOS (Fact Table) ---
    
    # Seleccionamos las claves foráneas y las métricas del viaje
    fact_trips = df[[
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'passenger_count', 'trip_distance', 'pulocationid', 'dolocationid',
        'payment_type', 'fare_amount', 'tip_amount', 'total_amount'
    ]].copy()

    # Creamos una clave primaria (surrogate key) para la tabla de hechos
    fact_trips['trip_id'] = fact_trips.index

    # Retornamos un diccionario con nuestras 3 tablas
    # Esto pasará al Exporter para que guarde cada una en PostgreSQL
    return {
        "dim_vendor": dim_vendor,
        "dim_payment_type": dim_payment_type,
        "fact_trips": fact_trips
    }