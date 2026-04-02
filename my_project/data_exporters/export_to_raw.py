import math
import pandas as pd
import gc
import time
import requests
import os
import pyarrow.parquet as pq
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(urls, **kwargs) -> None:
    if not urls or not isinstance(urls, list):
        print("Error: No se recibió una lista de URLs válida.")
        return

    # =========================================================
    # FASE 1: Configuracion
    # =========================================================

    schema_name = 'raw'
    table_name = 'ny_taxi_data'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    chunk_size = 100000
    temp_file_path = '/tmp/temp_taxi_month.parquet' 

    # =========================================================
    # FASE 2: PROCESAMIENTO
    # =========================================================
    for url in urls:
        print(f"\n{'='*50}")
        print(f"📥 PROCESANDO: {url.split('/')[-1]}")
        print(f"{'='*50}")
        
        try:
            start_time = time.time()
            
            # 1. Descarga Local
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status() 
                with open(temp_file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192*4):
                        f.write(chunk)
            
            print(f"✔️ Descargado en {time.time() - start_time:.2f}s.")
            
            parquet_file = pq.ParquetFile(temp_file_path)
            parquet_cols = parquet_file.schema.names
            
            # 2. CONEXIÓN CORTAFUEGOS (Se abre y cierra por cada archivo)
            with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
                
                # PREGUNTA CLAVE: ¿La tabla ya existe?
                check_table_query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}');"
                table_exists = loader.load(check_table_query).iloc[0, 0]
                
                if table_exists:
                    # Si ya existe, aplicamos la evolución de esquema
                    cols_df = loader.load(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';")
                    existing_cols = set(cols_df['column_name'].str.lower().tolist())
                    
                    for col in parquet_cols:
                        if col.lower() not in existing_cols:
                            print(f"⚠️ [SCHEMA DRIFT] Nueva columna detectada: '{col}'. Alterando PostgreSQL...")
                            loader.execute(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col} TEXT;")
                else:
                    print("✨ Primera ejecución detectada. La tabla se creará automáticamente en este instante.")

                # 3. Inserción Chunk a Chunk
                total_rows = parquet_file.metadata.num_rows
                total_chunks = math.ceil(total_rows / chunk_size)
                
                print(f"   Insertando {total_rows} filas...")

                for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
                    chunk_start_time = time.time()
                    df_chunk = batch.to_pandas()
                    
                    # Forzar todo a string para evitar choques de tipos
                    for col in df_chunk.columns:
                        df_chunk[col] = df_chunk[col].apply(lambda x: str(x) if pd.notna(x) else None)
                    
                    loader.export(
                        df_chunk,
                        schema_name,
                        table_name,
                        index=False,
                        if_exists='append',
                        chunksize=chunk_size, 
                    )
                    
                    print(f"   [Lote {i + 1}/{total_chunks}] OK en {time.time() - chunk_start_time:.2f}s")
                    
                    del df_chunk
                    gc.collect() 
                
            print(f"🚀 MES COMPLETADO.")
            
        except requests.exceptions.RequestException as req_err:
            print(f"⚠️ Omitido: El archivo {url.split('/')[-1]} no existe (posible mes futuro o no publicado).")
        except Exception as e:
            print(f"❌ Error procesando {url}. Detalle: {e}")
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
    print("\n✅ Carga histórica infalible finalizada.")