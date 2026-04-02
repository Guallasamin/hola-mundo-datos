# 1. Usar la imagen oficial de Mage AI como base
FROM mageai/mageai:latest

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /home/src

# 3. Copiar tu archivo de requerimientos (para pandas, psycopg2, sqlalchemy, etc.)
COPY requirements.txt .

# 4. Instalar las dependencias de Python que necesite tu pipeline
RUN pip3 install --no-cache-dir -r requirements.txt

# 5. Copiar todo el contenido de tu proyecto al contenedor
COPY . /home/src/