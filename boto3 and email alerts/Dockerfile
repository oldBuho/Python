FROM bitnami/spark:3.5

# Copia el archivo requirements.txt dentro del contenedor
COPY requirements.txt /app/requirements.txt

# Instala las librerías de Python especificadas en requirements.txt
RUN pip install -r /app/requirements.txt
