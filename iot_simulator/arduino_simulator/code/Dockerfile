FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY main.py .

# Crear un directorio para datos si se usan archivos locales
RUN mkdir -p /data

# Establecer variables de entorno por defecto
ENV FILE_TO_PROCESS=/data/sensor_data.csv
ENV TOPIC=projects/your-project/topics/iot-sensors
ENV INTERVAL_SECONDS=60
ENV PORT=8080

# Exponer el puerto para el servidor Flask
EXPOSE 8080

# Health check mejorado para verificar el servidor Flask
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Comando por defecto
ENTRYPOINT ["python", "main.py"]