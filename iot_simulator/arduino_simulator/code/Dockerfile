FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Evitar buffering para ver logs inmediatamente en Cloud Logging
ENV PYTHONUNBUFFERED=1

# Comando para ejecutar el script directamente (Cloud Run Job)
CMD ["python", "main.py"]