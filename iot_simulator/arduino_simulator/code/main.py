import pandas as pd
import json
import time
import os
from datetime import datetime
from google.cloud import pubsub_v1
from google.cloud import storage
import logging
import numpy as np
import io

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimulationState:
    """Clase para gestionar el estado de la simulación"""
    def __init__(self):
        self.messages_sent = 0
        self.start_time = datetime.now()
        self.current_row = 0
        self.total_rows = 0
        self.is_running = True
    
    def increment_messages(self):
        self.messages_sent += 1
    
    def update_current_row(self, row, total):
        self.current_row = row
        self.total_rows = total
    
    def get_stats(self):
        """Obtiene estadísticas de la simulación"""
        elapsed_time = datetime.now() - self.start_time
        
        if elapsed_time.total_seconds() > 0:
            messages_per_minute = (self.messages_sent / elapsed_time.total_seconds()) * 60
        else:
            messages_per_minute = 0
        
        return {
            "messages_sent": self.messages_sent,
            "current_row": self.current_row,
            "total_rows": self.total_rows,
            "progress_percentage": (self.current_row / self.total_rows * 100) if self.total_rows > 0 else 0,
            "start_time": self.start_time.isoformat(),
            "elapsed_time_seconds": elapsed_time.total_seconds(),
            "messages_per_minute": messages_per_minute,
            "is_running": self.is_running,
            "timestamp": datetime.now().isoformat()
        }


class IoTSensorSimulator:
    def __init__(self, csv_file, topic_path, use_gcs=False, state=None):
        """
        Inicializa el simulador de sensor IoT
        
        Args:
            csv_file: Ruta al archivo CSV con los datos (local o GCS)
            topic_path: Ruta completa del topic de Pub/Sub
            use_gcs: Si es True, lee el archivo desde Google Cloud Storage
            state: Objeto SimulationState para compartir estado
        """
        self.csv_file = csv_file
        self.topic_path = topic_path
        self.use_gcs = use_gcs
        self.df = None
        self.current_index = 0
        self.location_info = {}
        self.state = state if state else SimulationState()
        
        # Configurar clientes
        self.publisher = pubsub_v1.PublisherClient()
        if use_gcs:
            self.storage_client = storage.Client()
        
        # Cargar y preparar datos
        self.load_and_prepare_data()
        self.state.total_rows = len(self.df)
    
    def load_from_gcs(self, gcs_path):
        """Carga archivo CSV desde Google Cloud Storage"""
        try:
            # Parsear ruta GCS: gs://bucket-name/path/to/file.csv
            if gcs_path.startswith('gs://'):
                gcs_path = gcs_path[5:]
            
            bucket_name, blob_path = gcs_path.split('/', 1)
            
            logger.info(f"Cargando desde GCS: {bucket_name}/{blob_path}")
            
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            # Descargar contenido a memoria
            content = blob.download_as_bytes()
            
            # Leer CSV desde bytes
            df = pd.read_csv(io.BytesIO(content))
            return df
            
        except Exception as e:
            logger.error(f"Error al cargar desde GCS: {str(e)}")
            raise
    
    def load_and_prepare_data(self):
        """Carga el archivo CSV (local o GCS) y extrae información de ubicación"""
        try:
            if self.use_gcs:
                logger.info(f"Cargando datos desde GCS: {self.csv_file}")
                self.df = self.load_from_gcs(self.csv_file)
            else:
                logger.info(f"Cargando datos desde local: {self.csv_file}")
                self.df = pd.read_csv(self.csv_file)
            
            # Buscar información de ubicación
            self.extract_location_info()
            
            logger.info(f"Datos cargados. Total de filas: {len(self.df)}")
            
        except Exception as e:
            logger.error(f"Error al cargar datos: {str(e)}")
            raise
    
    def extract_location_info(self):
        """Extrae información de ubicación del DataFrame"""
        location_row = None
        
        # Buscar fila con información de ubicación
        for i in range(min(20, len(self.df))):  # Buscar en las primeras 20 filas
            row_values = self.df.iloc[i].astype(str).str.lower()
            
            # Verificar si esta fila contiene información de ubicación
            if any(keyword in ' '.join(row_values) for keyword in 
                   ['latitude', 'longitude', 'altitude', 'location.id']):
                
                # Verificar si realmente tiene valores numéricos/strings válidos
                row_dict = self.df.iloc[i].to_dict()
                
                # Extraer valores si existen
                location_data = {}
                for key in ['latitude', 'longitude', 'altitude', 'Location.ID']:
                    if key in row_dict:
                        location_data[key] = row_dict[key]
                    elif key.lower() in row_dict:
                        location_data[key] = row_dict[key.lower()]
                
                # Solo usar si tiene al menos algunos datos válidos
                if location_data:
                    self.location_info = location_data
                    location_row = i
                    logger.info(f"Información de ubicación encontrada en fila {i}: {self.location_info}")
                    break
        
        # Si no se encontró en filas específicas, buscar en columnas
        if not self.location_info:
            self.location_info = {}
            for col in ['latitude', 'longitude', 'altitude', 'Location.ID']:
                if col in self.df.columns:
                    # Tomar el primer valor no nulo
                    value = self.df[col].dropna().iloc[0] if not self.df[col].dropna().empty else None
                    if value is not None:
                        self.location_info[col] = value
        
        # Si aún no hay información, usar valores por defecto
        if not self.location_info:
            logger.warning("No se encontró información de ubicación específica, usando valores por defecto")
            self.location_info = {
                'latitude': 0.0,
                'longitude': 0.0,
                'altitude': 0.0,
                'Location.ID': 'unknown'
            }
    
    def create_sensor_data_json(self, row_data):
        """
        Crea el JSON con la estructura requerida para los datos del sensor
        """
        # Extraer datos de sensores principales
        main_sensors = {
            "BMP280": row_data.get('BMP280', np.nan),
            "SHT31TE": row_data.get('SHT31TE', np.nan),
            "SHT31HE": row_data.get('SHT31HE', np.nan),
            "SHT31TI": row_data.get('SHT31TI', np.nan),
            "SHT31HI": row_data.get('SHT31HI', np.nan),
            "Absolute_humidity": row_data.get('Absolute_humidity', np.nan),
            "Td_deficit": row_data.get('Td_deficit', np.nan)
        }
        
        # Extraer datos de gases contaminantes
        gas_sensors = {
            "CO_A4_P1": row_data.get('CO_A4_P1', np.nan),
            "NO_B4_P1": row_data.get('NO_B4_P1', np.nan),
            "NO2_B43F_P1": row_data.get('NO2_B43F_P1', np.nan),
            "OX_A431_P1": row_data.get('OX_A431_P1', np.nan),
            "D300": row_data.get('D300', np.nan)
        }
        
        # Crear estructura completa del mensaje
        message = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "location_info": self.location_info,
            "main_sensors": main_sensors,
            "gas_sensors": gas_sensors,
            "metadata": {
                "source_file": self.csv_file,
                "row_index": self.current_index,
                "message_number": self.state.messages_sent + 1,
                "total_rows": len(self.df),
                "data_source": "gcs" if self.use_gcs else "local"
            }
        }
        
        # Reemplazar NaN con None para serialización JSON
        def replace_nan(obj):
            if isinstance(obj, dict):
                return {k: replace_nan(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_nan(item) for item in obj]
            elif pd.isna(obj):
                return None
            return obj
        
        return replace_nan(message)
    
    def send_to_pubsub(self, data):
        """
        Envía datos al topic de Pub/Sub
        """
        try:
            # Convertir a JSON
            json_data = json.dumps(data).encode('utf-8')
            
            # Publicar mensaje
            future = self.publisher.publish(self.topic_path, json_data)
            message_id = future.result()
            
            # Actualizar estado
            self.state.increment_messages()
            
            logger.info(f"Mensaje #{self.state.messages_sent} enviado exitosamente. Message ID: {message_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error al enviar a Pub/Sub: {str(e)}")
            return False
    
    def simulate_sensor(self, interval_seconds=60):
        """
        Simula el envío de datos del sensor cada intervalo especificado
        
        Args:
            interval_seconds: Intervalo entre envíos (por defecto 60 segundos)
        """
        logger.info("Iniciando simulación de sensor IoT...")
        logger.info(f"Enviando datos al topic: {self.topic_path}")
        logger.info(f"Intervalo de envío: {interval_seconds} segundos")
        
        try:
            while self.state.is_running:
                if self.current_index >= len(self.df):
                    self.current_index = 0
                    logger.info("Reiniciando ciclo de datos...")
                
                # Obtener datos de la fila actual
                row = self.df.iloc[self.current_index]
                row_dict = row.to_dict()
                
                # Actualizar estado con la fila actual
                self.state.update_current_row(self.current_index, len(self.df))
                
                # Crear JSON estructurado
                sensor_data = self.create_sensor_data_json(row_dict)
                
                # Enviar a Pub/Sub
                logger.info(f"Enviando fila {self.current_index + 1}/{len(self.df)}")
                success = self.send_to_pubsub(sensor_data)
                
                if success:
                    # Incrementar índice
                    self.current_index += 1
                
                # Esperar intervalo especificado
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Simulación detenida por el usuario")
            self.state.is_running = False
        except Exception as e:
            logger.error(f"Error en la simulación: {str(e)}")
            self.state.is_running = False
            raise


def parse_gcs_path(file_path):
    """Determina si la ruta es GCS y la parsea"""
    if file_path.startswith('gs://'):
        return True, file_path
    return False, file_path

from flask import Flask, jsonify
import threading

app = Flask(__name__)

# Variable global para compartir estado
simulation_state = SimulationState()
simulator_instance = None

@app.route('/')
def health_check():
    return 'OK', 200

@app.route('/stats')
def get_stats():
    """Endpoint para obtener estadísticas de la simulación"""
    try:
        stats = simulation_state.get_stats()
        
        # Añadir información adicional si está disponible
        if simulator_instance:
            stats.update({
                "csv_file": simulator_instance.csv_file,
                "topic": simulator_instance.topic_path,
                "use_gcs": simulator_instance.use_gcs
            })
        
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/reset')
def reset_counters():
    """Endpoint para reiniciar los contadores"""
    try:
        global simulation_state
        simulation_state = SimulationState()
        
        if simulator_instance:
            simulator_instance.state = simulation_state
            simulator_instance.current_index = 0
        
        return jsonify({
            "status": "counters_reset",
            "timestamp": datetime.now().isoformat()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def run_simulation_thread(csv_file, topic, interval):
    global simulator_instance, simulation_state
    
    # Determinar si es GCS o local
    use_gcs, processed_path = parse_gcs_path(csv_file)
    
    # Crear y ejecutar simulador
    try:
        simulator_instance = IoTSensorSimulator(
            csv_file=processed_path,
            topic_path=topic,
            use_gcs=use_gcs,
            state=simulation_state
        )
        
        # Ejecutar simulación continua
        simulator_instance.simulate_sensor(interval_seconds=interval)
        
    except Exception as e:
        logger.error(f"Error en la ejecución de la simulación: {str(e)}")
        simulation_state.is_running = False

def main():
    # Obtener variables de entorno
    csv_file = os.environ.get('FILE_TO_PROCESS')
    topic = os.environ.get('TOPIC')
    interval = int(os.environ.get('INTERVAL_SECONDS', '60'))
    port = int(os.environ.get('PORT', 8080))
    
    if not csv_file or not topic:
        logger.error("Las variables de entorno FILE_TO_PROCESS y TOPIC deben estar definidas")
        return
    
    # Iniciar simulación en un hilo separado
    simulation_thread = threading.Thread(
        target=run_simulation_thread, 
        args=(csv_file, topic, interval),
        daemon=True
    )
    simulation_thread.start()
    
    # Iniciar servidor Flask (bloqueante)
    logger.info(f"Iniciando servidor web en puerto {port}")
    logger.info(f"Endpoint de estadísticas disponible en: http://localhost:{port}/stats")
    app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    main()