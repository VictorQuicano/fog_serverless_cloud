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
import sys
import signal

# Configurar logging más detallado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class SimulationState:
    """Clase para gestionar el estado de la simulación"""
    def __init__(self):
        self.messages_sent = 0
        self.start_time = datetime.now()
        self.current_row = 0
        self.total_rows = 0
        self.is_running = True
        self.last_log_time = datetime.now()
        self.log_interval = 60  # Segundos entre logs de estado
        
    def increment_messages(self):
        self.messages_sent += 1
    
    def update_current_row(self, row, total):
        self.current_row = row
        self.total_rows = total
    
    def should_log_stats(self):
        """Determina si es tiempo de registrar estadísticas"""
        current_time = datetime.now()
        elapsed = (current_time - self.last_log_time).total_seconds()
        return elapsed >= self.log_interval
    
    def update_last_log_time(self):
        self.last_log_time = datetime.now()
    
    def get_stats(self):
        """Obtiene estadísticas de la simulación"""
        elapsed_time = datetime.now() - self.start_time
        
        if elapsed_time.total_seconds() > 0:
            messages_per_minute = (self.messages_sent / elapsed_time.total_seconds()) * 60
            messages_per_second = self.messages_sent / elapsed_time.total_seconds()
        else:
            messages_per_minute = 0
            messages_per_second = 0
        
        return {
            "messages_sent": self.messages_sent,
            "current_row": self.current_index,
            "total_rows": self.total_rows,
            "progress_percentage": (self.current_row / self.total_rows * 100) if self.total_rows > 0 else 0,
            "start_time": self.start_time.isoformat(),
            "elapsed_time": str(elapsed_time),
            "elapsed_seconds": elapsed_time.total_seconds(),
            "messages_per_minute": messages_per_minute,
            "messages_per_second": messages_per_second,
            "is_running": self.is_running,
            "timestamp": datetime.now().isoformat()
        }
    
    def log_status_summary(self):
        """Registra un resumen del estado"""
        stats = self.get_stats()
        logger.info("=" * 80)
        logger.info("ESTADO DE SIMULACIÓN")
        logger.info("=" * 80)
        logger.info(f"Tiempo transcurrido: {stats['elapsed_time']}")
        logger.info(f"Mensajes enviados: {stats['messages_sent']}")
        logger.info(f"Progreso: {stats['current_row']}/{stats['total_rows']} ({stats['progress_percentage']:.2f}%)")
        logger.info(f"Velocidad: {stats['messages_per_minute']:.2f} mensajes/minuto ({stats['messages_per_second']:.2f} msg/seg)")
        logger.info(f"Estado: {'ACTIVO' if self.is_running else 'DETENIDO'}")
        logger.info("-" * 80)


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
        self.last_progress_log = 0
        
        # Configurar clientes
        logger.info(f"Inicializando cliente Pub/Sub para topic: {topic_path}")
        self.publisher = pubsub_v1.PublisherClient()
        if use_gcs:
            logger.info("Inicializando cliente Google Cloud Storage")
            self.storage_client = storage.Client()
        
        # Cargar y preparar datos
        self.load_and_prepare_data()
        self.state.total_rows = len(self.df)
        
        # Configurar manejador de señales
        self.setup_signal_handlers()
    
    def setup_signal_handlers(self):
        """Configura manejadores para señales de interrupción"""
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        logger.info("Manejadores de señales configurados (Ctrl+C para detener)")
    
    def handle_signal(self, signum, frame):
        """Maneja señales de terminación"""
        logger.info(f"\n{'='*60}")
        logger.info(f"Señal {signum} recibida. Deteniendo simulación...")
        logger.info(f"{'='*60}")
        self.state.is_running = False
    
    def load_from_gcs(self, gcs_path):
        """Carga archivo CSV desde Google Cloud Storage"""
        try:
            # Parsear ruta GCS: gs://bucket-name/path/to/file.csv
            if gcs_path.startswith('gs://'):
                gcs_path = gcs_path[5:]
            
            bucket_name, blob_path = gcs_path.split('/', 1)
            
            logger.info(f"Cargando desde GCS: bucket={bucket_name}, blob={blob_path}")
            
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            # Verificar si el archivo existe
            if not blob.exists():
                logger.error(f"Archivo no encontrado en GCS: {gcs_path}")
                raise FileNotFoundError(f"Archivo no encontrado en GCS: {gcs_path}")
            
            # Descargar contenido a memoria
            logger.info(f"Descargando archivo desde GCS...")
            content = blob.download_as_bytes()
            logger.info(f"Archivo descargado. Tamaño: {len(content)} bytes")
            
            # Leer CSV desde bytes
            df = pd.read_csv(io.BytesIO(content))
            
            logger.info(f"DataFrame cargado. Columnas: {list(df.columns)}")
            logger.info(f"Filas totales: {len(df)}")
            logger.info(f"Primeras filas:\n{df.head(2)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error al cargar desde GCS: {str(e)}", exc_info=True)
            raise
    
    def load_and_prepare_data(self):
        """Carga el archivo CSV (local o GCS) y extrae información de ubicación"""
        try:
            if self.use_gcs:
                logger.info(f"Cargando datos desde GCS: {self.csv_file}")
                self.df = self.load_from_gcs(self.csv_file)
            else:
                logger.info(f"Cargando datos desde archivo local: {self.csv_file}")
                if not os.path.exists(self.csv_file):
                    logger.error(f"Archivo local no encontrado: {self.csv_file}")
                    raise FileNotFoundError(f"Archivo no encontrado: {self.csv_file}")
                
                self.df = pd.read_csv(self.csv_file)
                logger.info(f"DataFrame cargado. Columnas: {list(self.df.columns)}")
                logger.info(f"Filas totales: {len(self.df)}")
                logger.info(f"Primeras filas:\n{self.df.head(2)}")
            
            # Buscar información de ubicación
            self.extract_location_info()
            
            logger.info(f"Datos cargados exitosamente. Total de filas: {len(self.df)}")
            
        except Exception as e:
            logger.error(f"Error al cargar datos: {str(e)}", exc_info=True)
            raise
    
    def extract_location_info(self):
        """Extrae información de ubicación del DataFrame"""
        location_row = None
        
        logger.info("Extrayendo información de ubicación...")
        
        # Buscar fila con información de ubicación
        for i in range(min(20, len(self.df))):  # Buscar en las primeras 20 filas
            row_values = self.df.iloc[i].astype(str).str.lower()
            
            # Verificar si esta fila contiene información de ubicación
            location_keywords = ['latitude', 'longitude', 'altitude', 'location.id', 'location', 'gps']
            if any(keyword in ' '.join(row_values) for keyword in location_keywords):
                
                # Verificar si realmente tiene valores numéricos/strings válidos
                row_dict = self.df.iloc[i].to_dict()
                
                # Extraer valores si existen
                location_data = {}
                possible_keys = ['latitude', 'longitude', 'altitude', 'Location.ID', 'location_id', 'location']
                
                for key in possible_keys:
                    if key in row_dict:
                        value = row_dict[key]
                        if pd.notna(value):
                            location_data[key] = value
                
                # Solo usar si tiene al menos algunos datos válidos
                if location_data:
                    self.location_info = location_data
                    location_row = i
                    logger.info(f"✓ Información de ubicación encontrada en fila {i}: {self.location_info}")
                    break
        
        # Si no se encontró en filas específicas, buscar en columnas
        if not self.location_info:
            logger.info("Buscando información de ubicación en columnas...")
            self.location_info = {}
            column_locations = []
            
            for col in self.df.columns:
                if any(keyword in col.lower() for keyword in ['lat', 'lon', 'alt', 'location', 'gps']):
                    column_locations.append(col)
            
            if column_locations:
                logger.info(f"Columnas de ubicación encontradas: {column_locations}")
                for col in column_locations:
                    # Tomar el primer valor no nulo
                    non_null_values = self.df[col].dropna()
                    if not non_null_values.empty:
                        value = non_null_values.iloc[0]
                        self.location_info[col] = value
                        logger.info(f"  - {col}: {value}")
        
        # Si aún no hay información, usar valores por defecto
        if not self.location_info:
            logger.warning("⚠ No se encontró información de ubicación específica, usando valores por defecto")
            self.location_info = {
                'latitude': 0.0,
                'longitude': 0.0,
                'altitude': 0.0,
                'Location.ID': 'unknown'
            }
        else:
            logger.info(f"✓ Información de ubicación final: {self.location_info}")
    
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
            json_data = json.dumps(data, indent=2) if self.state.messages_sent % 100 == 0 else json.dumps(data)
            encoded_data = json_data.encode('utf-8')
            
            # Publicar mensaje
            future = self.publisher.publish(self.topic_path, encoded_data)
            message_id = future.result()
            
            # Actualizar estado
            self.state.increment_messages()
            
            # Log detallado cada 10 mensajes, simple para el resto
            if self.state.messages_sent % 10 == 0:
                logger.info(f"✓ Mensaje #{self.state.messages_sent} enviado. ID: {message_id[:20]}...")
                if self.state.messages_sent % 100 == 0:
                    logger.debug(f"Datos enviados:\n{json_data}")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error al enviar a Pub/Sub: {str(e)}")
            logger.error(f"Datos que causaron el error: {data.get('metadata', {})}")
            return False
    
    def test_pubsub_connection(self):
        """Verifica la conexión al topic de Pub/Sub"""
        try:
            logger.info(f"Verificando conexión a Pub/Sub topic: {self.topic_path}")
            
            # Intentar obtener información del topic
            from google.cloud.pubsub_v1 import PublisherClient
            publisher = PublisherClient()
            
            # Verificar que el topic existe
            topic_info = publisher.get_topic(request={"topic": self.topic_path})
            logger.info(f"✓ Conexión a Pub/Sub exitosa. Topic: {topic_info.name}")
            logger.info(f"  - Etiquetas: {topic_info.labels}")
            logger.info(f"  - Schema: {topic_info.schema_settings}")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error de conexión a Pub/Sub: {str(e)}")
            logger.error("  Verifica que:")
            logger.error("  1. El topic exista")
            logger.error("  2. Tienes permisos de publicación")
            logger.error("  3. El formato del topic sea correcto: projects/<project-id>/topics/<topic-name>")
            return False
    
    def simulate_sensor(self, interval_seconds=60):
        """
        Simula el envío de datos del sensor cada intervalo especificado
        
        Args:
            interval_seconds: Intervalo entre envíos (por defecto 60 segundos)
        """
        logger.info("=" * 80)
        logger.info("INICIANDO SIMULACIÓN DE SENSOR IoT")
        logger.info("=" * 80)
        logger.info(f"Topic de destino: {self.topic_path}")
        logger.info(f"Archivo de datos: {self.csv_file}")
        logger.info(f"Filas totales: {len(self.df)}")
        logger.info(f"Intervalo de envío: {interval_seconds} segundos")
        logger.info(f"Fuente de datos: {'GCS' if self.use_gcs else 'Local'}")
        logger.info("-" * 80)
        logger.info("Información de ubicación detectada:")
        for key, value in self.location_info.items():
            logger.info(f"  - {key}: {value}")
        logger.info("=" * 80)
        
        cycle_count = 0
        start_time = datetime.now()
        
        try:
            while self.state.is_running:
                cycle_count += 1
                
                if self.current_index >= len(self.df):
                    self.current_index = 0
                    cycle_count += 1
                    logger.info(f"♻  Reiniciando ciclo de datos. Ciclo #{cycle_count}")
                    self.state.log_status_summary()
                
                # Obtener datos de la fila actual
                row = self.df.iloc[self.current_index]
                row_dict = row.to_dict()
                
                # Actualizar estado con la fila actual
                self.state.update_current_row(self.current_index, len(self.df))
                
                # Crear JSON estructurado
                sensor_data = self.create_sensor_data_json(row_dict)
                
                # Log de progreso cada 50 filas
                if self.current_index % 50 == 0:
                    progress_percent = (self.current_index / len(self.df)) * 100
                    logger.info(f"📊 Progreso: {self.current_index}/{len(self.df)} ({progress_percent:.1f}%) - Ciclo {cycle_count}")
                
                # Enviar a Pub/Sub
                logger.debug(f"Enviando fila {self.current_index + 1}/{len(self.df)}")
                success = self.send_to_pubsub(sensor_data)
                
                if success:
                    # Incrementar índice
                    self.current_index += 1
                
                # Log de estado periódico
                if self.state.should_log_stats():
                    self.state.log_status_summary()
                    self.state.update_last_log_time()
                
                # Esperar intervalo especificado
                logger.debug(f"Esperando {interval_seconds} segundos...")
                for i in range(interval_seconds):
                    if not self.state.is_running:
                        break
                    time.sleep(1)
                    
                    # Log cada 10 segundos de espera
                    if i > 0 and i % 10 == 0 and interval_seconds > 10:
                        remaining = interval_seconds - i
                        logger.debug(f"Espera: {i}/{interval_seconds}s ({remaining}s restantes)")
                
        except KeyboardInterrupt:
            logger.info("\n" + "="*80)
            logger.info("Simulación detenida por el usuario (Ctrl+C)")
            logger.info("="*80)
            self.state.is_running = False
        except Exception as e:
            logger.error(f"\n{'='*80}")
            logger.error(f"ERROR CRÍTICO EN LA SIMULACIÓN: {str(e)}")
            logger.error(f"{'='*80}")
            self.state.is_running = False
            raise
        finally:
            # Log final de estadísticas
            total_time = datetime.now() - start_time
            logger.info("\n" + "="*80)
            logger.info("SIMULACIÓN FINALIZADA")
            logger.info("="*80)
            logger.info(f"Duración total: {total_time}")
            logger.info(f"Mensajes enviados: {self.state.messages_sent}")
            logger.info(f"Ciclos completados: {cycle_count}")
            logger.info(f"Filas procesadas: {self.current_index} de {len(self.df)}")
            
            if total_time.total_seconds() > 0:
                avg_rate = self.state.messages_sent / total_time.total_seconds()
                logger.info(f"Tasa promedio: {avg_rate:.2f} mensajes/segundo")
            
            logger.info("="*80)


def parse_gcs_path(file_path):
    """Determina si la ruta es GCS y la parsea"""
    if file_path.startswith('gs://'):
        return True, file_path
    return False, file_path


def main():
    """Función principal para ejecutar la simulación"""
    # Obtener variables de entorno
    csv_file = os.environ.get('FILE_TO_PROCESS')
    topic = os.environ.get('TOPIC')
    interval = int(os.environ.get('INTERVAL_SECONDS', '60'))
    
    logger.info("=" * 80)
    logger.info("IOT SENSOR SIMULATOR")
    logger.info("=" * 80)
    logger.info(f"Versión: 2.0 (Sin Flask, solo logs)")
    logger.info(f"Python: {sys.version}")
    logger.info("=" * 80)
    
    # Validar variables de entorno
    if not csv_file:
        logger.error("ERROR: Variable de entorno FILE_TO_PROCESS no está definida")
        logger.error("Ejemplo: export FILE_TO_PROCESS='gs://mi-bucket/datos.csv'")
        logger.error("O: export FILE_TO_PROCESS='/ruta/local/datos.csv'")
        return 1
    
    if not topic:
        logger.error("ERROR: Variable de entorno TOPIC no está definida")
        logger.error("Ejemplo: export TOPIC='projects/mi-proyecto/topics/mi-topico'")
        logger.error("O: export TOPIC='mi-topico' (se auto-completará)")
        return 1
    
    logger.info(f"CONFIGURACIÓN:")
    logger.info(f"  • Archivo CSV: {csv_file}")
    logger.info(f"  • Topic Pub/Sub: {topic}")
    logger.info(f"  • Intervalo: {interval} segundos")
    logger.info("-" * 80)
    
    # Verificar y formatear el topic
    if not topic.startswith('projects/'):
        original_topic = topic
        topic = f"projects/arduino-tesis/topics/{topic}"
        logger.info(f"Topic formateado: '{original_topic}' → '{topic}'")
    
    # Determinar si es GCS o local
    use_gcs, processed_path = parse_gcs_path(csv_file)
    logger.info(f"Fuente de datos: {'Google Cloud Storage' if use_gcs else 'Archivo local'}")
    
    # Crear y ejecutar simulador
    try:
        logger.info("Inicializando simulador...")
        simulator = IoTSensorSimulator(
            csv_file=processed_path,
            topic_path=topic,
            use_gcs=use_gcs
        )
        
        # Test de conexión
        logger.info("Probando conexión a Pub/Sub...")
        if not simulator.test_pubsub_connection():
            logger.error("Fallo en la conexión a Pub/Sub. Abortando...")
            return 1
        
        # Mostrar información del dataset
        logger.info("=" * 80)
        logger.info("INFORMACIÓN DEL DATASET")
        logger.info("=" * 80)
        logger.info(f"Columnas disponibles ({len(simulator.df.columns)}):")
        for i, col in enumerate(simulator.df.columns, 1):
            logger.info(f"  {i:2d}. {col}")
        
        logger.info(f"\nPrimeras 3 filas de datos:")
        logger.info(f"{simulator.df.head(3).to_string()}")
        logger.info("=" * 80)
        
        # Confirmar inicio
        logger.info("\n" + "="*80)
        logger.info("TODO LISTO PARA INICIAR")
        logger.info("="*80)
        logger.info("La simulación comenzará en 5 segundos...")
        logger.info("Presiona Ctrl+C en cualquier momento para detener")
        logger.info("="*80 + "\n")
        
        time.sleep(5)
        
        # Ejecutar simulación continua
        simulator.simulate_sensor(interval_seconds=interval)
        
        logger.info("Simulación terminada exitosamente")
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"ERROR: Archivo no encontrado - {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"ERROR CRÍTICO: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\nAplicación terminada por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {str(e)}", exc_info=True)
        sys.exit(1)