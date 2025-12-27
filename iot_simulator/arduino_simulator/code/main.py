import pandas as pd
import json
import time
import os
from datetime import datetime
from google.cloud import pubsub_v1
import logging
import numpy as np

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IoTSensorSimulator:
    def __init__(self, csv_file, topic_path):
        """
        Inicializa el simulador de sensor IoT
        
        Args:
            csv_file: Ruta al archivo CSV con los datos
            topic_path: Ruta completa del topic de Pub/Sub (ej: 'projects/project-id/topics/topic-name')
        """
        self.csv_file = csv_file
        self.topic_path = topic_path
        self.df = None
        self.current_index = 0
        self.location_info = {}
        
        # Configurar cliente de Pub/Sub
        self.publisher = pubsub_v1.PublisherClient()
        
        # Cargar y preparar datos
        self.load_and_prepare_data()
    
    def load_and_prepare_data(self):
        """Carga el archivo CSV y extrae información de ubicación"""
        try:
            logger.info(f"Cargando datos desde: {self.csv_file}")
            self.df = pd.read_csv(self.csv_file)
            
            # Buscar información de ubicación en las filas
            # Asumiendo que la información está en alguna de las primeras filas
            location_row = None
            
            for i in range(min(10, len(self.df))):  # Buscar en las primeras 10 filas
                if 'latitude' in str(self.df.iloc[i].values).lower():
                    location_row = i
                    break
            
            if location_row is not None:
                # Extraer información de ubicación
                self.location_info = {
                    'latitude': self.df.iloc[location_row].get('latitude', np.nan),
                    'longitude': self.df.iloc[location_row].get('longitude', np.nan),
                    'altitude': self.df.iloc[location_row].get('altitude', np.nan),
                    'Location.ID': self.df.iloc[location_row].get('Location.ID', '')
                }
                logger.info(f"Información de ubicación encontrada: {self.location_info}")
                
                # Remover la fila de ubicación del dataframe principal si es necesario
                if location_row < len(self.df):
                    self.df = self.df.drop(location_row).reset_index(drop=True)
            else:
                logger.warning("No se encontró información de ubicación específica en el CSV")
                
            logger.info(f"Datos cargados. Total de filas: {len(self.df)}")
            
        except Exception as e:
            logger.error(f"Error al cargar datos: {str(e)}")
            raise
    
    def create_sensor_data_json(self, row_data):
        """
        Crea el JSON con la estructura requerida para los datos del sensor
        
        Args:
            row_data: Diccionario con los datos de una fila del CSV
            
        Returns:
            Diccionario con la estructura JSON requerida
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
                "source_file": os.path.basename(self.csv_file),
                "row_index": self.current_index,
                "total_rows": len(self.df)
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
        
        Args:
            data: Diccionario con los datos a enviar
        """
        try:
            # Convertir a JSON
            json_data = json.dumps(data).encode('utf-8')
            
            # Publicar mensaje
            future = self.publisher.publish(self.topic_path, json_data)
            message_id = future.result()
            
            logger.info(f"Mensaje enviado exitosamente. Message ID: {message_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error al enviar a Pub/Sub: {str(e)}")
            return False
    
    def simulate_sensor(self):
        """
        Simula el envío de datos del sensor cada minuto
        """
        logger.info("Iniciando simulación de sensor IoT...")
        logger.info(f"Enviando datos al topic: {self.topic_path}")
        
        try:
            while self.current_index < len(self.df):
                # Obtener datos de la fila actual
                row = self.df.iloc[self.current_index]
                row_dict = row.to_dict()
                
                # Crear JSON estructurado
                sensor_data = self.create_sensor_data_json(row_dict)
                
                # Enviar a Pub/Sub
                logger.info(f"Enviando fila {self.current_index + 1}/{len(self.df)}")
                self.send_to_pubsub(sensor_data)
                
                # Incrementar índice (circular)
                self.current_index += 1
                if self.current_index >= len(self.df):
                    self.current_index = 0
                    logger.info("Reiniciando ciclo de datos...")
                
                # Esperar 60 segundos
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("Simulación detenida por el usuario")
        except Exception as e:
            logger.error(f"Error en la simulación: {str(e)}")
    
    def run_once(self):
        """
        Envía una sola fila (útil para testing)
        """
        if self.current_index >= len(self.df):
            self.current_index = 0
        
        row = self.df.iloc[self.current_index]
        row_dict = row.to_dict()
        
        sensor_data = self.create_sensor_data_json(row_dict)
        
        logger.info(f"Enviando fila {self.current_index + 1}/{len(self.df)}")
        success = self.send_to_pubsub(sensor_data)
        
        if success:
            self.current_index += 1
        
        return success


def main():
    # Obtener variables de entorno
    csv_file = os.environ.get('FILE_TO_PROCESS')
    topic = os.environ.get('TOPIC')
    
    if not csv_file or not topic:
        logger.error("Las variables de entorno FILE_TO_PROCESS y TOPIC deben estar definidas")
        logger.error("Ejemplo de uso:")
        logger.error("FILE_TO_PROCESS=/data/Antwerp/sensor_data.csv TOPIC=projects/my-project/topics/antwerp-sensors python sensor_simulator.py")
        return
    
    if not os.path.exists(csv_file):
        logger.error(f"Archivo no encontrado: {csv_file}")
        return
    
    # Crear y ejecutar simulador
    try:
        simulator = IoTSensorSimulator(csv_file, topic)
        
        # Para testing: enviar una sola fila
        # simulator.run_once()
        
        # Para producción: ejecutar simulación continua
        simulator.simulate_sensor()
        
    except Exception as e:
        logger.error(f"Error en la ejecución: {str(e)}")


if __name__ == "__main__":
    main()