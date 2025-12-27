import os
import json
import logging
import time
from datetime import datetime
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.api_core import exceptions

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sanitize_keys(obj):
    """Reemplaza puntos por guiones bajos en las claves de diccionarios recursivamente"""
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            new_key = k.replace('.', '_')
            new_dict[new_key] = sanitize_keys(v)
        return new_dict
    elif isinstance(obj, list):
        return [sanitize_keys(item) for item in obj]
    return obj

class FogNode:
    def __init__(self):
        self.project_id = os.environ.get('PROJECT_ID')
        self.subscription_id = os.environ.get('SUBSCRIPTION_ID')
        self.dataset_id = os.environ.get('DATASET_ID', 'iot_data')
        self.table_id = os.environ.get('TABLE_ID', 'sensor_readings')
        
        if not all([self.project_id, self.subscription_id]):
            raise ValueError("Faltan variables de entorno requeridas: PROJECT_ID, SUBSCRIPTION_ID")

        self.subscriber = pubsub_v1.SubscriberClient()
        self.bq_client = bigquery.Client(project=self.project_id)
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        self.setup_bigquery()

    def setup_bigquery(self):
        """Crea el dataset y la tabla en BigQuery si no existen"""
        # Crear dataset
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} ya existe")
        except exceptions.NotFound:
            logger.info(f"Creando dataset {self.dataset_id}...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # O la región que corresponda
            self.bq_client.create_dataset(dataset)
            logger.info(f"Dataset {self.dataset_id} creado")

        # Esquema de la tabla
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("location_info", "RECORD", fields=[
                bigquery.SchemaField("latitude", "FLOAT"),
                bigquery.SchemaField("longitude", "FLOAT"),
                bigquery.SchemaField("altitude", "FLOAT"),
                bigquery.SchemaField("Location_ID", "STRING"), # Sanitzado de Location.ID
            ]),
            bigquery.SchemaField("main_sensors", "RECORD", fields=[
                bigquery.SchemaField("BMP280", "FLOAT"),
                bigquery.SchemaField("SHT31TE", "FLOAT"),
                bigquery.SchemaField("SHT31HE", "FLOAT"),
                bigquery.SchemaField("SHT31TI", "FLOAT"),
                bigquery.SchemaField("SHT31HI", "FLOAT"),
                bigquery.SchemaField("Absolute_humidity", "FLOAT"),
                bigquery.SchemaField("Td_deficit", "FLOAT"),
            ]),
            bigquery.SchemaField("gas_sensors", "RECORD", fields=[
                bigquery.SchemaField("CO_A4_P1", "FLOAT"),
                bigquery.SchemaField("NO_B4_P1", "FLOAT"),
                bigquery.SchemaField("NO2_B43F_P1", "FLOAT"),
                bigquery.SchemaField("OX_A431_P1", "FLOAT"),
                bigquery.SchemaField("D300", "FLOAT"),
            ]),
            bigquery.SchemaField("metadata", "RECORD", fields=[
                bigquery.SchemaField("source_file", "STRING"),
                bigquery.SchemaField("row_index", "INTEGER"),
                bigquery.SchemaField("message_number", "INTEGER"),
                bigquery.SchemaField("total_rows", "INTEGER"),
                bigquery.SchemaField("data_source", "STRING"),
            ]),
            bigquery.SchemaField("processing_timestamp", "TIMESTAMP"), # Tiempo de ingestión
            bigquery.SchemaField("processing_node", "STRING"), # Nombre del nodo (hostname)
        ]

        # Crear tabla
        try:
            self.bq_client.get_table(self.table_ref)
            logger.info(f"Tabla {self.table_id} ya existe")
            # Podríamos verificar evolución de esquema aquí, pero lo omitimos por simplicidad
        except exceptions.NotFound:
            logger.info(f"Creando tabla {self.table_id}...")
            table = bigquery.Table(self.table_ref, schema=schema)
            # Particionar por tiempo de ingestión puede ser útil
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"  # Usar el timestamp del dato sensor
            )
            self.bq_client.create_table(table)
            logger.info(f"Tabla {self.table_id} creada")

    def callback(self, message):
        """Procesa mensaje de Pub/Sub"""
        try:
            data_str = message.data.decode("utf-8")
            logger.debug(f"Recibido mensaje: {data_str[:100]}...")
            
            data_json = json.loads(data_str)
            
            # Sanitizar claves (especialmente Location.ID -> Location_ID)
            data_json = sanitize_keys(data_json)
            
            # Enriquecer con datos del nodo
            data_json['processing_timestamp'] = datetime.utcnow().isoformat() + "Z"
            data_json['processing_node'] = os.environ.get('HOSTNAME', 'unknown-node')

            # Insertar en BigQuery
            errors = self.bq_client.insert_rows_json(self.table_ref, [data_json])
            
            if errors == []:
                logger.info("Datos insertados correctamente en BQ")
                message.ack()
            else:
                logger.error(f"Errores al insertar en BQ: {errors}")
                # No hacemos nack inmediatamente para evitar bucle infinito rápido,
                # pero en producción se debería manejar DLQ
                message.nack()
                
        except Exception as e:
            logger.error(f"Error procesando mensaje: {str(e)}")
            message.nack()

    def start(self):
        logger.info(f"Escuchando mensajes en {self.subscription_path}...")
        streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path, callback=self.callback
        )
        logger.info(f"Iniciando streaming pull...")

        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        except Exception as e:
            logger.error(f"Error en streaming pull: {e}")
            streaming_pull_future.cancel()

if __name__ == '__main__':
    node = FogNode()
    node.start()
