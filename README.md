# IoT Sensor Simulation & Monitoring System

Este proyecto implementa un ecosistema completo para la simulación, procesamiento y visualización de datos de sensores IoT utilizando **Google Cloud Platform (GCP)**. La arquitectura combina servicios Serverless (Cloud Run) con infraestructura híbrida (Fog Nodes en GCE).

## 📂 Estructura del Proyecto

### 1. `iot_simulator/`
Núcleo del sistema de simulación e infraestructura base.

*   **`00_create_bucket.yml`**: Playbook de Ansible para crear los buckets de Google Cloud Storage necesarios para almacenar los datos crudos del sensor.
*   **`01_upload_data.yml`**: Playbook para cargar los archivos CSV locales de los sensores al bucket de GCS.
*   **`arduino_simulator/`**: Contiene el código y despliegue del simulador de sensores.
    *   **`code/`**: Código Python (`main.py`) que lee CSVs y publica en Pub/Sub. Diseñado para ejecutarse como Cloud Run Jobs.
    *   **`01_containers_yamls/`**: Playbooks para orquestar el despliegue masivo de simuladores (uno por ciudad/archivo) en Cloud Run.
*   **`fog_nodes/`**: Capa de procesamiento intermedio (Fog Computing).
    *   **`code/`**: Script Python que consume de Pub/Sub e inserta datos en **BigQuery**.
    *   **`playbooks/`**: Automatización para crear instancias GCE, configurar dependencias y levantar el servicio de procesamiento.
*   **`frontend/`**: Sistema de visualización.
    *   **`app.py`**: Dashboard interactivo con **Streamlit** que consulta BigQuery.
    *   **`ansible/`**: Playbooks para desplegar el dashboard en Cloud Run.

---

## 🛠️ Guía de los Archivos YAML (Ansible)

El proyecto utiliza Ansible para automatizar todo el ciclo de vida del sistema:

| Archivo / Carpeta | Función Principal |
| :--- | :--- |
| **`iot_simulator/00_create_bucket.yml`** | Configura el almacenamiento inicial en GCS. |
| **`iot_simulator/01_upload_data.yml`** | Sincroniza los datasets de sensores con la nube. |
| **`arduino_simulator/01_containers_yamls/01_cloud-run-manager.yaml`** | Gestiona la creación y ejecución de **Cloud Run Jobs** para la simulación. |
| **`arduino_simulator/01_containers_yamls/03_run_playbooks.yaml`** | Playbook "maestro" que gatilla las simulaciones para todas las ciudades configuradas. |
| **`fog_nodes/playbooks/01_create_infrastructure.yaml`** | Crea las instancias de cómputo (Fog Nodes) y las suscripciones de Pub/Sub. |
| **`fog_nodes/playbooks/02_configure_nodes.yaml`** | Instala Python, copia el código y configura **systemd** para que el nodo procesador sea resiliente. |
| **`frontend/ansible/deploy_to_cloud_run.yaml`** | Despliega el dashboard de visualización en Cloud Run de forma serverless. |

---

## 🚀 Flujo de Trabajo Típico

1.  **Infraestructura Base**: Ejecutar `00_create_bucket.yml` y `01_upload_data.yml`.
2.  **Consumidores (Fog Nodes)**: Ejecutar los playbooks en `fog_nodes/playbooks/` para tener listos los nodos que guardarán datos en BigQuery.
3.  **Simulación**: Lanzar `arduino_simulator/01_containers_yamls/03_run_playbooks.yaml` para empezar a enviar datos a la nube.
4.  **Visualización**: Desplegar el frontend con `frontend/ansible/deploy_to_cloud_run.yaml` para monitorear los sensores en tiempo real.

---

## 🔐 Requisitos de Seguridad

El sistema utiliza Cuentas de Servicio (Service Accounts) para la autenticación entre componentes:
*   El archivo `sa-ansible-key.json` es crítico para que Ansible y las aplicaciones puedan interactuar con BigQuery, Pub/Sub y Storage.
*   Se recomienda asignar roles de `BigQuery Data Viewer` y `BigQuery Job User` a la cuenta de servicio del Dashboard.
