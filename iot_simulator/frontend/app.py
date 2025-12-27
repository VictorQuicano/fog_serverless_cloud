import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import time
from datetime import datetime, timedelta

# Configuración de la página
st.set_page_config(
    page_title="IoT Sensor Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS personalizados
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .stAlert {
        padding: 0.5rem;
        margin-bottom: 0.5rem;
    }
    h1, h2, h3 {
        color: #0e1117;
    }
</style>
""", unsafe_allow_html=True)

# --- Funciones de Datos ---

@st.cache_resource
def get_bq_client():
    return bigquery.Client()

def get_locations():
    """Obtiene la lista de Location_IDs únicos disponibles"""
    client = get_bq_client()
    query = """
    SELECT DISTINCT location_info.Location_ID as location_id
    FROM `arduino-tesis.iot_data.sensor_readings`
    WHERE location_info.Location_ID IS NOT NULL
    ORDER BY location_id
    """
    try:
        df = client.query(query).to_dataframe()
        return df['location_id'].tolist()
    except Exception as e:
        st.error(f"Error al conectar con BigQuery: {e}")
        return []

def get_sensor_data(location_id, limit=500):
    """Obtiene datos históricos para una ubicación específica"""
    client = get_bq_client()
    query = f"""
    SELECT 
        timestamp,
        location_info.Location_ID,
        
        -- Main Sensors
        main_sensors.BMP280,
        main_sensors.SHT31TE,
        main_sensors.SHT31HE,
        main_sensors.SHT31TI,
        main_sensors.SHT31HI,
        main_sensors.Absolute_humidity,
        main_sensors.Td_deficit,
        
        -- Gas Sensors
        gas_sensors.CO_A4_P1,
        gas_sensors.NO_B4_P1,
        gas_sensors.NO2_B43F_P1,
        gas_sensors.OX_A431_P1,
        gas_sensors.D300,
        
        metadata.source_file,
        processing_node
        
    FROM `arduino-tesis.iot_data.sensor_readings`
    WHERE location_info.Location_ID = @location_id
    ORDER BY timestamp DESC
    LIMIT @limit
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
            bigquery.ScalarQueryParameter("limit", "INTEGER", limit)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

# --- Interfaz de Usuario ---

st.title("🏭 IoT Sensor Monitoring Dashboard")
st.markdown("Visualización en tiempo real de sensores IoT almacenados en BigQuery")

# Sidebar
with st.sidebar:
    st.header("Configuración")
    
    # Selector de ubicación
    locations = get_locations()
    if not locations:
        st.warning("No se encontraron datos de ubicación. Verifica la conexión a BQ.")
        selected_location = None
    else:
        selected_location = st.selectbox(
            "Seleccionar Ubicación (Location ID)",
            locations,
            index=0
        )
    
    data_limit = st.slider("Cantidad de registros a mostrar", 100, 2000, 500)
    
    if st.button("🔄 Actualizar Datos"):
        st.cache_data.clear()
        st.experimental_rerun()

    st.markdown("---")
    st.markdown("### Estado del Sistema")
    st.info(f"Proyecto: arduino-tesis\nDataset: iot_data\nTabla: sensor_readings")

# Contenido Principal
if selected_location:
    # Cargar datos
    with st.spinner(f'Cargando datos para {selected_location}...'):
        df = get_sensor_data(selected_location, data_limit)

    if not df.empty:
        # Convertir timestamp a datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # --- SECCIÓN DE ALERTAS ---
        st.header("🚨 Panel de Alertas y Salud")
        
        col1, col2, col3 = st.columns(3)
        
        # Métricas generales
        latest = df.iloc[0]
        col1.metric("Última Actualización", latest['timestamp'].strftime('%H:%M:%S'))
        col2.metric("Fuente", latest['source_file'])
        col3.metric("Nodo Procesador", latest['processing_node'])
        
        # Detección de NULLs
        sensor_columns = [
            'BMP280', 'SHT31TE', 'SHT31HE', 'SHT31TI', 'SHT31HI', 
            'Absolute_humidity', 'Td_deficit',
            'CO_A4_P1', 'NO_B4_P1', 'NO2_B43F_P1', 'OX_A431_P1', 'D300'
        ]
        
        null_issues = []
        for col in sensor_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                null_issues.append((col, null_count))
        
        if null_issues:
            st.error(f"⚠️ **ATENCIÓN**: Se detectaron valores faltantes (NULL) en {len(null_issues)} sensores.")
            for sensor, count in null_issues:
                st.warning(f"🔴 Sensor **{sensor}**: {count} lecturas nulas en los últimos {data_limit} registros.")
        else:
            st.success("✅ Todos los sensores están reportando datos correctamente (Sin valores NULL).")

        # --- SECCIÓN DE GRÁFICOS ---
        
        # 1. Main Sensors
        st.markdown("---")
        st.header("📈 Main Sensors (Temperatura/Humedad)")
        
        main_cols = ['BMP280', 'SHT31TE', 'SHT31HE', 'SHT31TI', 'SHT31HI', 'Absolute_humidity', 'Td_deficit']
        
        tab1, tab2 = st.tabs(["Gráfico Combinado", "Gráficos Individuales"])
        
        with tab1:
            fig_main = px.line(df, x='timestamp', y=main_cols,
                             title=f"Histórico de Sensores Principales - {selected_location}",
                             markers=True)
            st.plotly_chart(fig_main, use_container_width=True)
            
        with tab2:
            cols = st.columns(2)
            for i, col_name in enumerate(main_cols):
                with cols[i % 2]:
                    # Verificar si hay datos
                    if df[col_name].isnull().all():
                        st.warning(f"Sin datos para {col_name}")
                        continue
                        
                    fig = px.line(df, x='timestamp', y=col_name, title=col_name)
                    # Resaltar nulos si hubiese (rompen la línea)
                    fig.update_traces(connectgaps=False) 
                    st.plotly_chart(fig, use_container_width=True)

        # 2. Gas Sensors
        st.markdown("---")
        st.header("☣️ Gas Sensors (Contaminantes)")
        
        gas_cols = ['CO_A4_P1', 'NO_B4_P1', 'NO2_B43F_P1', 'OX_A431_P1', 'D300']
        
        fig_gas = px.line(df, x='timestamp', y=gas_cols,
                        title=f"Histórico de Sensores de Gas - {selected_location}",
                        template="plotly_dark")
        st.plotly_chart(fig_gas, use_container_width=True)
        
        # --- TABLA DE DATOS RAW ---
        with st.expander("Ver Datos Crudos (Tabla)"):
            st.dataframe(df)

    else:
        st.warning(f"No se encontraron datos para la ubicación {selected_location}.")
else:
    st.info("Por favor selecciona una ubicación en la barra lateral para comenzar.")
