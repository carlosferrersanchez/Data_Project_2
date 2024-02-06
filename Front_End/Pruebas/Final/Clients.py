import streamlit as st
import pandas as pd
import psycopg2
import re
import time
from getpass import getpass
import numpy as np

# AJUSTAR MÁRGENES
custom_css = """
<style>
body {
    margin: 0px;
}

[data-testid="stTheme"] {
    background-color: #f9f2f2 !important;
    font-family: serif !important;
}

[data-testid="stSidebar"] {
    background-color: #04b194 !important;
}

[data-testid="stBlockContainer"] {
    background-color: #e6e6e6 !important;
}
</style>
"""

# Clase para manejar la base de datos
class BaseDeDatos:
    # Credenciales de la base de datos como atributos estáticos
    HOST = '34.38.87.73'
    DATABASE = 'DP2'
    USER = 'postgres'
    PASSWORD = getpass("Introduce la contraseña de la base de datos: ")
    PORT = '5432'

    @staticmethod
    def conectar():
        return psycopg2.connect(
            host=BaseDeDatos.HOST,
            database=BaseDeDatos.DATABASE,
            user=BaseDeDatos.USER,
            password=BaseDeDatos.PASSWORD
        )

    @staticmethod
    def consultar(consulta_sql, parametros=None):
        with BaseDeDatos.conectar() as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(consulta_sql, parametros)
                return cursor.fetchall()

    @staticmethod
    def ejecutar(consulta_sql, parametros=None):
        with BaseDeDatos.conectar() as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(consulta_sql, parametros)
                conexion.commit()

# Establecer la conexión a la base de datos PostgreSQL
conn = psycopg2.connect(
    host=BaseDeDatos.HOST,
    database=BaseDeDatos.DATABASE,
    user=BaseDeDatos.USER,
    password=BaseDeDatos.PASSWORD,
    port=BaseDeDatos.PORT
)

# Logo en la barra lateral
st.sidebar.image('/Users/paulglobal/Documents/GitHub/Data_Project_2/Front_End/Pruebas/Final/LOGO.png', use_column_width=True)

# Resto de tu aplicación Streamlit
st.title('City Operations Dashboard')

st.header('_OnlyRides_ is here: :blue[Valencia] :sunglasses:')


# Consulta SQL para obtener datos de ride_requests
consulta_sql = "SELECT pick_up_point, request_status FROM ride_requests"
df = pd.read_sql_query(consulta_sql, conn)

# Función para extraer latitud y longitud desde la columna 'pick_up_point'
def extract_coordinates(point):
    match = re.search(r"\((-?\d+\.\d+),(-?\d+\.\d+)\)", point)
    if match:
        return float(match.group(2)), float(match.group(1))
    return None, None

# Aplicar la función a la columna 'pick_up_point' y crear nuevas columnas 'latitude' y 'longitude'
df['latitude'], df['longitude'] = zip(*df['pick_up_point'].map(extract_coordinates))

# Mapeo de estados a colores en formato hexadecimal
color_mapping = {
    'Waiting': '#FF0000',  # Rojo
    'Missed': '#FFFF00',   # Amarillo
    'Match': '#008000'     # Verde
}

# Agregar una columna 'color' al DataFrame
df['color'] = df['request_status'].map(color_mapping)

# Configurar la barra lateral (sidebar)
st.sidebar.title('City Operations Dashboard')

# Opción para auto refrescar
auto_refresh = st.sidebar.checkbox('Auto Refresh', False)
if auto_refresh:
    refresh_rate = st.sidebar.number_input('Refresh Rate (s)', min_value=1, value=10)

# Consulta SQL para obtener datos de ride_requests
consulta_sql_ride_requests = """
SELECT rr.id_customer, rr.request_time_start, rr.request_time_end, rr.request_status,
       c.name AS customer_name, c.surname AS customer_surname
FROM ride_requests rr
JOIN customers c ON rr.id_customer = c.id_customer
"""
df_ride_requests = pd.read_sql_query(consulta_sql_ride_requests, conn)

# Calcula la diferencia de tiempo entre request_time_end y request_time_start
df_ride_requests['duration'] = (df_ride_requests['request_time_end'] - df_ride_requests['request_time_start']).astype('timedelta64[s]') / 3600

# Mergear nombres y apellidos de los clientes con su duración y estado del request
df_ride_requests['customer_full_name'] = df_ride_requests['customer_name'] + ' ' + df_ride_requests['customer_surname']
df_ride_requests['color'] = df_ride_requests['request_status'].map(color_mapping)

# Mostrar toda la información en una tabla
st.header('Información de Trayectos y Clientes')
st.write(df_ride_requests[['id_customer', 'customer_full_name', 'duration', 'request_status']])

# Gráfico de barras para mostrar la distribución de estados de viaje
st.header('Distribución de Estados de Viaje')
state_counts = df_ride_requests['request_status'].value_counts()
st.bar_chart(state_counts)

# Gráfico de líneas para mostrar la tendencia temporal de la duración de los trayectos
st.header('Tendencia Temporal de Duración de Trayectos')
duration_trend = df_ride_requests.groupby(df_ride_requests['request_time_start'].dt.date)['duration'].mean()
st.line_chart(duration_trend)

# Configurar la visualización del mapa con Streamlit Maps


st.map(df,
    latitude='col1',
    longitude='col2',
    size='col3',
    color='col4')


# Auto refrescar la página si está habilitado
if auto_refresh:
    time.sleep(refresh_rate)
    st.experimental_rerun()

# No olvides cerrar la conexión a la base de datos al final de tu script
conn.close()

# Function to parse coordinates in the format (-0.38654,39.47213)
def parse_coordinates(coord):
    if coord:
        lat, lon = map(float, coord.strip('()').split(','))
        return lat, lon
    return None, None

# Apply the function to the 'pick_up_point' column to extract coordinates
df_ride_requests['latitude'], df_ride_requests['longitude'] = zip(*df_ride_requests['pick_up_point'].map(parse_coordinates))

# Configurar la visualización del mapa con Streamlit Maps
st.header('Mapa de Ride Requests con Colores')
st.map(df_ride_requests, 
    latitude='latitude',  # Use 'latitude' column for latitude coordinates
    longitude='longitude',  # Use 'longitude' column for longitude coordinates
    color='color',  # Use 'color' column for specifying colors based on request status
    use_container_width=True)  # Puedes personalizar el tamaño aquí

