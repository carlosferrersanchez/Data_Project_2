import streamlit as st
import pandas as pd
import psycopg2
import re
import time
import streamlit_shadcn_ui as ui
import time
from getpass import getpass

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

blue = "<style>div.stDivider.horizontal {border: 2px solid #04b194;}</style>"

# Logo en la barra lateral
st.sidebar.image('/Users/paulglobal/Documents/GitHub/Data_Project_2/Front_End/Pruebas/Final/LOGO.png', use_column_width=True)

# Resto de tu aplicación Streamlit
st.title('Client Distribution Dashboard')

st.header('_OnlyRides_ is here: :blue[Valencia] :sunglasses:')

# Consulta SQL para obtener datos de clientes con request_status 'Waiting'
consulta_sql_ride_requests_waiting = """
SELECT c.name AS customer_name, c.surname AS customer_surname,
       rr.request_time_start, rr.request_time_end
FROM ride_requests rr
JOIN customers c ON rr.id_customer = c.id_customer
WHERE rr.request_status = 'Waiting'
"""
df_waiting_requests = pd.read_sql_query(consulta_sql_ride_requests_waiting, conn)



# Consulta SQL para calcular el tiempo medio de espera para solicitudes 'Matched'
consulta_sql_matched_duration = """
SELECT AVG(EXTRACT(EPOCH FROM rr.request_time_end - rr.request_time_start)) as average_waiting_time
FROM ride_requests rr
WHERE rr.request_status = 'Matched'
"""
df_average_waiting_time = pd.read_sql_query(consulta_sql_matched_duration, conn)

# Calcular el tiempo medio de espera en minutos y segundos
average_waiting_time_seconds = df_average_waiting_time['average_waiting_time'].iloc[0]
average_waiting_time_minutes = average_waiting_time_seconds // 60
average_waiting_time_seconds_remainder = average_waiting_time_seconds % 60
average_waiting_time_seconds_remainder_formatted = "{:.2f}".format(average_waiting_time_seconds_remainder)

# Mostrar el tiempo medio de espera
st.metric(label='Tiempo Medio de Espera', value=f'{int(average_waiting_time_minutes)} minutos {average_waiting_time_seconds_remainder_formatted} segundos')

# Consulta SQL para obtener datos de clientes con request_status 'Waiting'
consulta_sql_ride_requests_waiting = """
SELECT c.name AS customer_name, c.surname AS customer_surname,
       rr.pick_up_point
FROM ride_requests rr
JOIN customers c ON rr.id_customer = c.id_customer
WHERE rr.request_status = 'Waiting'
"""
df_waiting_requests = pd.read_sql_query(consulta_sql_ride_requests_waiting, conn)

# Función para extraer latitud y longitud desde la columna 'pick_up_point'
def extract_coordinates(point):
    match = re.search(r"\((-?\d+\.\d+),(-?\d+\.\d+)\)", point)
    if match:
        return float(match.group(2)), float(match.group(1))
    return None, None

# Aplicar la función a la columna 'pick_up_point' y crear nuevas columnas 'latitude' y 'longitude'
df_waiting_requests['latitude'], df_waiting_requests['longitude'] = zip(*df_waiting_requests['pick_up_point'].map(extract_coordinates))

# Mapeo de estados a colores en formato hexadecimal
st.map(df_waiting_requests, size=20, color='#FF0000')

# Configurar la barra lateral (sidebar)
st.sidebar.title('City Operations Dashboard')

# Opción para auto refrescar
auto_refresh = st.sidebar.checkbox('Auto Refresh', False)
if auto_refresh:
    refresh_rate = st.sidebar.number_input('Refresh Rate (s)', min_value=1, value=10)

# Mostrar la tabla filtrada de solicitudes en espera
st.subheader('Clientes Esperando')
st.write(df_waiting_requests)


# Consulta SQL para obtener datos de ride_requests
consulta_sql_ride_requests = """
SELECT rr.id_customer, rr.request_time_start, rr.request_time_end, rr.request_status,
       c.name AS customer_name, c.surname AS customer_surname,
       rr.pick_up_point  -- Agregado para obtener la columna pick_up_point
FROM ride_requests rr
JOIN customers c ON rr.id_customer = c.id_customer
"""
df_ride_requests = pd.read_sql_query(consulta_sql_ride_requests, conn)

# Función para parsear las coordenadas desde la columna 'pick_up_point'
def parse_coordinates(coord):
    if coord:
        lat, lon = map(float, coord.strip('()').split(','))
        return lat, lon
    return None, None

# Aplicar la función a la 'pick_up_point' para extraer coordenadas
df_ride_requests['latitude'], df_ride_requests['longitude'] = zip(*df_ride_requests['pick_up_point'].map(parse_coordinates))

# Calcular la duración del trayecto como la diferencia entre request_time_end y request_time_start
df_ride_requests['duration'] = (df_ride_requests['request_time_end'] - df_ride_requests['request_time_start']).dt.total_seconds() / 3600

# Mostrar toda la información en una tabla
st.subheader('Información de últimos trayectos')
st.write(df_ride_requests)

# Gráfico de barras para mostrar la distribución de estados de viaje
st.header('Distribución de Estados de Viaje')
state_counts = df_ride_requests['request_status'].value_counts()
st.bar_chart(state_counts)


# Auto refrescar la página si está habilitado
if auto_refresh:
    time.sleep(refresh_rate)
    st.experimental_rerun()

# No olvides cerrar la conexión a la base de datos al final de tu script
conn.close()
