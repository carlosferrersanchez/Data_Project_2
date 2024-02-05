import streamlit as st
import pandas as pd
import psycopg2
import streamlit_shadcn_ui as ui

class BaseDeDatos:
    # Credenciales de la base de datos como atributos estáticos
    HOST = '34.38.87.73'
    DATABASE = 'DP2'
    USER = 'postgres'
    PASSWORD = '1234'
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

st.header('_OnlyRides_ is here: :blue[Valencia]')


if st.button("Refresh Rides", key="refresh_button"):
    # Add your refresh logic here
    st.write("Conductores activos actualizados")
    
    import streamlit as st

option = st.selectbox(
   "¿Como quieres que contacte el conductor cuando llegue a la ubicación?",
   ("Email", "WhatsApp", "Teléfono"),
   index=None,
   placeholder="Elige método de contacto",
)

st.write('You selected:', option)

# Mostrar métricas
cols = st.columns(3)
with cols[0]:
    ui.metric_card(title="Total Revenue", content="$45,231.89", description="+20.1% from last month", key="card1")
with cols[1]:
    ui.metric_card(title="Total Revenue", content="$45,231.89", description="+20.1% from last month", key="card2")
with cols[2]:
    ui.metric_card(title="Total Revenue", content="$45,231.89", description="+20.1% from last month", key="card3")

# Interruptor para alternar entre viajes activos y todos los viajes
show_active_rides = st.checkbox('Active Rides', True)

# Función para cargar los datos de los viajes
def cargar_datos_rides(activos=True):
    if activos:
        consulta_sql = "SELECT id_driver, current_position FROM active_vehicles WHERE service_status IN ('Active', 'On route')"
    else:
        # Consulta SQL para todos los viajes usando la columna correcta de la tabla rides
        consulta_sql = "SELECT ride_current_position FROM rides"

    try:
        datos = BaseDeDatos.consultar(consulta_sql)
        if activos:
            # Suponemos que las coordenadas se guardan como 'POINT(lon lat)'
            df = pd.DataFrame(datos, columns=['id_driver', 'position'])
            df['lat'] = df['position'].apply(lambda x: float(x.split(',')[1].strip(')')))
            df['lon'] = df['position'].apply(lambda x: float(x.split(',')[0].strip('POINT(')))
            return df[['id_driver', 'lat', 'lon']]
        else:
            # Si no estás consultando los datos de active_vehicles, simplemente crea el DataFrame con las coordenadas
            df = pd.DataFrame(datos, columns=['position'])
            df['lat'] = df['position'].apply(lambda x: float(x.split(',')[1].strip(')')))
            df['lon'] = df['position'].apply(lambda x: float(x.split(',')[0].strip('POINT(')))
            return df[['lat', 'lon']]
    except Exception as e:
        st.error(f'Error al cargar datos de los viajes: {e}')
        return pd.DataFrame(columns=['id_driver', 'lat', 'lon'])

# Consulta SQL para obtener los clientes que están esperando su viaje
consulta_sql_clientes = """
SELECT rr.id_request, c.name, c.surname
FROM ride_requests rr
JOIN customers c ON rr.id_customer = c.id_customer
WHERE rr.request_status = 'Active' AND c.customer_status = 'Active'
"""

# Ejecutar la consulta SQL y obtener los datos en un DataFrame
with BaseDeDatos.conectar() as conexion:
    df_clientes = pd.read_sql_query(consulta_sql_clientes, conexion)

# Cargar y mostrar el mapa con los viajes especificados
ride_data = cargar_datos_rides(show_active_rides)
if not ride_data.empty:
    st.map(ride_data)
else:
    st.write("No hay viajes para mostrar.")

# Mostrar la cola de clientes que están esperando su viaje
st.header('Clientes en cola para su viaje:')
if not df_clientes.empty:
    st.table(df_clientes)
else:
    st.write("No hay clientes en cola en este momento.")


