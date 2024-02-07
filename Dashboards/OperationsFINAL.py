import streamlit as st
import pandas as pd
import psycopg2
import streamlit_shadcn_ui as ui
import time


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

blue = "<style>div.stDivider.horizontal {border: 2px solid #04b194;}</style>"

# Logo en la barra lateral
st.sidebar.image('LOGO.png', use_column_width=True)

# Resto de tu aplicación Streamlit
st.title('City Operations Dashboard')
st.markdown(blue, unsafe_allow_html=True)  # Divider de color azul
st.header('_OnlyRides_ is here: :blue[Valencia] :sunglasses:')


# Función para contar la cantidad de vehículos con service_status "With Passengers"
def contar_vehiculos():
    consulta_sql_with_passengers = "SELECT COUNT(*) FROM active_vehicles WHERE service_status = 'With Passengers'"
    
    try:
        with BaseDeDatos.conectar() as conexion:
            count_with_passengers = BaseDeDatos.consultar(consulta_sql_with_passengers)[0][0]
            return count_with_passengers
    except Exception as e:
        st.error(f'Error counting vehicles: {e}')
        return None

# Obtener la cantidad de vehículos con service_status "With Passengers"
count_with_passengers = contar_vehiculos()

# Mostrar la cantidad de vehículos con pasajeros
if count_with_passengers is not None:
    st.success(f'Vehículos con pasajeros abordo: {count_with_passengers}')
else:
    st.warning("Unable to retrieve vehicle information. Please try again later.")

# Configurar el auto-refresh en el sidebar
st.sidebar.title("Auto Refresh")
auto_refresh = st.sidebar.checkbox('Seleccionar Tiempo', False)
if auto_refresh:
    refresh_rate = st.sidebar.number_input('Refresh rate in seconds', min_value=1, value=10)

# Función para cargar y mostrar los datos
def cargar_datos_y_mostrar(activos=True):
    while True:
        # Mostrar métricas
        cols = st.columns(3)
        with cols[0]:
            # Consulta SQL para obtener el total de clientes activos
            consulta_clientes_activos = "SELECT COUNT(*) FROM customers WHERE customer_status = 'Active'"
            total_clientes_activos = BaseDeDatos.consultar(consulta_clientes_activos)[0][0]
            if total_clientes_activos is not None:
                ui.metric_card(title="Clientes Activos", content=total_clientes_activos, description="Clientes usando la aplicación", key="card4_clientes_activos")
            else:
                st.warning("No hay datos de clientes activos disponibles en este momento.")

        with cols[1]:
            # Consulta SQL para obtener el rating medio
            consulta_rating_medio = "SELECT AVG(rating) FROM rating"
            media_rating = BaseDeDatos.consultar(consulta_rating_medio)[0][0]

            if media_rating is not None:
                ui.metric_card(title="Rating Medio", content=f"{media_rating:.2f}", description="Sentimiento del cliente", key="card5")
            else:
                st.warning("No hay datos de calificación disponibles en este momento.")

        with cols[2]:
            # Consulta SQL para obtener el total de conductores activos
            consulta_conductores_activos = "SELECT COUNT(*) FROM drivers WHERE driver_status = 'Active'"
            total_conductores_activos = BaseDeDatos.consultar(consulta_conductores_activos)[0][0]
            if total_conductores_activos is not None:
                ui.metric_card(title="Vehículos Activos", content=total_conductores_activos, description="Vehiculos operativos recorriendo rutas", key="card6")
            else:
                st.warning("No hay datos de conductores activos disponibles en este momento.")
              
        # Interruptor para alternar entre viajes activos y todos los viajes
        show_active_rides = st.checkbox('Ver todos los vehiculos activos sin pasajeros', True)
        
        # Explicación del mapa
        st.caption('Mapa de vehículos con pasajeros abordo')
        
        # Función para cargar los datos de los viajes
        def cargar_datos_rides(activos=True):
            if activos:
                consulta_sql = "SELECT id_driver, current_position FROM active_vehicles WHERE service_status IN ('With Passengers')"
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

        # Consulta SQL para obtener los conductores activos
        consulta_sql_conductores_activos = """
        SELECT id_driver, name, surname, driver_license
        FROM drivers
        WHERE driver_status = 'Active'
        """

        # Ejecutar la consulta SQL y obtener los datos en un DataFrame
        with BaseDeDatos.conectar() as conexion:
            df_conductores_activos = pd.read_sql_query(consulta_sql_conductores_activos, conexion)

        # Cargar y mostrar el mapa con los viajes especificados
        ride_data = cargar_datos_rides(show_active_rides)
        if not ride_data.empty:
            st.map(ride_data)
        else:
            st.write("No hay viajes para mostrar.")

        # Mostrar la cola de clientes que están esperando su viaje
        st.header('Clientes en cola esperando a su viaje:')
        if not df_clientes.empty:
            st.table(df_clientes)
        else:
            st.write("No hay clientes en cola en este momento.")

        # Mostrar conductores activos
        st.header('Conductores en activo trabajando:')
        if not df_conductores_activos.empty:
            st.table(df_conductores_activos)
        else:
            st.write("No hay conductores activos en este momento.")

        # Esperar durante el tiempo especificado en segundos
        if auto_refresh:
            time.sleep(refresh_rate)
            st.experimental_rerun()

# Llamar a la función para cargar y mostrar los datos
cargar_datos_y_mostrar(True)  # Puedes ajustar esto según tus necesidades, por ejemplo, cargar los datos activos primero
