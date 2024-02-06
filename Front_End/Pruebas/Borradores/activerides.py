import streamlit as st
import pandas as pd
from creacion_clase import BaseDeDatos
from st_pages import Page, show_pages
import streamlit_shadcn_ui as ui


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


# Cargar y mostrar el mapa con los viajes especificados
ride_data = cargar_datos_rides(show_active_rides)
if not ride_data.empty:
    st.map(ride_data)
else:
    st.write("No hay viajes para mostrar.")

