import streamlit as st
import pandas as pd
import time
from creacion_clase import BaseDeDatos

#AJUSTAR MÁRGENES
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
st.markdown(custom_css, unsafe_allow_html=True)

#TÍTULO DEL DASHBOARD
st.title('Situación actual')

#LISTA DESPLEGABLE DEPENDIENTE - Para que me devuelva en una lista todos los conductores Inactivos (Activos actualmente no hay)
conductores_activos = BaseDeDatos.consultar("SELECT id_driver, name, surname FROM drivers WHERE driver_status = 'Inactive'")
opciones_conductores = {f"ID: {id_driver} | {name} {surname}": id_driver for id_driver, name, surname in conductores_activos}
nombre_conductor_seleccionado = st.selectbox("Selecciona un Conductor", list(opciones_conductores.keys()))
id_conductor_seleccionado = opciones_conductores[nombre_conductor_seleccionado]

#ETIQUETAS DEPENDIENTES
col1, col2, col3, col4 = st.columns(4)
#PRIMERO SACO LOS DATOS CON CONSULTA A CLOUD SQL - Para hacerlos dependientes de la lista que hemos seleccionado arriba
marca_coche = BaseDeDatos.consultar(f"SELECT brand FROM cars WHERE id_driver = {id_conductor_seleccionado}")
modelo_coche = BaseDeDatos.consultar(f"SELECT model FROM cars WHERE id_driver = {id_conductor_seleccionado}")
asientos_coche = BaseDeDatos.consultar(f"SELECT car_seats FROM cars WHERE id_driver = {id_conductor_seleccionado}")
adaptado_dis = BaseDeDatos.consultar(f"SELECT dissability_readyness FROM cars WHERE id_driver = {id_conductor_seleccionado}")
#MUESTRO LOS VALORES EN LA ETIQUETA
col1.metric("MODELO", f"{modelo_coche[0][0]}", delta=None, delta_color="normal", help=None, label_visibility="visible")
col2.metric("MARCA", f"{marca_coche[0][0]}", delta=None, delta_color="normal", help=None, label_visibility="visible")
col3.metric("ASIENTOS DISPONIBLES", f"{asientos_coche[0][0]}", delta=None, delta_color="normal", help=None, label_visibility="visible")
col4.metric("ADAPTADO", f"{adaptado_dis[0][0]}", delta=None, delta_color="normal", help=None, label_visibility="visible")

#EJEMPLO DE MAPA Y POSICIÓN - De momento estático, pero una vez el sistema de actualización esté completo, bastaría con que esas coordenadas sean las de la tabla "Active_Vehicle"
map_data = pd.DataFrame({
    'lat': [39.45897],
    'lon': [-0.3979]
})

# Mostrar el mapa en Streamlit
st.map(map_data)