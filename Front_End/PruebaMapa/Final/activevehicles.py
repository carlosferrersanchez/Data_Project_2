import streamlit as st
import pandas as pd
import time
from creacion_clase import BaseDeDatos
import streamlit as st

# Título del dashboard
st.title('BlablaCar Only Rides')

# Switch para mostrar vehículos activos
switch = st.checkbox("Show Active Vehicles", value=False)

# Lista desplegable de conductores activos
if switch:  # Si el switch está activado, entonces obtenemos y mostramos los conductores activos
    conductores_activos = BaseDeDatos.consultar("SELECT id_driver, name, surname FROM drivers WHERE driver_status = 'Active'")
    if conductores_activos:
        opciones_conductores = {f"ID: {id_driver} | {name} {surname}": id_driver for id_driver, name, surname in conductores_activos}
        nombre_conductor_seleccionado = st.selectbox("Selecciona un Conductor Activo", list(opciones_conductores.keys()))
        id_conductor_seleccionado = opciones_conductores[nombre_conductor_seleccionado]

        # Etiquetas dependientes y consulta de información del coche del conductor seleccionado
        col1, col2, col3, col4 = st.columns(4)
        marca_coche = BaseDeDatos.consultar(f"SELECT brand FROM cars WHERE id_driver = {id_conductor_seleccionado}")
        modelo_coche = BaseDeDatos.consultar(f"SELECT model FROM cars WHERE id_driver = {id_conductor_seleccionado}")
        asientos_coche = BaseDeDatos.consultar(f"SELECT car_seats FROM cars WHERE id_driver = {id_conductor_seleccionado}")
        adaptado_dis = BaseDeDatos.consultar(f"SELECT disability_readiness FROM cars WHERE id_driver = {id_conductor_seleccionado}")

        # Mostrar la información en etiquetas
        col1.metric("MARCA", marca_coche[0][0] if marca_coche else "N/A")
        col2.metric("MODELO", modelo_coche[0][0] if modelo_coche else "N/A")
        col3.metric("ASIENTOS DISPONIBLES", asientos_coche[0][0] if asientos_coche else "N/A")
        col4.metric("ADAPTADO", adaptado_dis[0][0] if adaptado_dis else "N/A")
    else:
        st.write("No hay conductores activos en este momento.")

#EJEMPLO DE MAPA Y POSICIÓN - De momento estático, pero una vez el sistema de actualización esté completo, bastaría con que esas coordenadas sean las de la tabla "Active_Vehicle"
map_data = pd.DataFrame({
    'lat': [39.45897],
    'lon': [-0.3979]
})

# Mostrar el mapa en Streamlit
st.map(map_data)


