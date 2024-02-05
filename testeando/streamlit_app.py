import streamlit as st
import pandas as pd
import folium
import json
import base64
from google.cloud import bigquery
import time

# Función para obtener datos desde BigQuery
def get_bigquery_data(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    df = client.query(query).to_dataframe()
    return df

def main():
    st.title("Visualización de Datos desde BigQuery a Streamlit")

    # Configura tus detalles de proyecto, conjunto de datos y tabla
    project_id = 'vigilant-willow-411714'
    dataset_id = 'dataset_streamlit'
    table_id = 'tabla_streamlit'

    # Recupera los datos de BigQuery
    df = get_bigquery_data(project_id, dataset_id, table_id)

    # Muestra el DataFrame en Streamlit
    st.write("Datos de BigQuery:")
    st.dataframe(df)

    # Coordenadas de la ruta
    route_coordinates = [
        [39.460049048762, -0.332885245795235],
        [39.46242982798688, -0.32848241816521306],
        [39.46565697652788, -0.35190134923325567],
        [39.46590183941864, -0.3710909646224089],
        [39.46615860244742, -0.3816803253877278],
        [39.470846431781624, -0.37729223189941474]
    ]

    # Establece las coordenadas del centro de Valencia, España
    valencia_center_coordinates = [39.4699, -0.3763]

    # Contenedor para el mapa
    map_container = st.empty()

    while True:
        for i in range(len(df)):
            # Visualiza las coordenadas en un mapa con folium
            m = folium.Map(location=valencia_center_coordinates, zoom_start=13)  # Ajusta el nivel de zoom aquí

            # Agrega un marcador para cada conjunto de coordenadas en la fila actual
            coordinates = [float(coord) for coord in df.loc[i, 'coordinates'].strip('[]').split(', ')]
            icon = folium.Icon(color='red', icon='car', prefix='fa')
            folium.Marker(location=coordinates, popup=f"Vehicle ID: {df.loc[i, 'vehicle_id']}", icon=icon).add_to(m)

            # Agrega una línea roja que conecta las coordenadas de la ruta
            folium.PolyLine(locations=route_coordinates, color='red').add_to(m)

            # Convierte el mapa de Folium a HTML y muestra el HTML directamente en Streamlit
            map_html = f'<iframe width="1000" height="500" src="data:text/html;base64,{base64.b64encode(m._repr_html_().encode()).decode()}" frameborder="0" allowfullscreen="true"></iframe>'
            map_container.markdown(map_html, unsafe_allow_html=True)

            # Espera 2 segundos antes de la próxima actualización
            time.sleep(2)

if __name__ == "__main__":
    main()
