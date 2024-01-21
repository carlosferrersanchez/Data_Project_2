import streamlit as st
import pydeck as pdk

MAPBOX_ACCESS_TOKEN = "pk.eyJ1IjoicGF1bHZsYyIsImEiOiJjbHJudDJybzAxZmxtMmtrMGQ5d29jNG91In0.VJX0fw8L8sH12CrEHukY4A"

st.title("Valencia Map Demo")

view_state = pdk.ViewState(
    latitude=39.46975,  # Latitude of Valencia
    longitude=-0.37739,  # Longitude of Valencia
    zoom=11,
    pitch=0,
)

st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/streets-v11",
    initial_view_state=view_state,
    mapbox_key=MAPBOX_ACCESS_TOKEN,
))


