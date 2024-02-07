'''
# Coordenadas a iterar
coordenadas = [
    (-0.3973, 39.45852),
    (-0.3979, 40.45898)
]

# Índice de la coordenada actual, guardado en la sesión
if 'indice_coordenada' not in st.session_state:
    st.session_state.indice_coordenada = 0

# Actualizar la coordenada
indice = st.session_state.indice_coordenada
map_data = pd.DataFrame({'lat': [coordenadas[indice][1]], 'lon': [coordenadas[indice][0]]})
st.map(map_data)

# Incrementar el índice
st.session_state.indice_coordenada = (indice + 1) % len(coordenadas)

# Esperar y luego reejecutar el script
time.sleep(10)
st.experimental_rerun()

'''