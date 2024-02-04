import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import streamlit_shadcn_ui as ui
import streamlit as st
import pandas as pd
import time
from creacion_clase import BaseDeDatos

# Simulación de carga de datos desde una base de datos o fuente de datos
def cargar_datos():
    # Aquí iría tu código para cargar datos de la base de datos
    # Por ahora, usaremos datos de ejemplo
    return pd.DataFrame({
        'Fecha': pd.date_range(start='1/1/2022', periods=100),
        'Ventas': pd.np.random.randint(100, 500, size=(100,)),
        'Clientes': pd.np.random.randint(10, 50, size=(100,))
    })

datos = cargar_datos()

# Cálculo de métricas para las tarjetas
total_ventas = datos['Ventas'].sum()
total_clientes = datos['Clientes'].sum()
promedio_ventas = datos['Ventas'].mean()

# Diseño del dashboard
st.title('Dashboard de Ventas')

cols = st.columns(3)
with cols[0]:
    ui.metric_card(title="Total Ventas", content=f"${total_ventas:,.2f}", description="Total acumulado", key="card1")
with cols[1]:
    ui.metric_card(title="Clientes Atendidos", content=f"{total_clientes}", description="Total acumulado", key="card2")
with cols[2]:
    ui.metric_card(title="Promedio Ventas", content=f"${promedio_ventas:,.2f}", description="Promedio diario", key="card3")

# Creación de un gráfico simple
st.subheader('Tendencia de Ventas')
fig, ax = plt.subplots()
ax.plot(datos['Fecha'], datos['Ventas'], label='Ventas')
ax.plot(datos['Fecha'], datos['Clientes'], label='Clientes')
ax.set_xlabel('Fecha')
ax.set_ylabel('Cantidad')
ax.legend()
st.pyplot(fig)

# Mostrar una tabla con los datos
st.subheader('Datos Detallados')
st.table(datos)