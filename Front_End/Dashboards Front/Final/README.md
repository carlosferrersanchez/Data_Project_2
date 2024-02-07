# Dashboard de Distribución de Clientes y Operaciones de la Ciudad

Este proyecto consiste en un dashboard interactivo creado con Streamlit, una biblioteca de Python para desarrollar aplicaciones web de forma rápida y sencilla. El dashboard proporciona información detallada sobre la distribución de clientes y operaciones en una ciudad específica. A continuación, se detallan las características principales y el funcionamiento del dashboard:

## Características Principales

1. **Distribución de Clientes en Espera:**
   - Visualización en un mapa interactivo que muestra la ubicación de los clientes en espera para un viaje, representados por marcadores rojos.

2. **Análisis del Tiempo Medio de Espera:**
   - Cálculo del tiempo medio de espera para las solicitudes de viaje en minutos y segundos.

3. **Información Detallada de Viajes:**
   - Tabla interactiva que muestra detalles completos sobre los últimos viajes realizados, incluyendo identificadores de cliente, horarios de solicitud y finalización, estado de la solicitud y ubicación de recogida.

4. **Distribución de Estados de Viaje:**
   - Gráfico de barras que ilustra la distribución de los diferentes estados de los viajes, como "Esperando", "Completado", etc.

## Requisitos

- Se requiere una conexión a una base de datos PostgreSQL que contenga los datos relevantes sobre clientes y viajes.

## Configuración

1. Asegúrate de tener las siguientes bibliotecas Python instaladas. Puedes instalarlas ejecutando `pip install -r requirements.txt`:
   - Streamlit
   - Pandas
   - Psycopg2
   - Streamlit_Shadcn_UI

2. Proporciona las credenciales de tu base de datos PostgreSQL (host, nombre de la base de datos, usuario, contraseña y puerto) al inicio del script.

## Uso

- Ejecuta el script del dashboard y accede a la URL proporcionada en la terminal para abrir la aplicación en tu navegador web.
- Explora los diferentes paneles y visualizaciones para analizar la distribución de clientes y las operaciones de la ciudad.

## Licencia

Este proyecto está bajo la [Licencia MIT](LICENSE).


