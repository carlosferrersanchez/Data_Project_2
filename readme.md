
# Data Project 2: OnlyCars

¡Bienvenidos al Data Project 2! Este proyecto ha sido desarrollado por: Juan Cornejo, Fernando Cabrera, Pablo Ivorra y Carlos Ferrer.

# Índice

En este repositorio, se encuentra la solución en Google Cloud que hemos diseñado. Consta de las siguientes partes:
- Objetivo del Data Project
- Flujograma del proceso
- Generador de la Base de Datos y Actualizador del movimiento
- Dataflow en Streaming
- Streamlit y mapa con información de los usuarios


# Objetivo del Data Project 
El objetivo de este Data Project es la creación e implementación de un sistema de rides en la ciudad de Valencia, cogiendo como base el modelo de negocio de la empresa Blablacar.

Nuestro programa OnlyCars recoge usuarios en toda la ciudad, tanto conductores dispuestos a compartir rutas en coche como usuarios que quieren encontrar drivers que coincidan con su destino.

Nuestro Modelo se ha tratado de un trabajo en Google Cloud, podemos observar en el siguiente diagrama el aproach de nuestro proyecto:

![BBDComputacionD](/images/Computacion.png)

En este esquema podemos ver todo el alcance que hemos tenido y lo que hemos querido mostrar y ejecutar, una aplicación gestionada y alojada en su mayoría en Google Cloud, menos los generadores y demás procesos "locales" para la demostración del proyecto.

# Flujograma del proceso

En este repositorio, se encuentra la solución en Google Cloud que hemos diseñado. Consta de las siguientes partes:

1. Generador e ingesta a la BBDD en Google Cloud SQL

2. Actualizador del movimiento de los usuarios y drivers

3. Generador de datos con envío a Pub/Sub

4. Dataflow con matcheos de usuarios y drivers en streaming

5. Streaming y visualizador con Streamlit de un mapa de Valencia a tiempo real

En cada paso del flujograma presentado hemos podido trabajar en equipo para tener una solucion factible y que responda la premisa del proyecto y del desafío, ganando una gran **trazabilidad** sobre todos los usuarios convergentes en la aplicación.

Aquí podemos ver a grandes rasgos la estructura y nuestro proceso en el Data Project:

![Proceso DP2](/images/Proceso.png)

Como podemos observar, se detalla cada proceso y cada parte comentada, desde la creacion de los usuarios de manera en este caso artifical, como su actializacion, ingesta y matcheo y registro en nuestra base de datos.


# Generador de la Base de Datos y Actualizador del movimiento

La primera tarea concebida a la hora de crear nuestro proyecto fue diseñar una solida Base de datos.

Podemos observar su estructura aquí:

![BBDD](/images/BBDD.png)

Esta estuctura está formada de tal forma que recoge todos nuestros datos durante todo el proceso de la aplicación, de forma que tenemos datos iniciales y estáticos, como misma información sobre los usuarios, como datos dinámicos que nos permiten trazar y saber en que punto se encuentra cada usuario.

De forma inicial tenemos un par de tablas pre-ingestadas, pero por otro lado tenemos tablas actualizables y donde ingestamos los datos transformados.

Como resultado tenemos un DWH en Google Cloud SQL que nos permite desplegar todo nuestro servicio.

Cabe destacar que tenemos un sistema de actualización de movimiento, por lo que las tablas están recogiendo en todo momento información actual de los usuarios, tanto clientes y conductores, en que punto están, cuantas personas caben en el coche, si está lleno o no, si hay matcheo o no, etc.

Gracias a Cloud SQL podemos tener este **sistema de actualización a tiempo real** por usuario.


# Dataflow en Streaming

Para poder hacer funcionar el Dataflow y el proceso de Match, tenemos un Publiser de mensajes que bebe de la Basde de Datos, este publicador envia cada pocos segundos los datos de **todos los usuarios activos**.

Este Dataflow: 

![Dataflow](/images/Dataflow.png)

Recoge esos mensajes, los lee, los transforma y los devuelve a otro Pub/Sub, si hablamos mas en detalle, su función consta de, mediante ventanas de tiempo, comprar en streaming todos los datos de todos los usuarios activos, encontrando similitudes de rutas y devolviendo mensajes por cada matcheo o cada resultado.

Estos Matcheos constan de varios indicadores o checks:

1. Comparar las **rutas** de los usuarios y los drivers
2. Comparar y comprobar si tiene **asientos disponibles** para la solicitud.
3. Si el cliente y el conductor se encuentran a **menos de 50 metros** en la posición actual.

Si estas 3 condiciones se cumplen, **se genera un Match**, devolviendo un archivo JSON con los datos de los dos usuarios, la posición donde se encuentran, hacia donde se dirigen y el nuevo número de asientos disponibles que tiene el coche.

Cabe destacar que se genera un ID para el MATCH único, ya que cada cliente **solo puede tener 1 match** pero un conductor puede tener varios, así tenemos más trazabilidad sobre los viajes. 

# Streamlit y mapa con información de los usuarios

Por último, hemos querido representar los datos en un Mapa de Valencia con información completa y en tiempo real.

Gracias a Streamlit, podemos representar dónde se encuentran nuestros usuarios, tanto los que tienen coincidencias y matcheos como los que no. Podemos encontrar información también separada y ver diferentes indicadores, ya sea rating como precio medio generado de los drivers.

