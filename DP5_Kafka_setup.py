from confluent_kafka.admin import AdminClient, NewTopic
import requests
import json
def creacion_topics():
    #CONEXIÓN
    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092" #kafka:29092 en docker, o localhost:9092 en local
    })
    # CREACIÓN DE TOPICS
    topic_names = ["New_active_vehicle","Update_position_active_vehicle","Update_position_ride"] 
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topic_names]

    futures = admin_client.create_topics(new_topics)
    #CONTROL MENSAJE EN CONSOLA
    for topic, future in futures.items():
        try:
            future.result()  
            print(f"Topic {topic} creado con éxito.")
        except Exception as e:
            print(f"No se pudo crear el topic {topic}: {e}")

creacion_topics()