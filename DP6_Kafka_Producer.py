from confluent_kafka import Producer
import requests
import time
import json

def get_vehicle_activations():
    # Configuración del productor de Kafka
    producer_conf = {
        'bootstrap.servers':'localhost:9092',
        'client.id': 'python-producer'
    }
    producer = Producer(producer_conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    # Solicitar registros de emergencia de la API
    for _ in range(5):
        response = requests.get('http://localhost:5000/new_active_vehicle') #cambiar dirección si dockerizado. En vez de localhost, el nombre del servicio "flask"
        if response.status_code == 200:
            emergencia_json = response.text
            producer.produce('New_active_vehicle', emergencia_json.encode('utf-8'), callback=delivery_report)
            producer.poll(0)
        else:
            print(f"Error al obtener datos de la API: {response.status_code}")
        
        time.sleep(10)  # Espera 1 segundo antes de enviar el siguiente registro

    producer.flush()

get_vehicle_activations()