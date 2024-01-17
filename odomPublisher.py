from confluent_kafka import Producer
import json
import time
import random

# Configuración del productor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la dirección IP de tu contenedor Kafka
    'client.id': 'python-producer'
}

# Crea el productor
producer = Producer(conf)

def delivery_report(err, msg):
    """Callback llamado una vez que el mensaje ha sido entregado o ha fallado."""
    if err is not None:
        print('Error al entregar el mensaje: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))

def send_random_message(producer, topic):
    """Envía un mensaje al topic con valores aleatorios."""
    ruta_json = {
        'id': random.randint(1, 100),
        'latitud': round(random.uniform(-90, 90), 6),
        'longitud': round(random.uniform(-180, 180), 6)
    }

    # Imprime el JSON antes de enviarlo al topic
    print(ruta_json)

    producer.produce(topic, key='key', value=json.dumps(ruta_json), callback=delivery_report)
    producer.flush()

# Envia mensajes al topic 'rutas' con valores aleatorios
try:
    while True:
        send_random_message(producer, 'rutas')
        time.sleep(1)  # Espera 1 segundo entre mensajes
except KeyboardInterrupt:
    pass
finally:
    # Asegúrate de que todos los mensajes pendientes se hayan entregado antes de cerrar el productor
    producer.flush()
