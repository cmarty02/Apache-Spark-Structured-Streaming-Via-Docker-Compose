from confluent_kafka import Consumer, KafkaError

# Configuración del consumidor de Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'  # Lee desde el principio del topic si no hay offset almacenado
}

# Crea el consumidor
consumer = Consumer(conf)

# Suscríbete al topic "rutas"
consumer.subscribe(['rutas'])

try:
    while True:
        # Espera mensajes
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fin de la partición, no es un error
                continue
            else:
                print(f'Error al recibir mensaje: {msg.error()}')
                break

        # Imprime el valor del mensaje
        print(f'Recibido mensaje: {msg.value().decode("utf-8")}')

except KeyboardInterrupt:
    pass

finally:
    # Cierra el consumidor
    consumer.close()
