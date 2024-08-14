from confluent_kafka import Producer
import json
from clickhouse_driver import Client
from typing import *

dbname = 'default'

with open('E:\Work\kafka\secrets\pass.json') as json_file:
    data = json.load(json_file)
    
client = Client(data['server'][0]['host'],
                user=data['server'][0]['user'],
                password=data['server'][0]['password'],
                port=data['server'][0]['port'],
                verify=False,
                database=dbname,
                settings={"wait_end_of_query": True,
                          "numpy_columns": False, 'use_numpy': False},
                compression=True)


# kafka conn
config = {
    'bootstrap.servers': 'localhost:9093',  # адрес Kafka сервера
    'client.id': 'simple-producer',
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.username': 'admin',
    'sasl.password': 'admin-secret'
}

producer = Producer(**config)


# start of script
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def send_message(recieved_data):
    try:
        # Асинхронная отправка сообщения
        producer.produce('first', recieved_data.encode(
            'utf-8'), callback=delivery_report)
        # Поллинг для обработки обратных вызовов
        producer.poll(0)
    except BufferError:
        print(
            f"Local producer queue is full ({len(producer)} messages awaiting delivery): try again")



if __name__ == '__main__':
    query = '''select 
                    shk_id, 
                    reason_id, 
                    operation_dt, 
                    place_code,
                    employee_id,
                    chrt_id,
                    seller_id 
                from shkDefect_verdict
                where operation_dt >= yesterday() 
                limit 5000'''
    rows = client.execute(query)

    for row in rows:
        json_row = json.dumps(
            {
                'shk_id': row[0],
                'reason_id': row[1],
                'operation_dt': row[2].strftime('%Y-%m-%dT%H:%M:%S'),
                'place_code': row[3],
                'employee_id': row[4],
                'chrt_id': row[5],
                'seller_id': row[6],
            }
        )
        print(f'[LOG] send to kafka: {json_row}')
        send_message(json_row)
        producer.flush()