'''
Программа читает CSV файл с данными о недвижимости в Португалии и отправляет их в указанный Kafka топик с заданной скоростью.
'''
import argparse
import time
import json
from datetime import datetime, timezone
import pandas as pd
from kafka import KafkaProducer, errors

def get_producer(bootstrap: str) -> KafkaProducer:
    """
    Подключение к Kafka с повторными попытками при неудаче.
    :param bootstrap:
    :return: Kafka client instance
    """
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8')
            )
            print(f"Connected to Kafka at {bootstrap}")
            return producer
        except errors.NoBrokersAvailable:
            print(f"No brokers available at {bootstrap}, retrying in 5s...")
            time.sleep(5)

def main(csv_path: str, bootstrap: str, topic: str, rate: float, limit: int | None) -> None:
    """
    Чтение CSV и отправка записей в Kafka с заданной скоростью.

    :param csv_path: путь к CSV файлу
    :param bootstrap: адрес Kafka брокера для подключения
    :param topic: имя топика в Kafka, куда отправляются сообщения
    :param rate: скорость отправки сообщений в Kafka (сообщений в секунду)
    :param limit: максимальное количество сообщений, которое нужно отправить
    :return:
    """
    df = pd.read_csv(csv_path)

    # Создаем Kafka producer
    producer = get_producer(bootstrap)

    # Вычисляем общее количество сообщений и задержку между отправками
    total = len(df) if limit is None else min(limit, len(df))

    # Вычисляем задержку между сообщениями для достижения желаемой скорости
    delay = 1.0 / rate if rate > 0 else 0.0

    # Передаем сообщения в Kafka
    for idx in range(total):
        row = df.iloc[idx]
        record = row.to_dict()

        # добавляем временную метку для стриминга
        record["event_time"] = datetime.now(timezone.utc).isoformat() + "Z"

        key = record.get("district", idx)
        producer.send(topic, key=key, value=record)

        # периодически гарантируем доставку
        if idx % 200 == 0:
            producer.flush()

        print(f"Sent {idx+1}/{total} to topic={topic}")
        time.sleep(delay)

    producer.flush()
    print("Done. All messages sent.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--csv", dest="csv", required=True, help="Path to CSV")
    p.add_argument("--bootstrap", dest="bootstrap", default="localhost:9092", help="kafka bootstrap server")
    p.add_argument("--topic", default="portugal_properties", help="Kafka topic name")
    p.add_argument("--rate", dest="rate", type=float, default=10.0, help="messages per second")
    p.add_argument("--limit", dest="limit", type=int, default=None, help="limit messages (for test)")
    args = p.parse_args()
    main(
        csv_path=args.csv,
        bootstrap=args.bootstrap,
        topic=args.topic,
        rate=args.rate,
        limit=args.limit
    )