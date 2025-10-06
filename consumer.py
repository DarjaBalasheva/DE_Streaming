'''
Программа для потребления сообщений из Kafka, фильтрации, агрегации и сохранения в CSV.
'''
import argparse
import json
import time
from kafka import KafkaConsumer, errors
import pandas as pd

CSV_OUTPUT = "./result/aggregated_results.csv"

def get_consumer(bootstrap: str, topic: str) -> KafkaConsumer:
    """
    Подключение к Kafka с повторными попытками при неудаче.
    :param bootstrap:  Адрес Kafka брокера для подключения
    :param topic: Имя топика в Kafka откуда забираем сообщения
    :return: Kafka client instance
    """
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap],
                auto_offset_reset='earliest',
                group_id='property_aggregator',
                value_deserializer=lambda v: json.loads(v)
            )
            print(f"Connected to Kafka at {bootstrap}")
            return consumer
        except errors.NoBrokersAvailable:
            print(f"No brokers available at {bootstrap}, retrying in 5s...")
            time.sleep(5)

def process_stream(bootstrap: str, topic: str, agg_interval: float) -> None:
    """
    Чтение сообщений из Kafka, фильтрация, агрегация и сохранение в CSV.
    Аггрегация происходит каждые 30 секунд.
    :param agg_interval: Временной интервал для агрегации в секундах
    :param bootstrap: адрес Kafka брокера для подключения
    :param topic: имя топика в Kafka, куда отправляются сообщения
    :return:
    """

    # Создаем Kafka consumer
    consumer = get_consumer(bootstrap, topic)

    buffer = [] # буфер для накопления сообщений
    last_agg_time = time.time()  # инициализация времени последней агрегации

    for msg in consumer:
        record = msg.value
        try:
            price = float(record.get("Price", 0))
            area = float(record.get("TotalArea", 0))
        except ValueError:
            continue  # пропускаем некорректные строки, чтобы не упасть в ошибку

        # фильтр по цене и площади
        if price < 50000 or area < 20:
            continue

        # добавляем запись в буфер
        buffer.append(record)

        if time.time() - last_agg_time >= agg_interval:
            print("30 seconds passed, aggregating data")
            if buffer:
                # создаем DataFrame из буфера
                df = pd.DataFrame(buffer)
                grouped = df.groupby(["Type", "District"]).agg(
                    avg_price=("Price", "mean"),
                    avg_area=("TotalArea", "mean"),
                    count=("Price", "count")
                ).reset_index()

                # вычисляем среднюю цену за м2
                grouped["avg_price_per_m2"] = grouped["avg_price"] / grouped["avg_area"]

                # сохраняем результат
                grouped.to_csv(CSV_OUTPUT, index=False)
                print(f"[{time.strftime('%X')}] Aggregated {len(buffer)} messages -> {CSV_OUTPUT}")

                buffer.clear()
            last_agg_time = time.time() # обновляем время последней агрегации

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", dest="bootstrap", default="kafka:9092", help="kafka bootstrap server")
    p.add_argument("--topic", default="portugal_properties", help="Kafka topic name")
    p.add_argument("--agg_interval", type=float, default=30.00, help="aggregation interval in seconds")
    args = p.parse_args()
    process_stream(args.bootstrap, args.topic, args.agg_interval)
