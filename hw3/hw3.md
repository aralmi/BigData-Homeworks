# Домашнее задание 3 — Spark Streaming + Kafka 

## Описание

В работе реализована архитектура потоковой обработки сообщений из Telegram‑каналов через Kafka и Spark Streaming.  
Из‑за технической проблемы с авторизацией Telegram (ошибка `UPDATE_APP_TO_LOGIN` при использовании реального номера телефона и корректных `api_id`/`api_hash`), в качестве источника данных используется **тестовый (mock) producer**, который генерирует сообщения, полностью эквивалентные структуре реальных Telegram‑сообщений, и отправляет их в Kafka.

---

## Структура проекта


Основные файлы:

- `telegram_producer.py` — реальный Telegram‑producer на Telethon (соответствует условию ДЗ, но не запускается из‑за ошибки авторизации Telegram).
- `telegram_producer_mock.py` — mock‑producer, генерирует тестовые сообщения и отправляет их в Kafka.
- `spark_streaming_consumer.py` — Spark Streaming consumer, читает данные из Kafka и считает агрегаты в окнах 1/10 минут.


## Реальный Telegram‑producer (почему не используется)


### Фактическая ошибка при запуске

Команда:

```bash
/opt/anaconda3/bin/python3 ~/Desktop/Bigdata/hw3/telegram_producer.py
```

После ввода номера телефона `+79853675776` выводится:

```text
Подключение к Telegram...
Ошибка подключения: RPCError 406: UPDATE_APP_TO_LOGIN (caused by SendCodeRequest)
```

Смысл ошибки: сервер Telegram требует обновления/изменения клиента для входа, и авторизация через Telethon с указанными параметрами блокируется на стороне Telegram API, несмотря на корректность `api_id`/`api_hash` и номера телефона.

Поэтому в работе используется mock‑producer, а не реальный Telegram‑клиент, при сохранении всей остальной архитектуры (Kafka + Spark).


## Spark Streaming consumer

**Файл:** `spark_streaming_consumer.py`

Функциональность:

- чтение потока из Kafka‑топика `telegram_data`;
- парсинг JSON в колонки `username`, `timestamp`, `channel_id`, `message_text`;
- преобразование `timestamp` в тип `timestamp`;
- подсчёт количества сообщений по пользователям:
  - окно 1 минута, шаг 30 секунд;
  - окно 10 минут, шаг 30 секунд;
- вывод результатов в консоль в режиме `update`.


Пример фактического вывода при работе mock‑producer:

```text
Batch: 5
+-------------------+-------------------+--------+-------------+
|start              |end                |username|message_count|
+-------------------+-------------------+--------+-------------+
|2026-01-08 18:58:00|2026-01-08 19:08:00|vasya   |3            |
...
```


## Порядок запуска

### 1. Zookeeper

```bash
cd ~/Desktop/kafka_2.13-3.6.0
./bin/zookeeper-server-start.sh config/zookeeper.properties
```

Терминал оставить открытым.

### 2. Kafka Broker

```bash
cd ~/Desktop/kafka_2.13-3.6.0
./bin/kafka-server-start.sh config/server.properties
```

Терминал оставить открытым.

### 3. Создание топика `telegram_data` (один раз)

```bash
cd ~/Desktop/kafka_2.13-3.6.0
./bin/kafka-topics.sh --create \
  --topic telegram_data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

После сообщения `Created topic telegram_data.` терминал можно закрыть.

### 4. (Опционально) Просмотр сообщений из Kafka

```bash
cd ~/Desktop/kafka_2.13-3.6.0
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic telegram_data \
  --from-beginning
```

Здесь видны JSON‑сообщения, которые отправляет producer.

### 5. Запуск Mock‑producer

```bash
cd ~/Desktop/Bigdata/hw3
/opt/anaconda3/bin/python3 telegram_producer_mock.py
```

Mock‑producer раз в 2 секунды отправляет в Kafka сообщения с случайным `username` и `channel_id`.

### 6. Запуск Spark Streaming consumer

```bash
cd ~/Desktop/Bigdata/hw3
/opt/anaconda3/bin/python3 spark_streaming_consumer.py
```

Через несколько секунд появляются батчи с подсчитанными `message_count` по пользователям и временным окнам.

---

## Вывод

- Архитектура mock‑источник → Kafka → Spark Streaming реализована и демонстрирует подсчёт сообщений в скользящих окнах.
- Код реального Telegram‑producer соответствует требованиям ДЗ, но авторизация из‑за ошибки `RPCError 406: UPDATE_APP_TO_LOGIN` не проходит, даже при корректных `api_id`/`api_hash` и номере телефона.
- Для демонстрации решения используется mock‑producer с тем же форматом данных, что позволяет полноценно показать работу части с Kafka и Spark Streaming.