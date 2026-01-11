from kafka import KafkaProducer
import json
from datetime import datetime
import time
import random

# Мок данные вместо реального Telegram
MOCK_USERS = ['vasya', 'masha', 'ivan', 'olga', 'dmitry', 'anna']
MOCK_CHANNELS = [1050820672, 1149896996, 1101170442, 1036362176]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("=" * 80)
print("MOCK TELEGRAM PRODUCER ИНИЦИАЛИЗИРОВАН")
print("=" * 80)
print("\n📡 Отправляю тестовые сообщения в Kafka...\n")

try:
    while True:
        username = random.choice(MOCK_USERS)
        channel_id = random.choice(MOCK_CHANNELS)
        message_text = f"Test message from {username}"
        
        message = {
            'username': username,
            'timestamp': datetime.now().isoformat(),
            'channel_id': channel_id,
            'message_text': message_text
        }
        
        producer.send('telegram_data', message)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ {username} | Channel: {channel_id}")
        
        time.sleep(2)  # Отправляй каждые 2 секунды

except KeyboardInterrupt:
    print("\n\n✓ Producer остановлен")
    producer.close()
