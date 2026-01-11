from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from kafka import KafkaProducer
import json
from datetime import datetime
import asyncio
import os

api_id = 30793140
api_hash = 'fef4df78e71c7f00c45c4c174d1e3d8a'
phone_number = '+79853675776'  

# Каналы для мониторинга
CHANNELS = [
    1050820672, 1149896996, 1101170442, 1036362176, 
    1310155678, 1001872252, 1054549314, 1073571855
]

# Инициализация с сохранением сессии
session_file = os.path.expanduser('~/.telegram_session')
client = TelegramClient(session_file, api_id, api_hash)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@client.on(events.NewMessage(chats=CHANNELS))
async def handler(event):
    try:
        sender = await event.get_sender()
        username = sender.username if sender and sender.username else "anonymous"
        
        message = {
            'username': username,
            'timestamp': event.date.isoformat(),
            'channel_id': event.chat_id,
            'message_text': event.message.text[:100] if event.message.text else ""
        }
        
        producer.send('telegram_data', message)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ {username} | Channel: {event.chat_id}")
        
    except Exception as e:
        print(f"  Ошибка: {e}")

async def main():
    print("\n Подключение к Telegram...")
    try:
        await client.start(phone=phone_number)
        print(" Подключено к Telegram")
    except Exception as e:
        print(f" Ошибка подключения: {e}")
        return
    
    print(f"\n📡 Подключение к {len(CHANNELS)} каналам:")
    for chat_id in CHANNELS:
        try:
            await client(JoinChannelRequest(chat_id))
            print(f"   Канал {chat_id} присоединён")
        except Exception as e:
            print(f"   Ошибка подключения к {chat_id}: {e}")

    print("Ожидаю новых сообщений в каналах...\n")
    
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())


