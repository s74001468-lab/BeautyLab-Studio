# BeautyLab Studio Telegram Bot

Бот для записи клиентов салона красоты BeautyLab.

## Инструкция по запуску:

1. Скопируйте файл `.env.example` в `.env` и заполните своими данными:
   ```bash
   cp .env.example .env
   ```
2. Изучите файл `sheets_setup.md`, чтобы подключить Google Таблицу и получить файл `credentials.json`.
3. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
4. Запустите бота локально:
   ```bash
   python main.py
   ```
