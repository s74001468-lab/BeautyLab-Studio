import asyncio
import logging
import os
from datetime import datetime, timedelta
import uuid
import gspread
from google.oauth2.service_account import Credentials
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
SHEETS_CREDENTIALS_PATH = os.getenv("SHEETS_CREDENTIALS_PATH", "credentials.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

# --- Настройка Google Sheets ---
def init_sheets():
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_file(SHEETS_CREDENTIALS_PATH, scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
        logging.info("✅ Успешное подключение к Google Sheets!")
        return sheet
    except Exception as e:
        logging.error(f"🚨 Ошибка подключения к Google Sheets: {e}")
        return None

sheet = init_sheets()

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)

# Инициализируем планировщик задач
scheduler = AsyncIOScheduler()

# --- Логика напоминаний ---
async def send_reminder(user_id: int, text: str):
    try:
        await bot.send_message(user_id, text)
    except Exception as e:
        logging.error(f"Не удалось отправить напоминание юзеру {user_id}: {e}")

def schedule_reminders_for_booking(booking_id, telegram_id, b_date_str, b_time_str, service, master):
    b_dt_str = f"{b_date_str} {b_time_str}"
    try:
        booking_dt = datetime.strptime(b_dt_str, "%d.%m.%Y %H:%M")
    except Exception as e:
        logging.error(f"Ошибка парсинга даты для напоминания: {e}")
        return
        
    now = datetime.now()
    
    # За 24 часа
    rem_24_dt = booking_dt - timedelta(hours=24)
    if rem_24_dt > now:
        text_24 = f"🔔 Напоминание: у вас запись завтра в {b_time_str} на услугу '{service}' к мастеру {master}.\n\nЕсли планы изменились, пожалуйста, предупредите нас!"
        scheduler.add_job(send_reminder, 'date', run_date=rem_24_dt, args=[telegram_id, text_24], id=f"rem_24_{booking_id}", replace_existing=True)
        
    # За 2 часа
    rem_2_dt = booking_dt - timedelta(hours=2)
    if rem_2_dt > now:
        text_2 = f"🔔 Напоминание: ждем вас сегодня в {b_time_str} на услугу '{service}'. Мастер {master} будет готов!"
        scheduler.add_job(send_reminder, 'date', run_date=rem_2_dt, args=[telegram_id, text_2], id=f"rem_2_{booking_id}", replace_existing=True)

    # За 10 минут
    rem_10_dt = booking_dt - timedelta(minutes=10)
    if rem_10_dt > now:
        text_10 = f"🔔 Напоминание: мы ждем вас буквально через 10 минут! 😊"
        scheduler.add_job(send_reminder, 'date', run_date=rem_10_dt, args=[telegram_id, text_10], id=f"rem_10_{booking_id}", replace_existing=True)

def cancel_reminders(booking_id):
    # Удаляем все связанные с этой записью напоминания
    for prefix in ["rem_24_", "rem_2_", "rem_10_"]:
        job_id = f"{prefix}{booking_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)

async def restore_reminders_from_sheet():
    if not sheet:
        return
    try:
        sheet_data = await asyncio.to_thread(sheet.get_all_values)
        count = 0
        for row in sheet_data[1:]:
            if len(row) >= 11 and row[10] == "Active":
                b_id = row[0]
                t_id = int(row[1])
                service = row[4]
                master = row[5]
                b_date = row[6]
                b_time = row[7]
                schedule_reminders_for_booking(b_id, t_id, b_date, b_time, service, master)
                count += 1
        logging.info(f"✅ Успешно восстановлены напоминания для {count} активных записей.")
    except Exception as e:
        logging.error(f"Ошибка восстановления напоминаний: {e}")

# --- Машина состояний (FSM) для процесса записи ---
class BookingState(StatesGroup):
    waiting_for_service = State()
    waiting_for_master = State()
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_name = State()
    waiting_for_phone = State()
    waiting_for_comment = State()

# Главное меню
def get_main_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Записаться ✨"), KeyboardButton(text="Мои записи 📅")],
        [KeyboardButton(text="Связаться с администратором 👩‍💼")]
    ], resize_keyboard=True)

# Клавиатура выбора услуги
def get_services_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Женская стрижка (1ч)", callback_data="service_женская_стрижка")],
        [InlineKeyboardButton(text="Мужская стрижка (30мин)", callback_data="service_мужская_стрижка")],
        [InlineKeyboardButton(text="Окрашивание (1.5ч)", callback_data="service_окрашивание")],
        [InlineKeyboardButton(text="Маникюр/Педикюр (1ч)", callback_data="service_маникюр")],
    ])

# Клавиатура выбора мастера
def get_masters_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Анна", callback_data="master_анна"), 
         InlineKeyboardButton(text="Мария", callback_data="master_мария")],
        [InlineKeyboardButton(text="Алексей", callback_data="master_алексей"),
         InlineKeyboardButton(text="Любой мастер", callback_data="master_любой")],
    ])

# Клавиатура выбора даты (динамическая на 5 дней вперед)
WEEKDAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

def get_dates_keyboard():
    keyboard = []
    for i in range(1, 6): # Завтра + 4 дня
        date_obj = datetime.now() + timedelta(days=i)
        date_str = date_obj.strftime("%d.%m.%Y")
        weekday_str = WEEKDAYS_RU[date_obj.weekday()]
        btn_text = f"{date_str} ({weekday_str})"
        keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"date_{date_str}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# --- Настройки времени и услуг ---
SERVICES_DURATION = {
    "женская стрижка": 60,
    "мужская стрижка": 30,
    "окрашивание": 90,
    "маникюр": 60,
    "педикюр": 60
}

def generate_all_slots():
    slots = []
    start_time = datetime.strptime("10:00", "%H:%M")
    end_time = datetime.strptime("20:00", "%H:%M") # Салон работает до 20:00
    current = start_time
    while current < end_time:
        slots.append(current.strftime("%H:%M"))
        current += timedelta(minutes=30)
    return slots

def get_available_times(sheet_data, requested_date, requested_master, requested_duration):
    unavailable_segments = set()
    
    for row in sheet_data:
        # Проверяем, что в строке достаточно данных (как минимум 11 столбцов)
        if len(row) < 11:
            continue
        status = row[10]
        if status != "Active":
            continue
            
        date = row[6]
        master = row[5]
        
        # Если выбран "Любой мастер", мы должны проверять занятость всех,
        # но для MVP просто смотрим: тот же мастер или "Любой"
        if date == requested_date and (master == requested_master or requested_master == "Любой мастер" or master == "Любой мастер"):
            time_str = row[7]
            dur_str = row[8]
            try:
                b_time = datetime.strptime(time_str, "%H:%M")
                b_dur = int(dur_str)
            except:
                continue
            
            # Блокируем каждый 30-минутный отрезок, который занимает существующая запись
            b_current = b_time
            b_end = b_time + timedelta(minutes=b_dur)
            while b_current < b_end:
                unavailable_segments.add(b_current.strftime("%H:%M"))
                b_current += timedelta(minutes=30)
                
    available_slots = []
    all_slots = generate_all_slots()
    
    for slot_str in all_slots:
        slot_time = datetime.strptime(slot_str, "%H:%M")
        
        # Смотрим, поместится ли НАША длительность услуги (requested_duration), начиная с этого slot_time
        can_fit = True
        slot_end = slot_time + timedelta(minutes=requested_duration)
        if slot_end > datetime.strptime("20:00", "%H:%M"):
            can_fit = False # Услуга выходит за рамки рабочего дня салона
        else:
            check_time = slot_time
            while check_time < slot_end:
                if check_time.strftime("%H:%M") in unavailable_segments:
                    can_fit = False # Это время уже занято другой записью
                    break
                check_time += timedelta(minutes=30)
                
        if can_fit:
            available_slots.append(slot_str)
            
    return available_slots

# Клавиатура выбора времени (динамическая)
def get_times_keyboard(available_times):
    if not available_times:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Нет свободных окон 😔", callback_data="no_time")]
        ])
    
    keyboard = []
    row = []
    # Разобьем кнопки по 3 в ряд (чтобы вместились все 30-минутные шаги)
    for t in available_times:
        row.append(InlineKeyboardButton(text=t, callback_data=f"time_{t}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        "Добро пожаловать в BeautyLab Studio! 🌸\n\n"
        "Здесь вы можете записаться к нашим мастерам.\n"
        "Нажимая 'Записаться', вы даете согласие на обработку персональных данных."
    )
    await message.answer(text, reply_markup=get_main_keyboard())

# Кнопка связи с администратором
@dp.message(F.text == "Связаться с администратором 👩‍💼")
async def contact_admin(message: Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Написать Сергею ✉️", url="https://t.me/SergeyLending")]
    ])
    await message.answer("Если у вас возникли вопросы или нужна помощь, напишите нашему администратору:", reply_markup=keyboard)

# --- Обработчики процесса записи ---

# Нажатие на "Записаться"
@dp.message(F.text == "Записаться ✨")
async def start_booking(message: Message, state: FSMContext):
    await state.set_state(BookingState.waiting_for_service)
    await message.answer("Выберите услугу:", reply_markup=get_services_keyboard())

# Выбор услуги
@dp.callback_query(BookingState.waiting_for_service, F.data.startswith("service_"))
async def process_service(callback: CallbackQuery, state: FSMContext):
    service = callback.data.replace("service_", "").replace("_", " ")
    await state.update_data(service=service)
    await state.set_state(BookingState.waiting_for_master)
    await callback.message.edit_text("Выберите мастера:", reply_markup=get_masters_keyboard())

# Выбор мастера
@dp.callback_query(BookingState.waiting_for_master, F.data.startswith("master_"))
async def process_master(callback: CallbackQuery, state: FSMContext):
    master = callback.data.replace("master_", "").capitalize()
    await state.update_data(master=master)
    
    await state.set_state(BookingState.waiting_for_date)
    await callback.message.edit_text("Выберите дату визита:", reply_markup=get_dates_keyboard())

# Выбор даты
@dp.callback_query(BookingState.waiting_for_date, F.data.startswith("date_"))
async def process_date(callback: CallbackQuery, state: FSMContext):
    date = callback.data.replace("date_", "")
    await state.update_data(date=date)
    
    data = await state.get_data()
    service_name = data.get('service', '').lower()
    master = data.get('master', '')
    
    # Определяем реальную длительность услуги, по умолчанию 60 минут
    duration = 60
    for s_key, s_dur in SERVICES_DURATION.items():
        if s_key in service_name:
            duration = s_dur
            break
    await state.update_data(duration=duration)
    
    # Даем понять пользователю, что бот думает и грузит таблицу
    await callback.message.edit_text("⏳ Ищу свободные окошки, подождите пару секунд...")
    
    try:
        if sheet:
            # Вытягиваем все заявки из таблицы в фоновом режиме (чтобы бот не вис)
            sheet_data = await asyncio.to_thread(sheet.get_all_values)
            sheet_data = sheet_data[1:] if len(sheet_data) > 0 else [] # Убираем заголовок
        else:
            sheet_data = [] # Если таблица отвалилась, считаем что все свободно
            
        # Запускаем нашу "умную" функцию расчета окошек
        available_times = get_available_times(sheet_data, date, master, duration)
        
        await state.set_state(BookingState.waiting_for_time)
        text = f"Свободное время на {date}\n(Ваша услуга займет {duration} минут):"
        await callback.message.edit_text(text, reply_markup=get_times_keyboard(available_times))
    except Exception as e:
        logging.error(f"Ошибка загрузки расписания: {e}")
        await callback.message.edit_text("Произошла ошибка базы данных. Попробуйте нажать /start заново.")

# Обработчик кнопки "Нет окон"
@dp.callback_query(BookingState.waiting_for_time, F.data == "no_time")
async def process_no_time(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Выберите другой день!", show_alert=True)
    await state.set_state(BookingState.waiting_for_date)
    await callback.message.edit_text("Выберите дату визита:", reply_markup=get_dates_keyboard())

# Вспомогательная функция для поиска старых данных клиента
async def find_user_data_in_sheet(telegram_id: str):
    try:
        if not sheet: return None
        sheet_data = await asyncio.to_thread(sheet.get_all_values)
        # Ищем с конца, чтобы взять самые свежие данные
        for row in reversed(sheet_data[1:]):
            if len(row) >= 4 and row[1] == telegram_id:
                return {"name": row[2], "phone": row[3]}
    except Exception as e:
        logging.error(f"Ошибка поиска данных пользователя: {e}")
    return None

# Выбор времени
@dp.callback_query(BookingState.waiting_for_time, F.data.startswith("time_"))
async def process_time(callback: CallbackQuery, state: FSMContext):
    time = callback.data.replace("time_", "")
    await state.update_data(time=time)
    
    await callback.message.edit_text("⏳ Проверяем ваши прошлые записи...")
    telegram_id = str(callback.from_user.id)
    user_data = await find_user_data_in_sheet(telegram_id)
    
    if user_data:
        # Автоматически заполняем стейт
        await state.update_data(name=user_data["name"], phone=user_data["phone"])
        await state.set_state(BookingState.waiting_for_comment)
        await callback.message.edit_text(
            f"Я вас узнал, {user_data['name']}! 😊\n"
            f"Ваш номер телефона: {user_data['phone']}\n\n"
            "Есть ли какие-то дополнительные пожелания/комментарии?\n"
            "(Если нет, напишите 'нет'):"
        )
    else:
        await state.set_state(BookingState.waiting_for_name)
        await callback.message.edit_text("Как к вам обращаться?\n(Напишите ваше имя)")

# Ввод имени
@dp.message(BookingState.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    await state.set_state(BookingState.waiting_for_phone)
    await message.answer("Пожалуйста, введите ваш номер телефона:")

# Ввод телефона
@dp.message(BookingState.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await state.set_state(BookingState.waiting_for_comment)
    await message.answer("Есть ли какие-то дополнительные пожелания/комментарии?\n(Если нет, напишите 'нет'):")

# Ввод комментария и финал (Итерация 1: черновая запись)
@dp.message(BookingState.waiting_for_comment)
async def process_comment(message: Message, state: FSMContext):
    comment = message.text
    data = await state.get_data()
    # 1. Сохранение в Google Sheets
    try:
        if sheet:
            booking_id = str(uuid.uuid4())[:8].upper()
            created_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            telegram_id = message.from_user.id
            
            # Данные для столбцов по PRD
            row = [
                booking_id,             # ID
                str(telegram_id),       # Telegram ID
                data['name'],           # Имя клиента
                data['phone'],          # Телефон
                data['service'].title(),# Услуга
                data['master'],         # Мастер
                data['date'],           # Дата
                data['time'],           # Время
                str(data.get('duration', 60)), # Длительность
                comment,                # Комментарий
                "Active",               # Статус
                created_at              # Created_at
            ]
            
            # Запускаем синхронный gspread метод в фоновом потоке asyncio, 
            # чтобы бот не "подвисал" ожидая ответа от Google
            await asyncio.to_thread(sheet.append_row, row)
            logging.info(f"Запись {booking_id} добавлена в таблицу!")
            
            # Добавляем напоминания
            schedule_reminders_for_booking(
                booking_id, 
                telegram_id, 
                data['date'], 
                data['time'], 
                data['service'].title(), 
                data['master']
            )
        else:
            logging.error("Не удалось сохранить: нет подключения к таблице.")
    except Exception as e:
        logging.error(f"Ошибка записи в таблицу: {e}")
    # 2. Уведомляем клиента
    text = (
        "✅ Вы успешно записаны!\n\n"
        f"Услуга: {data['service'].title()}\n"
        f"Мастер: {data['master']}\n"
        f"Дата: {data['date']} в {data['time']}\n"
        f"Комментарий: {comment}\n\n"
        "Ждем вас в BeautyLab Studio!"
    )
    await message.answer(text, reply_markup=get_main_keyboard())
    
    # 3. Уведомляем админа
    if ADMIN_CHAT_ID:
        admin_text = (
            "🔔 Новая заявка на запись!\n"
            f"👤 Имя: {data['name']} (@{message.from_user.username})\n"
            f"📞 Тел: {data['phone']}\n"
            f"✂️ Услуга: {data['service'].title()}\n"
            f"👨‍🎨 Мастер: {data['master']}\n"
            f"📝 Комментарий: {comment}\n"
        )
        try:
            await bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_text)
        except Exception as e:
            logging.error(f"Не удалось отправить уведомление админу: {e}")

    await state.clear()


@dp.message(F.text == "Мои записи 📅")
async def show_my_bookings(message: Message):
    wait_msg = await message.answer("⏳ Смотрю ваши записи...")
    try:
        if not sheet:
            await wait_msg.edit_text("Ошибка подключения к базе данных.")
            return
        
        # Получаем все данные из таблицы
        sheet_data = await asyncio.to_thread(sheet.get_all_values)
        user_id = str(message.from_user.id)
        
        active_bookings = []
        for row in sheet_data[1:]:  # пропускаем первую строку с заголовками
            # Проверяем, что колонка Telegram ID совпадает с юзером, а статус "Active"
            if len(row) >= 11 and row[1] == user_id and row[10] == "Active":
                active_bookings.append({
                    "id": row[0],
                    "service": row[4],
                    "master": row[5],
                    "date": row[6],
                    "time": row[7]
                })
                
        if not active_bookings:
            await wait_msg.edit_text("У вас пока нет активных записей 🤷‍♀️")
            return
            
        # Генерируем кнопки для каждой активной записи
        keyboard = []
        for b in active_bookings:
            btn_text = f"❌ Отменить: {b['service']} ({b['date']} в {b['time']})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"cancel_{b['id']}")])
        
        keyboard.append([InlineKeyboardButton(text="◀️ Скрыть", callback_data="hide_bookings")])
        
        await wait_msg.edit_text(
            "Ваши активные записи:\n(Нажмите на запись, если хотите её отменить)", 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
    except Exception as e:
        logging.error(f"Ошибка получения записей: {e}")
        await wait_msg.edit_text("Произошла ошибка при поиске записей.")

# Обработка кнопки "Скрыть"
@dp.callback_query(F.data == "hide_bookings")
async def hide_bookings(callback: CallbackQuery):
    await callback.message.delete()

# Логика отмены конкретной записи
@dp.callback_query(F.data.startswith("cancel_"))
async def process_cancel_booking(callback: CallbackQuery):
    booking_id = callback.data.replace("cancel_", "")
    await callback.message.edit_text("⏳ Обработка отмены...")
    
    try:
        if not sheet:
            await callback.message.edit_text("Ошибка базы данных.")
            return
            
        sheet_data = await asyncio.to_thread(sheet.get_all_values)
        
        row_index = -1
        booking_date_str = ""
        booking_time_str = ""
        booking_service = ""
        booking_master = ""
        
        # Ищем строку с этим ID. Индексы в gspread начинаются с 1 (1 - это заголовки)
        for i, row in enumerate(sheet_data):
            if len(row) >= 11 and row[0] == booking_id:
                row_index = i + 1
                booking_service = row[4]
                booking_master = row[5]
                booking_date_str = row[6]
                booking_time_str = row[7]
                break
                
        if row_index == -1:
            await callback.message.edit_text("К сожалению, запись не найдена.")
            return
        
        # --- Проверяем Правило 24 часов ---
        full_time_str = f"{booking_date_str} {booking_time_str}"
        booking_dt = datetime.strptime(full_time_str, "%d.%m.%Y %H:%M")
        
        time_left = booking_dt - datetime.now()
        
        if time_left.total_seconds() < 24 * 3600:
            # Меньше 24 часов - запрещаем отменять через бота
            await callback.message.edit_text(
                "⚠️ **Отмена невозможна**\n\n"
                "До вашего визита осталось менее 24 часов. "
                "Автоматическая отмена через бота уже недоступна.\n\n"
                "Пожалуйста, свяжитесь с нашим администратором для отмены или переноса записи.",
                parse_mode="Markdown"
            )
        else:
            # Больше 24 часов - отменяем! Удаляем строку полностью
            await asyncio.to_thread(sheet.delete_rows, row_index)
            
            # Удаляем запланированные напоминания, чтобы не тревожить человека сообщениями
            cancel_reminders(booking_id)
            
            await callback.message.edit_text(
                f"✅ Ваша запись на **{booking_date_str} в {booking_time_str}** успешно отменена!", 
                parse_mode="Markdown"
            )
            
            # Уведомляем администратора об отмене
            if ADMIN_CHAT_ID:
                admin_text = (
                    "⚠️ Клиент отменил запись!\n"
                    f"✂️ Услуга: {booking_service}\n"
                    f"👨‍🎨 Мастер: {booking_master}\n"
                    f"🗓 Дата: {booking_date_str} в {booking_time_str}"
                )
                try:
                    await bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_text)
                except:
                    pass
                    
    except Exception as e:
        logging.error(f"Ошибка при отмене: {e}")
        await callback.message.edit_text("Не удалось отменить запись из-за ошибки сети.")


async def health_check(request):
    return web.Response(text="Bot is running!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"🌐 Web server started on port {port} (for Render health check)")

async def main():
    if not BOT_TOKEN:
        logging.error("🚨 ВНИМАНИЕ: Не указан BOT_TOKEN в файле .env!")
        return
        
    logging.info("🤖 Запуск BeautyLab Bot...")
    
    # 0. Запускаем фоновый веб-сервер для того чтобы бесплатные хостинги (Render/Railway) не убивали процесс
    await start_web_server()
    
    # 1. Восстанавливаем сохраненные напоминания из базы
    await restore_reminders_from_sheet()
    
    # 2. Запускаем планировщик
    scheduler.start()
    
    # 3. Запускаем бота
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
