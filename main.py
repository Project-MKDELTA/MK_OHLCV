import os
import asyncio
import logging
import time
import aiohttp
import matplotlib
import json
import base64
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import yaml
from datetime import datetime, timedelta, timezone
import uuid
import re
from functools import wraps

from aiogram import Bot, Dispatcher, types
from aiogram import F
from aiogram.types import FSInputFile
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest, TelegramAPIError # Для обработки ошибок отправки
from aiogram.utils.markdown import hbold, hcode, hitalic, hlink # Импортируем хелперы разметки

import description # Авто апдейт курса бтс и етх

# -------------------- Настройки и логирование --------------------
LOGS_DIR = "logs" # Если хочеться, поменяй
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)
    print(f"Создана директория для логов: {LOGS_DIR}")

log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
log_filepath = os.path.join(LOGS_DIR, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_filepath, encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)
logger.info("--- Запуск бота ---")

# -------------------- Утилита для MarkdownV2 эскейпинга --------------------
def escape_markdown_v2(text: str) -> str:
    """Экранирует специальные символы для MarkdownV2."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    # Экранируем каждый символ добавлением обратного слеша перед ним
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

# -------------------- Whitelist --------------------
WHITELIST_FILE = "whitelist.yaml" # тоже можно сменить
WHITELIST_ENABLED = True # форсированный вайтлист, сбрасываеться на значение True после каждого перезапуска, по необходимости отключаеться в админ панели.
ADMIN_ID = 123456789 # Смени на свой ID

def load_whitelist():
    start_time = time.time()
    logger.info("Загрузка whitelist...")
    try:
        if not os.path.exists(WHITELIST_FILE):
            logger.warning(f"Файл {WHITELIST_FILE} не найден. Создание нового с администратором.")
            whitelist = [{"id": ADMIN_ID, "username": "@admin_username"}] # замени на свой
            with open(WHITELIST_FILE, "w", encoding="utf-8") as f:
                yaml.dump(whitelist, f, allow_unicode=True)
            logger.info(f"Whitelist создан. Добавлен админ ID: {ADMIN_ID}. Время: {time.time() - start_time:.4f} сек.")
            return whitelist
        with open(WHITELIST_FILE, "r", encoding="utf-8") as f:
            whitelist = yaml.safe_load(f) or []
            if not any(user["id"] == ADMIN_ID for user in whitelist):
                 logger.warning(f"Админ ID {ADMIN_ID} не найден в whitelist. Добавляем.")
                 whitelist.append({"id": ADMIN_ID, "username": "@admin_username"}) # и тут я, замени
                 save_whitelist(whitelist)

            logger.info(f"Whitelist загружен ({len(whitelist)} пользователей). Время: {time.time() - start_time:.4f} сек.")
            return whitelist
    except Exception as e:
        logger.error(f"Ошибка загрузки whitelist из {WHITELIST_FILE}: {e}. Время: {time.time() - start_time:.4f} сек.", exc_info=True)
        # Замени админа
        return [{"id": ADMIN_ID, "username": "@admin_username"}]

def save_whitelist(whitelist):
    start_time = time.time()
    logger.info("Сохранение whitelist...")
    try:
        with open(WHITELIST_FILE, "w", encoding="utf-8") as f:
            yaml.dump(whitelist, f, allow_unicode=True)
        logger.info(f"Whitelist сохранен ({len(whitelist)} пользователей). Время: {time.time() - start_time:.4f} сек.")
    except Exception as e:
        logger.error(f"Ошибка сохранения whitelist в {WHITELIST_FILE}: {e}. Время: {time.time() - start_time:.4f} сек.")

def is_whitelisted(user_id: int) -> bool:
    start_time = time.time()
    if user_id == ADMIN_ID:
         return True
    if not WHITELIST_ENABLED:
        return True
    whitelist = load_whitelist()
    whitelisted = any(user["id"] == user_id for user in whitelist)
    logger.debug(f"Результат проверки whitelist для {user_id}: {whitelisted}. Время: {time.time() - start_time:.4f} сек.")
    return whitelisted

def add_to_whitelist(user_id: int, username: str):
    start_time = time.time()
    logger.info(f"Попытка добавления в whitelist: id={user_id}, username={username}")
    # Добавляем '@' если его нет и username не похож на ID_xxx
    safe_username = username if username.startswith('@') or username.startswith('ID_') else f"@{username}"

    whitelist = load_whitelist()
    if not any(user["id"] == user_id for user in whitelist):
        whitelist.append({"id": user_id, "username": safe_username})
        save_whitelist(whitelist)
        logger.info(f"УСПЕШНО: Добавлен в whitelist: {safe_username} ({user_id}). Время: {time.time() - start_time:.4f} сек.")
    else:
        updated = False
        for user in whitelist:
            if user["id"] == user_id and user.get("username") != safe_username:
                user["username"] = safe_username
                updated = True
                break
        if updated:
             save_whitelist(whitelist)
             logger.info(f"Обновлен username в whitelist для {user_id} на {safe_username}. Время: {time.time() - start_time:.4f} сек.")
        else:
             logger.info(f"Пользователь {safe_username} ({user_id}) уже в whitelist. Время: {time.time() - start_time:.4f} сек.")

def remove_from_whitelist(identifier: str) -> bool:
    start_time = time.time()
    logger.info(f"Попытка удаления из whitelist: идентификатор={identifier}")
    whitelist = load_whitelist()
    initial_len = len(whitelist)
    clean_identifier = identifier.strip()
    is_username = clean_identifier.startswith('@')

    if is_username:
        new_whitelist = [user for user in whitelist if user.get("username") != clean_identifier]
    else:
         try:
             target_id = int(clean_identifier)
             new_whitelist = [user for user in whitelist if user["id"] != target_id]
         except ValueError:
              logger.warning(f"Неверный формат идентификатора для удаления из WL (не ID и не @username): {identifier}")
              return False

    if len(new_whitelist) < initial_len:
        removed_user = next((user for user in whitelist if (is_username and user.get("username") == clean_identifier) or (not is_username and user["id"] == target_id)), None)
        save_whitelist(new_whitelist)
        if removed_user:
             logger.info(f"УСПЕШНО: Удален из whitelist: {removed_user.get('username','N/A')} ({removed_user['id']}). Идентификатор: {identifier}. Время: {time.time() - start_time:.4f} сек.")
        else:
             logger.info(f"УСПЕШНО: Пользователь с идентификатором {identifier} удален из whitelist. Время: {time.time() - start_time:.4f} сек.")
        return True
    else:
        logger.warning(f"НЕУДАЧА: Пользователь с идентификатором {identifier} не найден в whitelist. Время: {time.time() - start_time:.4f} сек.")
        return False

# -------------------- Banlist --------------------
BANLIST_FILE = "banlist.yaml" # так же сменить по желанию

def load_banlist():
    start_time = time.time()
    logger.info("Загрузка banlist...")
    try:
        if not os.path.exists(BANLIST_FILE):
            logger.warning(f"Файл {BANLIST_FILE} не найден. Создание пустого.")
            with open(BANLIST_FILE, "w", encoding="utf-8") as f: yaml.dump([], f)
            logger.info(f"Banlist создан. Время: {time.time() - start_time:.4f} сек.")
            return []
        with open(BANLIST_FILE, "r", encoding="utf-8") as f:
            banlist = yaml.safe_load(f) or []
            logger.info(f"Banlist загружен ({len(banlist)} пользователей). Время: {time.time() - start_time:.4f} сек.")
            return banlist
    except Exception as e:
        logger.error(f"Ошибка загрузки banlist из {BANLIST_FILE}: {e}. Время: {time.time() - start_time:.4f} сек.", exc_info=True)
        return []

def save_banlist(banlist):
    start_time = time.time()
    logger.info("Сохранение banlist...")
    try:
        with open(BANLIST_FILE, "w", encoding="utf-8") as f:
            yaml.dump(banlist, f, allow_unicode=True)
        logger.info(f"Banlist сохранен ({len(banlist)} пользователей). Время: {time.time() - start_time:.4f} сек.")
    except Exception as e:
        logger.error(f"Ошибка сохранения banlist в {BANLIST_FILE}: {e}. Время: {time.time() - start_time:.4f} сек.")

def is_banned(user_id: int) -> bool:
    start_time = time.time()
    if user_id == ADMIN_ID:
        return False
    banlist = load_banlist()
    banned = any(user["id"] == user_id for user in banlist)
    logger.debug(f"Результат проверки banlist для {user_id}: {banned}. Время: {time.time() - start_time:.4f} сек.")
    return banned

def ban_user(user_id: int, username: str):
    start_time = time.time()
    logger.info(f"Попытка бана пользователя: id={user_id}, username={username}")
    if user_id == ADMIN_ID:
        logger.warning(f"НЕУДАЧА: Попытка забанить администратора ({user_id}). Время: {time.time() - start_time:.4f} сек.")
        return False

    safe_username = username if username.startswith('@') or username.startswith('ID_') else f"@{username}"
    banlist = load_banlist()
    if not any(user["id"] == user_id for user in banlist):
        banlist.append({"id": user_id, "username": safe_username})
        save_banlist(banlist)
        logger.info(f"УСПЕШНО: Забанен пользователь: {safe_username} ({user_id}). Время: {time.time() - start_time:.4f} сек.")
        return True
    else:
        updated = False
        for user in banlist:
            if user["id"] == user_id and user.get("username") != safe_username:
                user["username"] = safe_username
                updated = True
                break
        if updated:
             save_banlist(banlist)
             logger.info(f"Обновлен username в banlist для {user_id} на {safe_username}. Время: {time.time() - start_time:.4f} сек.")
        else:
             logger.warning(f"НЕУДАЧА: Пользователь {safe_username} ({user_id}) уже забанен. Время: {time.time() - start_time:.4f} сек.")
        return False

def unban_user(user_id: int) -> bool:
    start_time = time.time()
    logger.info(f"Попытка разбана пользователя: id={user_id}")
    banlist = load_banlist()
    initial_len = len(banlist)
    new_banlist = [user for user in banlist if user["id"] != user_id]

    if len(new_banlist) < initial_len:
        removed_user = next((user for user in banlist if user["id"] == user_id), None)
        save_banlist(new_banlist)
        if removed_user:
            logger.info(f"УСПЕШНО: Разбанен пользователь: {removed_user.get('username', 'N/A')} ({user_id}). Время: {time.time() - start_time:.4f} сек.")
        else:
             logger.info(f"УСПЕШНО: Пользователь с ID {user_id} разбанен. Время: {time.time() - start_time:.4f} сек.")
        return True
    else:
        logger.warning(f"НЕУДАЧА: Пользователь с ID {user_id} не найден в banlist. Время: {time.time() - start_time:.4f} сек.")
        return False

# -------------------- Реферальные ссылки --------------------
REFERRALS_FILE = "referrals.yaml"

def load_referrals():
    start_time = time.time()
    logger.info("Загрузка реферальных ссылок...")
    try:
        if not os.path.exists(REFERRALS_FILE):
            logger.warning(f"Файл {REFERRALS_FILE} не найден. Создание пустого.")
            with open(REFERRALS_FILE, "w", encoding="utf-8") as f: yaml.dump([], f)
            logger.info(f"Файл рефералов создан. Время: {time.time() - start_time:.4f} сек.")
            return []
        with open(REFERRALS_FILE, "r", encoding="utf-8") as f:
            referrals = yaml.safe_load(f) or []
            logger.info(f"Реферальные ссылки загружены ({len(referrals)} шт.). Время: {time.time() - start_time:.4f} сек.")
            return referrals
    except Exception as e:
        logger.error(f"Ошибка загрузки рефералов из {REFERRALS_FILE}: {e}. Время: {time.time() - start_time:.4f} сек.", exc_info=True)
        return []

def save_referrals(referrals):
    start_time = time.time()
    logger.info("Сохранение реферальных ссылок...")
    try:
        with open(REFERRALS_FILE, "w", encoding="utf-8") as f:
            yaml.dump(referrals, f, allow_unicode=True)
        logger.info(f"Реферальные ссылки сохранены ({len(referrals)} шт.). Время: {time.time() - start_time:.4f} сек.")
    except Exception as e:
        logger.error(f"Ошибка сохранения рефералов в {REFERRALS_FILE}: {e}. Время: {time.time() - start_time:.4f} сек.")

def generate_referral_link(activations: int, expire_time_str: str, bot_username: str) -> tuple[str | None, str | None]:
    """Генерирует реферальную ссылку. Возвращает (ссылка, код) или (None, None) в случае ошибки."""
    start_time = time.time()
    logger.info(f"Генерация реф. ссылки: активаций={activations}, срок={expire_time_str}")
    code = str(uuid.uuid4())[:8]
    referrals = load_referrals()

    expiration_timestamp = None
    if expire_time_str and expire_time_str != "0":
        try:
            match = re.fullmatch(r"(\d+)([mhd])$", expire_time_str.lower()) # Используем fullmatch
            if match:
                value = int(match.group(1))
                unit = match.group(2)
                if value <= 0:
                     logger.warning(f"Некорректное значение времени '{value}' в '{expire_time_str}'. Ссылка будет бессрочной.")
                else:
                    current_time = time.time()
                    if unit == 'm': expiration_timestamp = current_time + value * 60
                    elif unit == 'h': expiration_timestamp = current_time + value * 3600
                    elif unit == 'd': expiration_timestamp = current_time + value * 86400
            else:
                 logger.warning(f"Неверный формат времени истечения '{expire_time_str}'. Ссылка будет бессрочной.")
        except Exception as e:
            logger.error(f"Ошибка парсинга времени истечения '{expire_time_str}': {e}. Ссылка будет бессрочной.", exc_info=True)

    referrals.append({
        "code": code,
        "activations_left": activations,
        "expiration": expiration_timestamp
    })
    save_referrals(referrals)
    link = f"https://t.me/{bot_username}?start={code}"
    logger.info(f"УСПЕШНО: Реф. ссылка создана: {link} (код: {code}). Время: {time.time() - start_time:.4f} сек.")
    return link, code

def activate_referral(code: str, user_id: int, username: str) -> bool:
    start_time = time.time()
    logger.info(f"Попытка активации реф. ссылки: код={code}, user_id={user_id}, username={username}")
    current_time = time.time()
    referrals = load_referrals()
    updated_referrals = []
    activated = False
    link_found = False

    for referral in referrals:
        if referral["code"] == code:
            link_found = True
            # Проверяем срок годности
            expiration = referral.get("expiration")
            if expiration and expiration < current_time:
                logger.warning(f"Реф. ссылка {code} истекла ({datetime.fromtimestamp(expiration).strftime('%Y-%m-%d %H:%M:%S')}). Не активирована.")
                # Не добавляем истекшую ссылку в updated_referrals (она будет удалена)
                continue # Переходим к следующей, вдруг дубликат кода

            # Проверяем активации
            if referral["activations_left"] > 0:
                logger.info(f"Активация реф. ссылки {code} для {username} ({user_id}). Осталось активаций: {referral['activations_left'] - 1}")
                add_to_whitelist(user_id, username)
                referral["activations_left"] -= 1
                activated = True

                # Добавляем обновленную ссылку в список, если активации еще остались
                if referral["activations_left"] > 0:
                    updated_referrals.append(referral)
                else:
                    logger.info(f"У реф. ссылки {code} закончились активации. Удаляем.")
                # Не прерываем цикл, если вдруг есть дубликаты кодов (маловероятно)
            else:
                 logger.warning(f"У реф. ссылки {code} не осталось активаций. Не активирована.")
                 updated_referrals.append(referral) # Сохраняем ссылку с 0 активаций
        else:
             updated_referrals.append(referral) # Добавляем другие ссылки в новый список

    if not link_found:
        logger.warning(f"НЕУДАЧА: Реф. ссылка с кодом {code} не найдена.")

    # Сохраняем обновленный список (с удаленными истекшими/использованными ссылками)
    if len(updated_referrals) != len(referrals): # Проверяем, изменился ли список
         save_referrals(updated_referrals)

    if activated:
         logger.info(f"УСПЕШНО: Реф. ссылка {code} активирована для {username} ({user_id}). Время: {time.time() - start_time:.4f} сек.")
    else:
         logger.info(f"НЕУДАЧА: Реф. ссылка {code} не активирована для {username} ({user_id}). Время: {time.time() - start_time:.4f} сек.")

    return activated

def get_active_referrals() -> list[str]:
    start_time = time.time()
    logger.info("Получение списка активных реферальных ссылок...")
    current_time = time.time()
    referrals = load_referrals()
    active_refs_info = []
    updated_referrals = []
    needs_saving = False

    for ref in referrals:
        expiration = ref.get("expiration")
        if expiration and expiration < current_time:
            logger.info(f"Реф. ссылка {ref['code']} истекла и будет удалена из списка.")
            needs_saving = True
            continue # Пропускаем истекшую

        if ref.get("activations_left", 0) <= 0:
             logger.info(f"Реф. ссылка {ref['code']} не имеет активаций и будет удалена.")
             needs_saving = True
             continue # Пропускаем без активаций

        # Если ссылка активна и валидна, добавляем ее в список для сохранения
        updated_referrals.append(ref)

        # Формируем информацию для вывода
        expiry_info = "бессрочная"
        if expiration:
            try:
                expiry_dt = datetime.fromtimestamp(expiration)
                expiry_info = f"до {escape_markdown_v2(expiry_dt.strftime('%d.%m.%Y %H:%M'))}" # Экранируем дату/время
            except Exception as e:
                logger.error(f"Ошибка форматирования времени истечения для ссылки {ref['code']}: {e}")
                expiry_info = f"до timestamp {escape_markdown_v2(str(expiration))}"

        ref_info = f"Код: {hcode(ref['code'])}, Активаций: {ref['activations_left']}, {expiry_info}"
        active_refs_info.append(ref_info)

    if needs_saving:
        logger.info("Сохранение списка рефералов после удаления истекших/использованных.")
        save_referrals(updated_referrals)

    logger.info(f"Найдено активных реф. ссылок: {len(active_refs_info)}. Время: {time.time() - start_time:.4f} сек.")
    return active_refs_info

def deactivate_referral(code: str) -> bool:
    start_time = time.time()
    logger.info(f"Попытка деактивации реф. ссылки: код={code}")
    referrals = load_referrals()
    initial_len = len(referrals)
    new_referrals = [ref for ref in referrals if ref.get("code") != code]

    if len(new_referrals) < initial_len:
        save_referrals(new_referrals)
        logger.info(f"УСПЕШНО: Реф. ссылка с кодом {code} деактивирована (удалена). Время: {time.time() - start_time:.4f} сек.")
        return True
    else:
        logger.warning(f"НЕУДАЧА: Реф. ссылка с кодом {code} не найдена для деактивации. Время: {time.time() - start_time:.4f} сек.")
        return False

# -------------------- Конфигурация API и WebApp--------------------
API_TOKEN = '' # Токен телеграм бота
API_BASE_URL = '' # Урл АПИ
API_AUTH_HEADER = {""} # Ключ АПИ

# -------------------- Отголоски WebApp--------------------
# WEBAPP_URL = "" # <-- ЗАМЕНИТЬ!
# if WEBAPP_URL == "":
#     logger.warning("!!!!!!!!!! WEBAPP_URL не настроен в main.py! !!!!!!!!!")
#
# Пока что в процессе написания, но честно уже сомневаюсь в том, выпущю я апдейт с вебапп или нет... хз.

# -------------------- Инициализация бота и FSM --------------------
storage = MemoryStorage()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=storage)

# -------------------- Декоратор для логирования времени выполнения --------------------
def log_execution_time(is_api_call=False):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            func_name = func.__name__
            logger.info(f"Начало выполнения {func_name}.")

            result = None
            success = False
            try:
                result = await func(*args, **kwargs)
                success = True
                if is_api_call and result is not None:
                    logger.info(f"УСПЕШНО: API вызов {func_name} завершен. Время: {time.time() - start_time:.4f} сек.")
                elif not is_api_call:
                     logger.info(f"УСПЕШНО: Функция {func_name} завершена. Время: {time.time() - start_time:.4f} сек.")
            except TelegramAPIError as e: # Ловим ошибки API отдельно
                logger.error(f"ОШИБКА Telegram API в {func_name}: {e}. Время: {time.time() - start_time:.4f} сек.", exc_info=True)
            except Exception as e:
                logger.error(f"ОШИБКА: Исключение в {func_name}: {e}. Время: {time.time() - start_time:.4f} сек.", exc_info=True)
            return result
        return wrapper
    return decorator


# -------------------- Авторизация API (проверка при старте) --------------------
@log_execution_time()
async def check_api_auth():
    """Проверяет доступность API и авторизацию при старте."""
    logger.info("Проверка авторизации API...")
    test_url = f"{API_BASE_URL}/candles/latest/btcusdt/1"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(test_url, headers=API_AUTH_HEADER, timeout=10) as response:
                if response.status == 200:
                    logger.info(f"УСПЕШНО: API авторизация и доступ подтверждены. Статус: {response.status}.")
                    return True
                elif response.status in [401, 403]:
                     logger.error(f"ОШИБКА АВТОРИЗАЦИИ API: Неверный API ключ? Статус: {response.status}.")
                     return False
                else:
                    logger.error(f"ОШИБКА ДОСТУПА К API: Сервер ответил со статусом {response.status}. URL: {test_url}.")
                    return False
    except aiohttp.ClientConnectorError as e:
         logger.error(f"ОШИБКА СОЕДИНЕНИЯ С API: Не удалось подключиться к {API_BASE_URL}. Ошибка: {e}.")
         return False
    except asyncio.TimeoutError:
         logger.error(f"ОШИБКА API: Превышен таймаут ожидания ответа от {test_url}.")
         return False
    except Exception as e:
        logger.error(f"НЕИЗВЕСТНАЯ ОШИБКА при проверке API: {e}.", exc_info=True)
        return False

# -------------------- Асинхронные функции для запросов к API --------------------

api_call_logger = log_execution_time(is_api_call=True)

@api_call_logger
async def get_candles(symbol: str, timeframe: str, limit: int = 1000, start_ts: int | None = None, end_ts: int | None = None) -> list[dict] | None:
    url = f"{API_BASE_URL}/candles/{symbol.lower()}/{timeframe}"
    params = {"limit": limit}
    if start_ts is not None: params["start_ts"] = start_ts
    if end_ts is not None: params["end_ts"] = end_ts
    param_str = '&'.join([f"{k}={v}" for k, v in params.items()])
    logger.info(f"Запрос API: {url}?{param_str}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=API_AUTH_HEADER, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                         logger.info(f"API вернуло {len(data)} свечей для {symbol}/{timeframe}.")
                         return data
                    else:
                        logger.error(f"API вернуло не список для {symbol}/{timeframe}. Тип: {type(data)}. Ответ: {data}")
                        return None
                elif response.status == 422:
                     error_details = await response.json()
                     logger.error(f"Ошибка валидации данных API (422) для {symbol}/{timeframe}. Параметры: {params}. Детали: {error_details}")
                     return None
                else:
                    logger.error(f"Ошибка API при запросе свечей {symbol}/{timeframe}. Статус: {response.status}. Параметры: {params}. Ответ: {await response.text()}")
                    return None
    except aiohttp.ClientConnectorError as e:
         logger.error(f"Ошибка соединения с API при запросе {symbol}/{timeframe}: {e}")
         return None
    except asyncio.TimeoutError:
         logger.error(f"Таймаут API при запросе свечей {symbol}/{timeframe}. Параметры: {params}")
         return None
    except Exception as e:
        logger.error(f"Исключение при запросе свечей {symbol}/{timeframe}: {e}", exc_info=True)
        return None

@api_call_logger
async def get_close_prices(symbol: str, timeframe: str, limit: int = 1000, start_ts: int | None = None, end_ts: int | None = None) -> list[dict] | None:
    url = f"{API_BASE_URL}/candles/close/{symbol.lower()}/{timeframe}"
    params = {"limit": limit}
    if start_ts is not None: params["start_ts"] = start_ts
    if end_ts is not None: params["end_ts"] = end_ts
    param_str = '&'.join([f"{k}={v}" for k, v in params.items()])
    logger.info(f"Запрос API (close): {url}?{param_str}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=API_AUTH_HEADER, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                        logger.info(f"API вернуло {len(data)} цен закрытия для {symbol}/{timeframe}.")
                        return data
                    else:
                        logger.error(f"API вернуло не список для close {symbol}/{timeframe}. Тип: {type(data)}. Ответ: {data}")
                        return None
                elif response.status == 422:
                     error_details = await response.json()
                     logger.error(f"Ошибка валидации данных API (422) для close {symbol}/{timeframe}. Параметры: {params}. Детали: {error_details}")
                     return None
                else:
                    logger.error(f"Ошибка API при запросе цен закрытия {symbol}/{timeframe}. Статус: {response.status}. Параметры: {params}. Ответ: {await response.text()}")
                    return None
    except aiohttp.ClientConnectorError as e:
         logger.error(f"Ошибка соединения с API при запросе close {symbol}/{timeframe}: {e}")
         return None
    except asyncio.TimeoutError:
         logger.error(f"Таймаут API при запросе цен закрытия {symbol}/{timeframe}. Параметры: {params}")
         return None
    except Exception as e:
        logger.error(f"Исключение при запросе цен закрытия {symbol}/{timeframe}: {e}", exc_info=True)
        return None

@api_call_logger
async def get_latest_candle(symbol: str, timeframe: str) -> dict | None:
    url = f"{API_BASE_URL}/candles/latest/{symbol.lower()}/{timeframe}"
    logger.info(f"Запрос API (latest): {url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=API_AUTH_HEADER, timeout=15) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and isinstance(data, dict):
                        logger.info(f"API вернуло последнюю свечу для {symbol}/{timeframe}.")
                        required_keys = ["timestamp", "open", "high", "low", "close", "volume"]
                        if not all(k in data for k in required_keys):
                             logger.warning(f"Последняя свеча для {symbol}/{timeframe} не содержит всех ключей: {data}. Заполняем нулями.")
                             data.setdefault("timestamp", int(time.time() * 1000))
                             data.setdefault("open", 0); data.setdefault("high", 0); data.setdefault("low", 0)
                             data.setdefault("close", 0); data.setdefault("volume", 0)
                        return data
                    elif data is None:
                         logger.warning(f"API вернуло null для последней свечи {symbol}/{timeframe}.")
                         return None
                    else:
                         logger.error(f"API вернуло неожиданный тип для latest {symbol}/{timeframe}. Тип: {type(data)}. Ответ: {data}")
                         return None
                elif response.status == 422:
                     error_details = await response.json()
                     logger.error(f"Ошибка валидации данных API (422) для latest {symbol}/{timeframe}. Детали: {error_details}")
                     return None
                else:
                    logger.error(f"Ошибка API при запросе последней свечи {symbol}/{timeframe}. Статус: {response.status}. Ответ: {await response.text()}")
                    return None
    except aiohttp.ClientConnectorError as e:
         logger.error(f"Ошибка соединения с API при запросе latest {symbol}/{timeframe}: {e}")
         return None
    except asyncio.TimeoutError:
         logger.error(f"Таймаут API при запросе последней свечи {symbol}/{timeframe}.")
         return None
    except Exception as e:
        logger.error(f"Исключение при запросе последней свечи {symbol}/{timeframe}: {e}", exc_info=True)
        return None

# -------------------- Список криптовалют --------------------
# В апи не было эндпоинта для получения списка поэтому он захардкожен
CRYPTO_LIST = sorted([
# список крипты в формате: "test", "test1", "test2"
])
logger.info(f"Загружен статический список криптовалют: {len(CRYPTO_LIST)} пар.")

class CryptoListPaginator:
    def __init__(self, crypto_list, page_size=15):
        self.crypto_list = crypto_list
        self.page_size = page_size
        self.total_pages = (len(crypto_list) + page_size - 1) // page_size
        logger.debug(f"Paginator создан: {self.total_pages} страниц по {self.page_size} элементов.")

    def get_page(self, page_num):
        if page_num < 1 or page_num > self.total_pages:
            logger.warning(f"Запрошен неверный номер страницы: {page_num}. Всего страниц: {self.total_pages}")
            return []
        start_idx = (page_num - 1) * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.crypto_list))
        return self.crypto_list[start_idx:end_idx]

# -------------------- Состояния для FSM --------------------
class QueryState(StatesGroup):
    waiting_for_query = State()

class AdminState(StatesGroup):
    waiting_for_activations = State()
    waiting_for_expire_time = State()
    waiting_for_remove_id = State()
    waiting_for_ban_id = State()
    waiting_for_unban_id = State()
    waiting_for_deactivate_ref_code = State()

class QuickChartState(StatesGroup):
    select_symbol = State()
    select_timeframe = State()
    select_period_type = State()
    enter_period_time = State()

# -------------------- Декоратор проверки доступа --------------------
def access_check(handler):
    @wraps(handler)
    async def wrapper(update: types.Update | types.Message | types.CallbackQuery, *args, **kwargs):
        # Определяем пользователя и его ID
        user = None
        if isinstance(update, types.Message):
            user = update.from_user
        elif isinstance(update, types.CallbackQuery):
            user = update.from_user
        # Если это не сообщение и не колбэк, пропускаем проверку (или можно добавить другие типы)
        if not user:
             logger.warning("Не удалось определить пользователя для проверки доступа.")
             return await handler(update, *args, **kwargs) # Пропускаем

        user_id = user.id
        username = user.username or f"ID_{user_id}"

        # 1. Проверка бана
        if is_banned(user_id):
            logger.warning(f"ДОСТУП ЗАПРЕЩЕН (БАН): Пользователь {username} ({user_id}) забанен.")
            if isinstance(update, types.Message):
                await update.answer("🚫 Вы заблокированы и не можете использовать этого бота.")
            elif isinstance(update, types.CallbackQuery):
                await update.answer("🚫 Вы заблокированы.", show_alert=True)
            return

        # 2. Проверка вайтлиста
        if not is_whitelisted(user_id):
             logger.warning(f"ДОСТУП ЗАПРЕЩЕН (НЕТ В WHITELIST): Пользователь {username} ({user_id}).")
             ref_message = " Обратитесь к администратору или используйте реферальную ссылку."
             if isinstance(update, types.Message):
                 await update.answer(f"🔒 У вас нет доступа к этому боту.{ref_message}")
             elif isinstance(update, types.CallbackQuery):
                 await update.answer("🔒 Нет доступа.", show_alert=True)
             return

        # Доступ разрешен
        return await handler(update, *args, **kwargs)
    return wrapper

# -------------------- Утилита для построения графиков --------------------
@log_execution_time()
async def plot_ohlcv_chart(candles: list[dict], symbol: str, timeframe: str, limit: int | None = None, date_range: str | None = None) -> str | None:
    logger.info(f"Создание OHLCV графика для {symbol}/{timeframe}...")
    if not candles:
        logger.warning("Нет данных для построения OHLCV графика.")
        return None
    try:
        plt.style.use('seaborn-v0_8-darkgrid')
        fig, ax = plt.subplots(figsize=(12, 7))
        timestamps = [c.get("timestamp", 0) for c in candles]
        opens = [float(c.get("open", 0)) for c in candles]
        highs = [float(c.get("high", 0)) for c in candles]
        lows = [float(c.get("low", 0)) for c in candles]
        closes = [float(c.get("close", 0)) for c in candles]
        if not any(p > 0 for p in opens + highs + lows + closes):
             logger.error("Все ценовые данные нулевые, график не может быть построен.")
             plt.close(fig) # Закрываем фигуру
             return None
        candle_indices = range(len(timestamps))
        colors = ['#26a69a' if closes[i] >= opens[i] else '#ef5350' for i in candle_indices]
        ax.vlines(candle_indices, lows, highs, color='black', linewidth=0.8, alpha=0.7)
        body_heights = [abs(opens[i] - closes[i]) for i in candle_indices]
        body_bottoms = [min(opens[i], closes[i]) for i in candle_indices]
        ax.bar(candle_indices, body_heights, bottom=body_bottoms, width=0.7, color=colors)
        title = f"{symbol.upper()} - {timeframe} мин"
        if limit: title += f" (Последние {limit} свечей)"
        elif date_range: title += f" ({date_range})" # date_range уже должен быть экранирован, если нужно
        ax.set_title(title, fontsize=14)
        ax.set_ylabel("Цена (USDT)", fontsize=12)
        ax.set_xticks([])
        ax.set_xlabel(f"Свечи ({len(candles)} шт.)", fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.5)
        chart_filename = f"chart_{uuid.uuid4()}.png"
        chart_path = os.path.join(LOGS_DIR, chart_filename)
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close(fig)
        logger.info(f"OHLCV график сохранен: {chart_path}")
        return chart_path
    except Exception as e:
        logger.error(f"Ошибка при создании OHLCV графика: {e}", exc_info=True)
        if 'fig' in locals() and plt.fignum_exists(fig.number): plt.close(fig)
        return None

@log_execution_time()
async def plot_close_price_chart(close_data: list[dict], symbol: str, timeframe: str, limit: int | None = None, date_range: str | None = None) -> str | None:
    logger.info(f"Создание графика цен закрытия для {symbol}/{timeframe}...")
    if not close_data:
        logger.warning("Нет данных для построения графика цен закрытия.")
        return None
    try:
        prices = [float(item.get('close', 0)) for item in close_data]
        timestamps = [item.get('timestamp', 0) for item in close_data]
        if not prices or all(p == 0 for p in prices):
             logger.error("Нет валидных цен закрытия для построения графика.")
             return None
        plt.style.use('seaborn-v0_8-darkgrid')
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.plot(range(len(prices)), prices, linestyle='-', color='#2962ff')
        title = f"{symbol.upper()} - Цены закрытия ({timeframe} мин)"
        if limit: title += f" (Последние {limit} записей)"
        elif date_range: title += f" ({date_range})" # date_range уже должен быть экранирован
        ax.set_title(title, fontsize=14)
        ax.set_ylabel("Цена (USDT)", fontsize=12)
        ax.set_xticks([])
        ax.set_xlabel(f"Записи ({len(prices)} шт.)", fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.5)
        chart_filename = f"close_chart_{uuid.uuid4()}.png"
        chart_path = os.path.join(LOGS_DIR, chart_filename)
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close(fig)
        logger.info(f"График цен закрытия сохранен: {chart_path}")
        return chart_path
    except TypeError as e:
        logger.error(f"Ошибка типа данных при создании графика закрытий: {e}. Данные: {close_data[:5]}...", exc_info=True)
        if 'fig' in locals() and plt.fignum_exists(fig.number): plt.close(fig)
        return None
    except Exception as e:
        logger.error(f"Ошибка при создании графика цен закрытия: {e}", exc_info=True)
        if 'fig' in locals() and plt.fignum_exists(fig.number): plt.close(fig)
        return None

# -------------------- Основное меню бота --------------------
@log_execution_time()
async def show_main_menu(user_id: int, message_id: int | None = None):
    """Отображает или редактирует главное меню."""
    logger.info(f"Отображение главного меню для user_id: {user_id}")

    btc_price_raw = description.BTC_PRICE
    eth_price_raw = description.ETH_PRICE

    menu_text = (
        f"Меню {hbold('MK_OHLCV📉📈')}\n"
        f"{hbold('BTC/USDT:')} {hcode(btc_price_raw)}$\n"
        f"{hbold('ETH/USDT:')} {hcode(eth_price_raw)}$"
    )

    # WebApp URL без параметров
    # full_webapp_url = WEBAPP_URL

    keyboard = [
         [
          # types.InlineKeyboardButton(text="📱 WebApp", web_app=types.WebAppInfo(url=full_webapp_url)), отголоски WebApp
            types.InlineKeyboardButton(text="❓ FAQ", callback_data="faq")
        ],
        [
            types.InlineKeyboardButton(text="🚀 Быстрые графики", callback_data="quick_charts"),
            types.InlineKeyboardButton(text="🕯 Запрос свечей", callback_data="candles"),
        ],
        [
            types.InlineKeyboardButton(text="❌ Цены закрытия", callback_data="close"),
            types.InlineKeyboardButton(text="📓 Список пар", callback_data="crypto_list_page_1")
        ],
    ]

    if user_id == ADMIN_ID:
         keyboard.append([types.InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])

    markup = types.InlineKeyboardMarkup(inline_keyboard=keyboard)

    # --- Логика отправки/редактирования меню ---
    try:
        if message_id:
            await bot.edit_message_text(
                chat_id=user_id, message_id=message_id, text=menu_text,
                reply_markup=markup, parse_mode="HTML"
            )
            logger.info(f"Главное меню отредактировано для user_id: {user_id}, message_id: {message_id}")
        else:
            await bot.send_message(user_id, text=menu_text, reply_markup=markup, parse_mode="HTML")
            logger.info(f"Главное меню отправлено для user_id: {user_id}")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            logger.warning(f"Не удалось отредактировать меню (не изменено) для {user_id}, msg_id {message_id}.")
        elif "message to edit not found" in str(e):
            logger.warning(f"Не удалось отредактировать меню (сообщение не найдено) для {user_id}, msg_id {message_id}. Отправляем новое.")
            try: # Отправляем новое, если старое не найдено
                 await bot.send_message(user_id, text=menu_text, reply_markup=markup, parse_mode="HTML")
            except Exception as send_err:
                 logger.error(f"Ошибка отправки нового меню после ошибки 'not found': {send_err}", exc_info=True)
        else:
            logger.error(f"Ошибка Telegram BadRequest при отображении/редактировании меню для {user_id}: {e}", exc_info=True)
            # Попробуем отправить новое сообщение в случае другой ошибки редактирования
            if message_id: # Только если пытались редактировать
                try:
                    await bot.send_message(user_id, text=menu_text, reply_markup=markup, parse_mode="HTML")
                    logger.info(f"Главное меню отправлено новым сообщением после другой ошибки редактирования для {user_id}")
                except Exception as send_err:
                    logger.error(f"Ошибка отправки нового меню после другой ошибки редактирования: {send_err}", exc_info=True)
    except Exception as e:
        logger.error(f"Неизвестная ошибка при отображении/редактировании меню для {user_id}: {e}", exc_info=True)


# -------------------- Хэндлер /start --------------------
@dp.message(Command("start"))
@log_execution_time()
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    # Используем get_full_name() и экранируем его для логов, username может быть None
    user_full_name = escape_markdown_v2(message.from_user.full_name)
    username = message.from_user.username or f"ID_{user_id}" # username для функций WL/BL
    safe_username_log = escape_markdown_v2(username) # Экранированный username для логов

    logger.info(f"Получена команда /start от {user_full_name} ({safe_username_log}, {user_id}). Текст: {message.text}")
    await state.clear()

    # 1. Проверка бана
    if is_banned(user_id):
        logger.warning(f"Пользователь {safe_username_log} ({user_id}) забанен, доступ через /start запрещен.")
        await message.answer("🚫 Вы заблокированы и не можете использовать этого бота.")
        return

    # 2. Обработка реферального кода
    args = message.text.split(maxsplit=1)
    ref_code = args[1] if len(args) > 1 else None

    if ref_code:
        logger.info(f"Пользователь {safe_username_log} ({user_id}) пришел с реферальным кодом: {ref_code}")
        # Передаем username без '@' если он есть, или ID_xxx
        activation_username = message.from_user.username if message.from_user.username else f"ID_{user_id}"
        if activate_referral(ref_code, user_id, activation_username):
            await message.answer("✅ Вы успешно добавлены в список допущенных пользователей по реферальной ссылке!")
            await show_main_menu(user_id)
            return
        else:
            # Проверяем, может пользователь УЖЕ в вайтлисте после неудачной активации
            if is_whitelisted(user_id):
                 logger.info(f"Пользователь {safe_username_log} ({user_id}) уже в вайтлисте, но реф. код {ref_code} не сработал. Показываем меню.")
                 # Используем hcode для кода, чтобы избежать проблем с MarkdownV2
                 await message.answer(f"⚠️ Реферальный код {hcode(ref_code)} недействителен или истек, но вы уже есть в списке допущенных.", parse_mode="HTML")
                 await show_main_menu(user_id)
            else:
                 logger.warning(f"Пользователь {safe_username_log} ({user_id}) не в вайтлисте и реф. код {ref_code} не сработал.")
                 await message.answer(f"❌ Реферальный код {hcode(ref_code)} недействителен или истек. Доступ запрещен.", parse_mode="HTML")
            return

    # 3. Если реф. кода не было, проверяем вайтлист
    if not is_whitelisted(user_id):
         logger.warning(f"Пользователь {safe_username_log} ({user_id}) не в вайтлисте, доступ через /start запрещен.")
         await message.answer("🔒 У вас нет доступа к этому боту. Обратитесь к администратору или используйте реферальную ссылку.")
         return

    # 4. Если пользователь в вайтлисте (или админ), показываем меню
    logger.info(f"Пользователь {safe_username_log} ({user_id}) авторизован. Показываем главное меню.")
    await show_main_menu(user_id)

# -------------------- Хэндлеры для кнопок основного меню --------------------
@dp.callback_query(F.data.in_(["candles", "close"]))
@access_check
@log_execution_time()
async def process_data_request_callback(callback: types.CallbackQuery, state: FSMContext):
    action = callback.data
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Нажата кнопка '{action}' пользователем {safe_username_log} ({user_id})")

    await state.clear()
    await state.set_state(QueryState.waiting_for_query)
    await state.update_data(action=action)

    # Экранируем примеры для MarkdownV2
    example1 = escape_markdown_v2("btcusdt 5 100")
    example2 = escape_markdown_v2("ethusdt 15 10:00 20.05.2023 12:30 21.05.2023")
    timeframes_info = escape_markdown_v2("число минут (например, 1, 5, 15, 30, 60, 120, 240, D - день)")
    limit_info = escape_markdown_v2("макс. 1000 свечей (для формата 1)")
    datetime_info = escape_markdown_v2("В UTC")

    prompt_text = (
        f"Введите запрос в одном из форматов:\n\n"
        f"1\\. `{example1}`\n\n"
        f"2\\. `{example2}`\n\n"
        f"*Таймфреймы:* {timeframes_info}\\.\n"
        f"*Лимит:* {limit_info}\\.\n"
        f"*Даты/Время:* {datetime_info}\\."
    )

    try:
        # Отправляем инструкцию новым сообщением
        await callback.message.answer(prompt_text, parse_mode="MarkdownV2")
        await callback.answer() # Закрываем часики на кнопке
    except TelegramAPIError as e:
        logger.error(f"Ошибка отправки инструкции запроса {action} для {safe_username_log}: {e}")
        await callback.answer("Ошибка отображения инструкции.", show_alert=True)


@dp.callback_query(F.data == "faq")
@access_check
@log_execution_time()
async def process_faq_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Нажата кнопка 'faq' пользователем {safe_username_log} ({user_id})")

    faq_link = "https://local-seatbelt-912.notion.site/MK_OHLCV-df2cb23c05864fa1bf9a530aa61af9a0" # ссылка на мой FAQ, смени
    faq_text = f"📄 <b>FAQ и инструкции по боту:</b>\n{hlink('Открыть FAQ', faq_link)}"

    keyboard = [[types.InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_main")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=keyboard)

    try:
        await callback.message.edit_text(faq_text, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest as e:
         if "message is not modified" not in str(e):
            logger.error(f"Ошибка редактирования сообщения для FAQ у {safe_username_log} ({user_id}): {e}")
            # Если не удалось отредактировать, отправим новым сообщением (редко нужно)
            # await callback.message.answer(faq_text, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
         logger.error(f"Неизвестная ошибка при показе FAQ у {safe_username_log} ({user_id}): {e}")

    await callback.answer()

# -------------------- Обработчик ввода запроса (FSM: QueryState.waiting_for_query) --------------------
@dp.message(QueryState.waiting_for_query)
@access_check
@log_execution_time()
async def process_query_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    query_text = message.text.strip().lower()
    logger.info(f"Получен ввод для запроса от {safe_username_log} ({user_id}): '{query_text}'")

    user_data = await state.get_data()
    action = user_data.get('action')
    if not action:
        logger.error(f"Не найдено действие (action) в состоянии FSM для {safe_username_log} ({user_id}). Сброс состояния.")
        await state.clear()
        await message.answer("Произошла внутренняя ошибка. Пожалуйста, попробуйте снова из главного меню.")
        return

    symbol = None
    timeframe = None
    limit = None
    start_ts = None
    end_ts = None
    date_range_str = None # Для заголовка графика (уже без Markdown)
    date_range_caption_str = None # Для подписи (с Markdown)

    date_range_pattern = re.compile(
        r"(\w+)\s+"
        r"([\w\d]+)\s+"
        r"(\d{1,2}:\d{2})\s+"
        r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\s+"
        r"(\d{1,2}:\d{2})\s+"
        r"(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})"
    )
    limit_pattern = re.compile(r"(\w+)\s+([\w\d]+)\s+(\d+)")

    match_date = date_range_pattern.fullmatch(query_text) # Используем fullmatch
    match_limit = limit_pattern.fullmatch(query_text) # Используем fullmatch

    if match_date:
        logger.info("Обнаружен формат запроса с диапазоном дат.")
        try:
            symbol = match_date.group(1)
            timeframe = match_date.group(2)
            start_time_str = match_date.group(3)
            start_date_str = match_date.group(4).replace('/', '.').replace('-', '.')
            end_time_str = match_date.group(5)
            end_date_str = match_date.group(6).replace('/', '.').replace('-', '.')

            dt_formats = ["%d.%m.%Y", "%d.%m.%y"]
            start_dt_naive = None; end_dt_naive = None
            for fmt in dt_formats:
                try: start_dt_naive = datetime.strptime(f"{start_date_str} {start_time_str}", f"{fmt} %H:%M"); break
                except ValueError: continue
            for fmt in dt_formats:
                 try: end_dt_naive = datetime.strptime(f"{end_date_str} {end_time_str}", f"{fmt} %H:%M"); break
                 except ValueError: continue
            if not start_dt_naive or not end_dt_naive: raise ValueError("Не удалось распознать формат даты. Используйте ДД.ММ.ГГГГ или ДД.ММ.ГГ.")

            start_dt_utc = start_dt_naive.replace(tzinfo=timezone.utc)
            end_dt_utc = end_dt_naive.replace(tzinfo=timezone.utc)
            if start_dt_utc >= end_dt_utc:
                 await message.answer("❌ Ошибка: Время начала должно быть раньше времени окончания.")
                 return
            start_ts = int(start_dt_utc.timestamp() * 1000)
            end_ts = int(end_dt_utc.timestamp() * 1000)

            logger.info(f"Распарсен диапазон: {symbol}, {timeframe}, start={start_dt_utc}, end={end_dt_utc} (ts: {start_ts}, {end_ts})")
            # Формируем строки для заголовка (без markdown) и подписи (с markdown)
            date_range_str = f"{start_dt_naive.strftime('%d.%m.%y %H:%M')} - {end_dt_naive.strftime('%d.%m.%y %H:%M')} UTC"
            date_range_caption_str = escape_markdown_v2(date_range_str) # Экранируем для подписи
            limit = 1000 # API все равно может вернуть меньше, если данных нет

        except ValueError as e:
            logger.error(f"Ошибка парсинга диапазона дат '{query_text}': {e}")
            # Экранируем пример для MarkdownV2
            example = escape_markdown_v2("ethusdt 15 10:00 20.05.23 12:30 21.05.23")
            error_msg = escape_markdown_v2(str(e))
            await message.answer(f"❌ Ошибка в формате даты/времени: {error_msg}\nПопробуйте еще раз\\. Пример: `{example}`", parse_mode="MarkdownV2")
            return
        except Exception as e:
            logger.error(f"Неизвестная ошибка парсинга диапазона дат '{query_text}': {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка при обработке вашего запроса. Попробуйте еще раз.")
            return

    elif match_limit:
        logger.info("Обнаружен формат запроса с лимитом.")
        try:
            symbol = match_limit.group(1)
            timeframe = match_limit.group(2)
            limit = int(match_limit.group(3))
            if limit <= 0 or limit > 1000:
                logger.warning(f"Некорректный лимит {limit} от {safe_username_log} ({user_id}). Установлен в 1000.")
                limit = 1000
                await message.answer(f"⚠️ Лимит скорректирован до {limit} (максимум 1000).")
            logger.info(f"Распарсен лимит: {symbol}, {timeframe}, limit={limit}")
        except ValueError:
            logger.warning(f"Не удалось распознать лимит в '{query_text}'")
            example = escape_markdown_v2("btcusdt 5 100")
            await message.answer(f"❌ Ошибка в формате лимита. Лимит должен быть числом. Попробуйте еще раз\\. Пример: `{example}`", parse_mode="MarkdownV2")
            return
        except Exception as e:
            logger.error(f"Неизвестная ошибка парсинга запроса с лимитом '{query_text}': {e}", exc_info=True)
            await message.answer("❌ Произошла ошибка при обработке вашего запроса. Попробуйте еще раз.")
            return
    else:
        logger.warning(f"Нераспознанный формат запроса от {safe_username_log} ({user_id}): '{query_text}'")
        example1_esc = escape_markdown_v2("символ таймфрейм лимит")
        example2_esc = escape_markdown_v2("символ таймфрейм ЧЧ:ММ ДД.ММ.ГГ ЧЧ:ММ ДД.ММ.ГГ")
        await message.answer(
            f"❌ Неверный формат запроса\\. Используйте:\n`{example1_esc}`\nили\n`{example2_esc}`\n\nПопробуйте еще раз\\.", parse_mode="MarkdownV2"
        )
        return

    # --- Проверка символа ---
    if symbol not in CRYPTO_LIST:
        logger.warning(f"Неизвестный символ '{symbol}' от {safe_username_log} ({user_id})")
        symbol_esc = escape_markdown_v2(symbol)
        await message.answer(
            f"❌ Неизвестный символ: `{symbol_esc}`\\. Посмотрите доступные пары в меню \\(кнопка 'Список пар'\\)\\.", parse_mode="MarkdownV2"
        )
        return

    # --- Выполнение запроса к API и построение графика ---
    await bot.send_chat_action(message.chat.id, "upload_photo")
    chart_path = None
    api_data = None
    caption = "" # Инициализация подписи

    try:
        symbol_upper_esc = escape_markdown_v2(symbol.upper())
        timeframe_esc = escape_markdown_v2(timeframe)

        if action == "candles":
            logger.info(f"Запрос свечей (get_candles) для {symbol}/{timeframe}, limit={limit}, start={start_ts}, end={end_ts}")
            api_data = await get_candles(symbol, timeframe, limit=limit, start_ts=start_ts, end_ts=end_ts)
            if api_data:
                 # Передаем date_range_str (без Markdown) в функцию графика для заголовка
                 chart_path = await plot_ohlcv_chart(api_data, symbol, timeframe, limit=None if start_ts else limit, date_range=date_range_str)
                 caption = f"🕯 {symbol_upper_esc} {timeframe_esc} мин"
                 if date_range_caption_str: caption += f"\n{date_range_caption_str}" # Используем экранированную строку
                 elif limit: caption += f"\nПоследние {limit} свечей"

        elif action == "close":
            logger.info(f"Запрос цен закрытия (get_close_prices) для {symbol}/{timeframe}, limit={limit}, start={start_ts}, end={end_ts}")
            api_data = await get_close_prices(symbol, timeframe, limit=limit, start_ts=start_ts, end_ts=end_ts)
            if api_data:
                chart_path = await plot_close_price_chart(api_data, symbol, timeframe, limit=None if start_ts else limit, date_range=date_range_str)
                caption = f"❌ {symbol_upper_esc} Цены закрытия ({timeframe_esc} мин)"
                if date_range_caption_str: caption += f"\n{date_range_caption_str}"
                elif limit: caption += f"\nПоследние {limit} записей"

        # --- Отправка результата ---
        if chart_path:
            logger.info(f"Отправка графика {chart_path} пользователю {safe_username_log} ({user_id})")
            chart_file = FSInputFile(chart_path)
            try:
                await message.answer_photo(chart_file, caption=caption, parse_mode="MarkdownV2")
                logger.info(f"График успешно отправлен {safe_username_log} ({user_id})")
            except TelegramAPIError as send_error:
                logger.error(f"Ошибка отправки графика {safe_username_log} ({user_id}): {send_error}", exc_info=True)
                # Попробуем отправить без форматирования
                try:
                    await message.answer_photo(chart_file, caption=re.sub(r'\\([_*\[\]()~`>#+\-=|{}.!])', r'\1', caption)) # Убираем экранирование
                    logger.info(f"График успешно отправлен {safe_username_log} ({user_id}) без форматирования после ошибки.")
                except Exception as fallback_send_error:
                     logger.error(f"Ошибка отправки графика без форматирования {safe_username_log} ({user_id}): {fallback_send_error}")
                     await message.answer("❌ Не удалось отправить график.")
            finally:
                try: os.remove(chart_path); logger.info(f"Временный файл графика {chart_path} удален.")
                except OSError as remove_error: logger.error(f"Ошибка удаления файла графика {chart_path}: {remove_error}")
        elif api_data is not None and not chart_path:
             await message.answer("⚠️ Не удалось построить график для полученных данных.")
        else: # api_data is None
             symbol_tf_esc = escape_markdown_v2(f"{symbol.upper()}/{timeframe}")
             await message.answer(f"❌ Не удалось получить данные от API для `{symbol_tf_esc}`\\. Попробуйте позже или проверьте параметры запроса\\.", parse_mode="MarkdownV2")

    except Exception as e:
        logger.error(f"Общая ошибка при обработке запроса и отправке графика для {safe_username_log} ({user_id}): {e}", exc_info=True)
        await message.answer("❌ Произошла непредвиденная ошибка при обработке вашего запроса.")

    finally:
        await state.clear()

# -------------------- Хэндлер для списка криптовалют с пагинацией --------------------
@dp.callback_query(F.data.startswith("crypto_list_page_"))
@access_check
@log_execution_time()
async def show_crypto_list_page(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Запрос страницы списка крипты от {safe_username_log} ({user_id}): {callback.data}")

    try:
        page = int(callback.data.split("_")[-1])
    except (ValueError, IndexError):
        logger.error(f"Не удалось извлечь номер страницы из callback_data: {callback.data}")
        await callback.answer("Ошибка! Неверный формат страницы.", show_alert=True)
        return

    paginator = CryptoListPaginator(CRYPTO_LIST)
    if page < 1 or page > paginator.total_pages:
        logger.warning(f"Запрошена несуществующая страница {page} (всего {paginator.total_pages}) пользователем {safe_username_log} ({user_id})")
        await callback.answer(f"Ошибка! Страницы {page} не существует.", show_alert=True)
        return

    current_page_data = paginator.get_page(page)
    if not current_page_data:
        logger.error(f"Paginator вернул пустую страницу {page} для {safe_username_log} ({user_id})")
        await callback.answer("Ошибка! Не удалось получить данные для страницы.", show_alert=True)
        return

    nav_buttons = []
    if page > 1: nav_buttons.append(types.InlineKeyboardButton(text="⬅️ Пред.", callback_data=f"crypto_list_page_{page - 1}"))
    if page < paginator.total_pages: nav_buttons.append(types.InlineKeyboardButton(text="След. ➡️", callback_data=f"crypto_list_page_{page + 1}"))
    back_button = [types.InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_main")]
    keyboard_rows = [nav_buttons] if nav_buttons else []
    keyboard_rows.append(back_button)
    markup = types.InlineKeyboardMarkup(inline_keyboard=keyboard_rows)

    crypto_text = "\n".join([hcode(escape_markdown_v2(item)) for item in current_page_data]) # Используем hcode и экранирование

    total_pages_num = paginator.total_pages
    title = f"📖 *Список доступных пар* \\(Страница {page}/{total_pages_num}\\)\\:"

    message_text = f"{title}\n\n{crypto_text}"

    try:
        await callback.message.edit_text(
            message_text,
            reply_markup=markup,
            parse_mode="MarkdownV2"
        )
        logger.info(f"Страница {page} списка крипты показана {safe_username_log} ({user_id})")
    except TelegramBadRequest as e:
         if "message is not modified" in str(e):
             logger.warning(f"Не удалось отредактировать список крипты (не изменен) для {safe_username_log} ({user_id}).")
         else:
             # Если ошибка парсинга, попробуем отправить без Markdown
             logger.error(f"Ошибка Telegram BadRequest при редактировании списка крипты для {safe_username_log} ({user_id}): {e}", exc_info=True)
             try:
                 raw_text = f"Список доступных пар (Страница {page}/{paginator.total_pages}):\n\n" + "\n".join(current_page_data)
                 await callback.message.edit_text(raw_text, reply_markup=markup)
                 logger.info(f"Список крипты отправлен {safe_username_log} без Markdown после ошибки.")
             except Exception as fallback_e:
                  logger.error(f"Ошибка отправки списка крипты без Markdown: {fallback_e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка при отображении списка крипты для {safe_username_log} ({user_id}): {e}", exc_info=True)

    await callback.answer()


# -------------------- Возврат в главное меню --------------------
@dp.callback_query(F.data == "back_to_main")
@access_check
@log_execution_time()
async def back_to_main_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Возврат в главное меню для {safe_username_log} ({user_id})")
    await state.clear()
    # Передаем callback для возможного ответа в show_main_menu в случае ошибки редактирования
    await show_main_menu(user_id, message_id=callback.message.message_id)
    await callback.answer()


# -------------------- Быстрые графики (FSM: QuickChartState) --------------------

@dp.callback_query(F.data == "quick_charts")
@access_check
@log_execution_time()
async def quick_charts_start(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Запуск 'Быстрых графиков' для {safe_username_log} ({user_id})")
    await state.clear()
    await state.set_state(QuickChartState.select_symbol)
    keyboard = [
        [types.InlineKeyboardButton(text="BTC/USDT", callback_data="qc_symbol_btcusdt"),
         types.InlineKeyboardButton(text="ETH/USDT", callback_data="qc_symbol_ethusdt")],
        [types.InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_main")]
    ]
    markup = types.InlineKeyboardMarkup(inline_keyboard=keyboard)
    await callback.message.edit_text("🚀 <b>Быстрые графики</b>\n\nВыберите пару:", reply_markup=markup, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(QuickChartState.select_symbol, F.data.startswith("qc_symbol_"))
@access_check
@log_execution_time()
async def quick_charts_select_symbol(callback: types.CallbackQuery, state: FSMContext):
    symbol = callback.data.split("_")[-1]
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Быстрые графики: {safe_username_log} ({user_id}) выбрал символ {symbol}")
    await state.update_data(quick_chart_symbol=symbol)
    await state.set_state(QuickChartState.select_timeframe)
    timeframes = [5, 15, 30, 60, 120]
    buttons = [types.InlineKeyboardButton(text=f"{tf} мин", callback_data=f"qc_tf_{tf}") for tf in timeframes]
    keyboard = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    keyboard.append([types.InlineKeyboardButton(text="🔙 Назад (Выбор пары)", callback_data="quick_charts")])
    keyboard.append([types.InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")])
    markup = types.InlineKeyboardMarkup(inline_keyboard=keyboard)
    await callback.message.edit_text(f"Выбрана пара: <b>{symbol.upper()}</b>\n\nВыберите таймфрейм:", reply_markup=markup, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(QuickChartState.select_timeframe, F.data.startswith("qc_tf_"))
@access_check
@log_execution_time()
async def quick_charts_select_timeframe(callback: types.CallbackQuery, state: FSMContext):
    try:
        timeframe = callback.data.split("_")[-1]
        user_id = callback.from_user.id
        username = callback.from_user.username or f"ID_{user_id}"
        safe_username_log = escape_markdown_v2(username)
        logger.info(f"Быстрые графики: {safe_username_log} ({user_id}) выбрал таймфрейм {timeframe}")
        await state.update_data(quick_chart_timeframe=timeframe)
        await state.set_state(QuickChartState.select_period_type)
        user_data = await state.get_data()
        symbol = user_data.get("quick_chart_symbol", "N/A")
        keyboard = [
            [types.InlineKeyboardButton(text="📈 Последние 500 свечей", callback_data="qc_type_latest")],
            [types.InlineKeyboardButton(text="🗓 За период времени (сегодня)", callback_data="qc_type_period")],
            [types.InlineKeyboardButton(text="🔙 Назад (Таймфрейм)", callback_data=f"qc_symbol_{symbol}")],
            [types.InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ]
        markup = types.InlineKeyboardMarkup(inline_keyboard=keyboard)
        # Используем HTML
        await callback.message.edit_text(
            f"Пара: <b>{symbol.upper()}</b>, Таймфрейм: <b>{timeframe} мин</b>\n\nВыберите тип данных:",
            reply_markup=markup,
            parse_mode="HTML"
        )
        await callback.answer()
    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка извлечения таймфрейма из {callback.data}: {e}")
        await callback.answer("Произошла ошибка. Попробуйте снова.", show_alert=True)
        await state.clear()
        await show_main_menu(callback.from_user.id, callback.message.message_id)


@dp.callback_query(QuickChartState.select_period_type, F.data == "qc_type_latest")
@access_check
@log_execution_time()
async def quick_charts_process_latest(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Быстрые графики: {safe_username_log} ({user_id}) выбрал 'Последние 500 свечей'")

    user_data = await state.get_data()
    symbol = user_data.get("quick_chart_symbol")
    timeframe = user_data.get("quick_chart_timeframe")
    limit = 500
    if not symbol or not timeframe:
        logger.error(f"Отсутствуют symbol или timeframe в состоянии FSM для {safe_username_log} ({user_id}) при запросе 'latest'. Данные: {user_data}")
        await callback.answer("Произошла внутренняя ошибка. Попробуйте снова.", show_alert=True)
        await state.clear()
        await show_main_menu(user_id, callback.message.message_id)
        return

    await callback.answer(f"Загружаю последние {limit} свечей для {symbol.upper()} {timeframe} мин...")
    await bot.send_chat_action(user_id, "upload_photo")

    chart_path = None
    try:
        candles = await get_candles(symbol, timeframe, limit=limit)
        if candles:
            chart_path = await plot_ohlcv_chart(candles, symbol, timeframe, limit=limit)
            caption = f"🚀 Быстрый график: {symbol.upper()} {timeframe} мин\nПоследние {limit} свечей"
        else:
            symbol_tf_esc = escape_markdown_v2(f"{symbol.upper()}/{timeframe}")
            await callback.message.answer(f"❌ Не удалось получить данные от API для `{symbol_tf_esc}`\\.", parse_mode="MarkdownV2")
            await state.clear()
            kb = [[types.InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]]
            # Пытаемся отредактировать сообщение, где были кнопки выбора периода
            try: await callback.message.edit_reply_markup(reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb))
            except: pass # Игнорируем ошибку, если сообщение уже удалено или изменилось
            return

        if chart_path:
            logger.info(f"Отправка быстрого графика (latest) {chart_path} пользователю {safe_username_log} ({user_id})")
            chart_file = FSInputFile(chart_path)
            try:
                 kb = [[types.InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]]
                 mk = types.InlineKeyboardMarkup(inline_keyboard=kb)
                 # Отправляем новым сообщением
                 await bot.send_photo(user_id, chart_file, caption=caption, reply_markup=mk)
                 logger.info(f"Быстрый график (latest) успешно отправлен {safe_username_log} ({user_id})")
                 # Удаляем предыдущее сообщение с кнопками выбора типа
                 try: await callback.message.delete()
                 except Exception as e: logger.debug(f"Не удалось удалить сообщение с кнопками выбора типа: {e}")
            except TelegramAPIError as send_error:
                logger.error(f"Ошибка отправки быстрого графика (latest) {safe_username_log} ({user_id}): {send_error}", exc_info=True)
                await callback.message.answer("❌ Не удалось отправить график.") # Отправляем в чат
            finally:
                try: os.remove(chart_path); logger.info(f"Временный файл графика {chart_path} удален.")
                except OSError as remove_error: logger.error(f"Ошибка удаления файла графика {chart_path}: {remove_error}")
        # else: # Ошибка получения данных обработана выше

    except Exception as e:
        logger.error(f"Ошибка при обработке 'latest' для быстрых графиков {safe_username_log} ({user_id}): {e}", exc_info=True)
        await callback.message.answer("❌ Произошла непредвиденная ошибка.") # Отправляем в чат

    finally:
        await state.clear()

@dp.callback_query(QuickChartState.select_period_type, F.data == "qc_type_period")
@access_check
@log_execution_time()
async def quick_charts_request_period_input(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    username = callback.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Быстрые графики: {safe_username_log} ({user_id}) выбрал 'За период времени'")
    await state.set_state(QuickChartState.enter_period_time)
    user_data = await state.get_data()
    symbol = user_data.get("quick_chart_symbol", "N/A")
    timeframe = user_data.get("quick_chart_timeframe", "N/A")

    kb = [
         [types.InlineKeyboardButton(text="🔙 Назад (Тип данных)", callback_data=f"qc_tf_{timeframe}")],
         [types.InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ]
    mk = types.InlineKeyboardMarkup(inline_keyboard=kb)

    # Используем HTML для форматирования запроса
    prompt_text = (
        f"Пара: <b>{symbol.upper()}</b>, Таймфрейм: <b>{timeframe} мин</b>\n\n"
        "Введите время начала и конца <b>сегодняшнего дня</b> (UTC) в формате:\n"
        "<code>ЧЧ:ММ ЧЧ:ММ</code> (или <code>ЧЧ ЧЧ:ММ</code>, <code>ЧЧ:ММ ЧЧ</code>, <code>ЧЧ ЧЧ</code>)\n"
        "Разделители времени: <code>:</code>, <code>_</code>, <code>-</code>.\n\n"
        "<b>Пример:</b> <code>10:00 16:30</code> (с 10:00 до 16:30 UTC)"
    )
    await callback.message.edit_text(prompt_text, reply_markup=mk, parse_mode="HTML")
    await callback.answer()

@dp.message(QuickChartState.enter_period_time)
@access_check
@log_execution_time()
async def quick_charts_process_period_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or f"ID_{user_id}"
    safe_username_log = escape_markdown_v2(username)
    time_input = message.text.strip()
    logger.info(f"Быстрые графики: {safe_username_log} ({user_id}) ввел время для периода: '{time_input}'")

    user_data = await state.get_data()
    symbol = user_data.get("quick_chart_symbol")
    timeframe = user_data.get("quick_chart_timeframe")
    # Лимит для запроса данных по периоду, можно сделать больше, т.к. время ограничено
    limit = 1000

    if not symbol or not timeframe:
        logger.error(f"Отсутствуют symbol или timeframe в состоянии FSM для {safe_username_log} ({user_id}) при обработке периода. Данные: {user_data}")
        await message.answer("Произошла внутренняя ошибка (отсутствуют данные). Попробуйте снова.")
        await state.clear()
        await show_main_menu(user_id)
        return

    time_pattern = re.compile(r"(\d{1,2})[:_\-]?(\d{2})?")
    parts = re.split(r'\s+', time_input) # Разделяем по пробелам
    if len(parts) != 2:
        await message.answer(
            "❌ Неверный формат. Нужно ввести два времени через пробел.\n"
            "<b>Пример:</b> <code>10:00 16:30</code>", parse_mode="HTML"
            )
        return # Не сбрасываем состояние

    start_time_str, end_time_str = parts

    def parse_time_part(time_part):
        match = time_pattern.fullmatch(time_part)
        if match:
            hour = int(match.group(1)); minute = int(match.group(2) or 0)
            if 0 <= hour <= 23 and 0 <= minute <= 59: return hour, minute
        raise ValueError(f"Неверный формат времени: {time_part}")

    try:
        start_hour, start_minute = parse_time_part(start_time_str)
        end_hour, end_minute = parse_time_part(end_time_str)
        now_utc = datetime.now(timezone.utc); today_utc = now_utc.date()
        start_dt_utc = datetime(today_utc.year, today_utc.month, today_utc.day, start_hour, start_minute, tzinfo=timezone.utc)
        end_dt_utc = datetime(today_utc.year, today_utc.month, today_utc.day, end_hour, end_minute, tzinfo=timezone.utc)

        if start_dt_utc >= end_dt_utc:
            await message.answer("❌ Ошибка: Время начала должно быть раньше времени окончания <b>в рамках сегодняшнего дня</b> (UTC).", parse_mode="HTML")
            return # Не сбрасываем состояние

        start_ts = int(start_dt_utc.timestamp() * 1000)
        end_ts = int(end_dt_utc.timestamp() * 1000)
        logger.info(f"Быстрые графики: Распарсен период для {safe_username_log} ({user_id}): {start_dt_utc.strftime('%H:%M')} - {end_dt_utc.strftime('%H:%M')} UTC. Timestamps: {start_ts}, {end_ts}")

        await bot.send_chat_action(user_id, "upload_photo")
        # Используем HTML для сообщения о загрузке
        await message.answer(f"Загружаю данные для <b>{symbol.upper()} {timeframe} мин</b> за период {start_hour:02d}:{start_minute:02d} - {end_hour:02d}:{end_minute:02d} UTC...", parse_mode="HTML")

        chart_path = None
        # date_range_str для заголовка графика (без Markdown)
        date_range_str = f"Сегодня {start_hour:02d}:{start_minute:02d} - {end_hour:02d}:{end_minute:02d} UTC"
        try:
            # Запрашиваем с лимитом 1000, API вернет свечи только в указанном диапазоне start_ts/end_ts
            candles = await get_candles(symbol, timeframe, limit=limit, start_ts=start_ts, end_ts=end_ts)
            if candles:
                 # Используем date_range_str для заголовка, limit для информации
                 chart_path = await plot_ohlcv_chart(candles, symbol, timeframe, limit=len(candles), date_range=date_range_str)
                 # Формируем подпись без Markdown V2
                 caption = f"🚀 Быстрый график: {symbol.upper()} {timeframe} мин\n{date_range_str}\n({len(candles)} свечей)"
            else:
                 symbol_tf_esc = escape_markdown_v2(f"{symbol.upper()}/{timeframe}")
                 await message.answer(f"❌ Не удалось получить данные от API для `{symbol_tf_esc}` за указанный период\\.", parse_mode="MarkdownV2")
                 await state.clear(); await show_main_menu(user_id); return

            if chart_path:
                logger.info(f"Отправка быстрого графика (period) {chart_path} пользователю {safe_username_log} ({user_id})")
                chart_file = FSInputFile(chart_path)
                try:
                     kb = [[types.InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]]
                     mk = types.InlineKeyboardMarkup(inline_keyboard=kb)
                     await message.answer_photo(chart_file, caption=caption, reply_markup=mk) # Отправляем без parse_mode
                     logger.info(f"Быстрый график (period) успешно отправлен {safe_username_log} ({user_id})")
                except TelegramAPIError as send_error:
                    logger.error(f"Ошибка отправки быстрого графика (period) {safe_username_log} ({user_id}): {send_error}", exc_info=True)
                    await message.answer("❌ Не удалось отправить график.")
                finally:
                    try: os.remove(chart_path); logger.info(f"Временный файл графика {chart_path} удален.")
                    except OSError as remove_error: logger.error(f"Ошибка удаления файла графика {chart_path}: {remove_error}")
            elif candles: # Данные есть, но график не построился
                 await message.answer("⚠️ Не удалось построить график для полученных данных.")

        except Exception as e:
             logger.error(f"Ошибка при обработке 'period' для быстрых графиков {safe_username_log} ({user_id}): {e}", exc_info=True)
             await message.answer("❌ Произошла непредвиденная ошибка.")

        finally:
            await state.clear()

    except ValueError as e:
        logger.warning(f"Ошибка парсинга времени '{time_input}' от {safe_username_log} ({user_id}): {e}")
        error_msg = escape_markdown_v2(str(e))
        example = escape_markdown_v2("10:00 16:30")
        await message.answer(f"❌ Ошибка в формате времени: {error_msg}\\. Попробуйте еще раз\\.\n*Пример:* `{example}`", parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Неизвестная ошибка при обработке ввода времени периода '{time_input}' от {safe_username_log} ({user_id}): {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке вашего запроса.")
        await state.clear()


# -------------------- Хэндлеры для админ-панели --------------------

def admin_only(handler):
    @wraps(handler)
    async def wrapper(update: types.Update | types.Message | types.CallbackQuery, *args, **kwargs):
        user = None
        if isinstance(update, types.Message): user = update.from_user
        elif isinstance(update, types.CallbackQuery): user = update.from_user
        if not user or user.id != ADMIN_ID:
            logger.warning(f"Попытка доступа к админ-функции не админом: user={user}")
            if isinstance(update, types.CallbackQuery): await update.answer("⛔ Доступ запрещен!", show_alert=True)
            elif isinstance(update, types.Message): await update.answer("⛔ У вас нет прав для выполнения этой команды.")
            return
        return await handler(update, *args, **kwargs)
    return wrapper

@dp.callback_query(F.data == "admin_panel")
@admin_only
@log_execution_time()
async def show_admin_panel_callback(callback: types.CallbackQuery):
    """Отображает админ-панель, редактируя сообщение."""
    await show_admin_panel(callback.from_user.id, callback.message.message_id)
    await callback.answer()

async def show_admin_panel(user_id: int, message_id: int | None = None):
    """Отображает или редактирует админ-панель."""
    username = f"ID_{user_id}"
    try: user_info = await bot.get_chat(user_id); username = user_info.username or username
    except: pass
    safe_username_log = escape_markdown_v2(username)
    logger.info(f"Отображение админ-панели для {safe_username_log} ({user_id})")

    keyboard = [
        [types.InlineKeyboardButton(text="➕ Реф. ссылка", callback_data="admin_create_ref"),
         types.InlineKeyboardButton(text="👀 Активные реф.", callback_data="admin_show_refs"),
         types.InlineKeyboardButton(text="🗑 Удалить реф.", callback_data="admin_remove_ref")],
        [types.InlineKeyboardButton(text="📜 Whitelist", callback_data="admin_view_whitelist"),
         types.InlineKeyboardButton(text="🚫 Banlist", callback_data="admin_view_banlist"),
         types.InlineKeyboardButton(text="📥 Лог", callback_data="admin_download_log")],
        [types.InlineKeyboardButton(text="➖ Удалить из WL", callback_data="admin_remove_wl"),
         types.InlineKeyboardButton(text="❌ Забанить", callback_data="admin_ban"),
         types.InlineKeyboardButton(text="✅ Разбанить", callback_data="admin_unban")],
        [types.InlineKeyboardButton(text=f"WL: {'ВКЛ ✅' if WHITELIST_ENABLED else 'ВЫКЛ ❌'}", callback_data="admin_toggle_whitelist")],
        [types.InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_main")]
    ]
    markup = types.InlineKeyboardMarkup(inline_keyboard=keyboard)
    text = "👑 <b>Админ-панель</b>\nВыберите действие:"

    try:
        if message_id:
             await bot.edit_message_text(
                 text=text,
                 chat_id=user_id,
                 message_id=message_id,
                 reply_markup=markup,
                 parse_mode="HTML"
             )
             logger.info(f"Админ-панель отредактирована для {safe_username_log} ({user_id})")
        else:
             await bot.send_message(user_id, text, reply_markup=markup, parse_mode="HTML")
             logger.info(f"Админ-панель отправлена для {safe_username_log} ({user_id})")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
             logger.error(f"Ошибка Telegram BadRequest при отображении админ-панели для {safe_username_log} ({user_id}): {e}")
        else:
             logger.warning(f"Админ-панель не изменена для {safe_username_log} ({user_id})")
    except Exception as e:
        # Ловим Pydantic ValidationError или другие ошибки
        logger.error(f"Неизвестная ошибка при отображении админ-панели для {safe_username_log} ({user_id}): {e}", exc_info=True) # Добавляем exc_info для Pydantic


# -------------------- Админские действия: Whitelist, Banlist, Логи --------------------

async def _display_user_list(callback: types.CallbackQuery, list_type: str, users: list[dict]):
    """Вспомогательная функция для отображения Whitelist/Banlist."""
    if not users:
        text = f"ℹ️ {list_type} пуст."
    else:
        # Экранируем ID и username для MarkdownV2
        lines = [f"{hcode(escape_markdown_v2(str(user['id'])))} \\- {escape_markdown_v2(user.get('username', 'N/A'))}" for user in users]
        title = escape_markdown_v2(f"{list_type} ({len(users)} пользователей):")
        icon = "📜" if list_type == "Whitelist" else "🚫"
        text = f"{icon} *{title}*\n\n" + "\n".join(lines)

    kb = [[types.InlineKeyboardButton(text="🔙 Назад в админку", callback_data="admin_panel")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2")
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
             logger.error(f"Ошибка редактирования {list_type}: {e}")
             # Попробуем без Markdown
             try:
                 raw_text = f"{icon} {list_type} ({len(users)} пользователей):\n\n" + "\n".join([f"{user['id']} - {user.get('username', 'N/A')}" for user in users]) if users else f"ℹ️ {list_type} пуст."
                 await callback.message.edit_text(raw_text, reply_markup=markup)
             except Exception as fallback_e:
                  logger.error(f"Ошибка отправки {list_type} без Markdown: {fallback_e}")
    await callback.answer()


@dp.callback_query(F.data == "admin_view_whitelist")
@admin_only
@log_execution_time()
async def admin_view_whitelist(callback: types.CallbackQuery):
    logger.info(f"Админ {callback.from_user.id} запросил просмотр whitelist.")
    await _display_user_list(callback, "Whitelist", load_whitelist())

@dp.callback_query(F.data == "admin_view_banlist")
@admin_only
@log_execution_time()
async def admin_view_banlist(callback: types.CallbackQuery):
    logger.info(f"Админ {callback.from_user.id} запросил просмотр banlist.")
    await _display_user_list(callback, "Banlist", load_banlist())


@dp.callback_query(F.data == "admin_download_log")
@admin_only
@log_execution_time()
async def admin_download_log(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    logger.info(f"Админ {user_id} запросил скачивание лог-файла: {log_filepath}")
    await callback.answer("📤 Отправляю лог-файл...")
    await bot.send_chat_action(user_id, "upload_document")

    if os.path.exists(log_filepath):
        try:
            log_file = FSInputFile(log_filepath)
            filename_esc = escape_markdown_v2(os.path.basename(log_filepath))
            await callback.message.answer_document(log_file, caption=f"📄 Текущий лог\\-файл:\n`{filename_esc}`", parse_mode="MarkdownV2")
            logger.info(f"Лог-файл {log_filepath} успешно отправлен админу {user_id}.")
        except TelegramAPIError as e:
            logger.error(f"Ошибка Telegram API при отправке лог-файла {log_filepath} админу {user_id}: {e}", exc_info=True)
            try: # Пробуем отправить без форматирования
                await callback.message.answer_document(log_file, caption=f"Текущий лог-файл:\n{os.path.basename(log_filepath)}")
            except Exception as fallback_e:
                logger.error(f"Ошибка отправки лога без Markdown: {fallback_e}")
                await callback.message.answer(f"❌ Не удалось отправить лог-файл: {e}")
        except Exception as e:
            logger.error(f"Неизвестная ошибка при отправке лог-файла {log_filepath} админу {user_id}: {e}", exc_info=True)
            await callback.message.answer(f"❌ Не удалось отправить лог-файл: {e}")
    else:
        logger.error(f"Лог-файл {log_filepath} не найден для отправки админу {user_id}.")
        filename_esc = escape_markdown_v2(os.path.basename(log_filepath))
        await callback.message.answer(f"❌ Лог\\-файл не найден: `{filename_esc}`", parse_mode="MarkdownV2")


# -------------------- Админские действия: Управление пользователями --------------------

@dp.callback_query(F.data == "admin_remove_wl")
@admin_only
@log_execution_time()
async def admin_request_remove_wl(callback: types.CallbackQuery, state: FSMContext):
    logger.info(f"Админ {callback.from_user.id} инициировал удаление из whitelist.")
    await state.set_state(AdminState.waiting_for_remove_id)
    kb = [[types.InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_panel")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
    # Используем HTML для простоты
    await callback.message.edit_text("➖ Введите User ID или @username пользователя для удаления из Whitelist:", reply_markup=markup, parse_mode="HTML")
    await callback.answer()

@dp.message(AdminState.waiting_for_remove_id)
@admin_only
@log_execution_time()
async def admin_process_remove_wl(message: types.Message, state: FSMContext):
    identifier = message.text.strip()
    admin_id = message.from_user.id
    logger.info(f"Админ {admin_id} ввел идентификатор для удаления из WL: {identifier}")

    if remove_from_whitelist(identifier):
        id_esc = escape_markdown_v2(identifier)
        await message.answer(f"✅ Пользователь `{id_esc}` удален из Whitelist\\.", parse_mode="MarkdownV2")
    else:
        id_esc = escape_markdown_v2(identifier)
        await message.answer(f"⚠️ Пользователь `{id_esc}` не найден в Whitelist\\.", parse_mode="MarkdownV2")

    await state.clear()
    await show_admin_panel(admin_id) # Отправляем админку новым сообщением

@dp.callback_query(F.data == "admin_ban")
@admin_only
@log_execution_time()
async def admin_request_ban(callback: types.CallbackQuery, state: FSMContext):
    logger.info(f"Админ {callback.from_user.id} инициировал бан.")
    await state.set_state(AdminState.waiting_for_ban_id)
    kb = [[types.InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_panel")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text("🚫 Введите User ID пользователя для бана:", reply_markup=markup, parse_mode="HTML")
    await callback.answer()

@dp.message(AdminState.waiting_for_ban_id)
@admin_only
@log_execution_time()
async def admin_process_ban(message: types.Message, state: FSMContext):
    identifier = message.text.strip()
    admin_id = message.from_user.id
    logger.info(f"Админ {admin_id} ввел ID для бана: {identifier}")
    try:
        user_id_to_ban = int(identifier)
        # Пытаемся получить username для лога
        user_info_str = f"ID_{user_id_to_ban}"
        try:
            chat_info = await bot.get_chat(user_id_to_ban)
            if chat_info.username: user_info_str = f"@{chat_info.username}"
        except Exception as e: logger.warning(f"Не удалось получить инфо о пользователе {user_id_to_ban} для бана: {e}")

        if ban_user(user_id_to_ban, user_info_str):
            id_esc = escape_markdown_v2(str(user_id_to_ban))
            await message.answer(f"🚫 Пользователь с ID `{id_esc}` забанен\\.", parse_mode="MarkdownV2")
        else:
            id_esc = escape_markdown_v2(str(user_id_to_ban))
            admin_id_esc = escape_markdown_v2(str(ADMIN_ID))
            if user_id_to_ban == ADMIN_ID: await message.answer(f"⚠️ Нельзя забанить администратора \\(ID: `{admin_id_esc}`\\)\\.", parse_mode="MarkdownV2")
            else: await message.answer(f"⚠️ Пользователь с ID `{id_esc}` уже был забанен\\.", parse_mode="MarkdownV2")

    except ValueError:
        logger.warning(f"Админ {admin_id} ввел нечисловой ID для бана: {identifier}")
        await message.answer("❌ Ошибка: Введите корректный числовой User ID.")
        return # Не сбрасываем состояние, даем исправить

    await state.clear()
    await show_admin_panel(admin_id) # Отправляем админку новым сообщением

@dp.callback_query(F.data == "admin_unban")
@admin_only
@log_execution_time()
async def admin_request_unban(callback: types.CallbackQuery, state: FSMContext):
    logger.info(f"Админ {callback.from_user.id} инициировал разбан.")
    await state.set_state(AdminState.waiting_for_unban_id)
    kb = [[types.InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_panel")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text("✅ Введите User ID пользователя для разбана:", reply_markup=markup, parse_mode="HTML")
    await callback.answer()

@dp.message(AdminState.waiting_for_unban_id)
@admin_only
@log_execution_time()
async def admin_process_unban(message: types.Message, state: FSMContext):
    identifier = message.text.strip()
    admin_id = message.from_user.id
    logger.info(f"Админ {admin_id} ввел ID для разбана: {identifier}")
    try:
        user_id_to_unban = int(identifier)
        id_esc = escape_markdown_v2(str(user_id_to_unban))
        if unban_user(user_id_to_unban):
            await message.answer(f"✅ Пользователь с ID `{id_esc}` разбанен\\.", parse_mode="MarkdownV2")
        else:
            await message.answer(f"⚠️ Пользователь с ID `{id_esc}` не найден в Banlist\\.", parse_mode="MarkdownV2")
    except ValueError:
        logger.warning(f"Админ {admin_id} ввел нечисловой ID для разбана: {identifier}")
        await message.answer("❌ Ошибка: Введите корректный числовой User ID.")
        return # Не сбрасываем состояние

    await state.clear()
    await show_admin_panel(admin_id) # Отправляем админку новым сообщением

@dp.callback_query(F.data == "admin_toggle_whitelist")
@admin_only
@log_execution_time()
async def admin_toggle_whitelist(callback: types.CallbackQuery):
    global WHITELIST_ENABLED
    WHITELIST_ENABLED = not WHITELIST_ENABLED
    status = "ВКЛЮЧЕН ✅" if WHITELIST_ENABLED else "ВЫКЛЮЧЕН ❌"
    logger.info(f"Админ {callback.from_user.id} переключил Whitelist. Новый статус: {status}")
    await callback.answer(f"Whitelist теперь {status}", show_alert=True)
    # Обновляем сообщение админ-панели
    await show_admin_panel(callback.from_user.id, callback.message.message_id)


# -------------------- Админские действия: Реферальные ссылки --------------------

@dp.callback_query(F.data == "admin_create_ref")
@admin_only
@log_execution_time()
async def admin_request_ref_activations(callback: types.CallbackQuery, state: FSMContext):
    logger.info(f"Админ {callback.from_user.id} инициировал создание реф. ссылки.")
    await state.set_state(AdminState.waiting_for_activations)
    kb = [[types.InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_panel")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text("➕ Введите количество активаций для реферальной ссылки (число):", reply_markup=markup, parse_mode="HTML")
    await callback.answer()

@dp.message(AdminState.waiting_for_activations)
@admin_only
@log_execution_time()
async def admin_process_ref_activations(message: types.Message, state: FSMContext):
    admin_id = message.from_user.id
    logger.info(f"Админ {admin_id} ввел кол-во активаций: {message.text}")
    try:
        activations = int(message.text.strip())
        if activations <= 0:
            await message.answer("❌ Количество активаций должно быть положительным числом.")
            return # Не сбрасываем состояние
        await state.update_data(ref_activations=activations)
        await state.set_state(AdminState.waiting_for_expire_time)
        kb = [[types.InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_panel")]]
        markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
        # Используем MarkdownV2 с экранированием
        prompt = (
            "⏰ Введите время жизни ссылки:\n"
            "`5m` \\- 5 минут\n"
            "`1h` \\- 1 час\n"
            "`7d` \\- 7 дней\n"
            "`0` \\- бессрочная\n\n"
            "Формат: число \\+ буква \\(m/h/d\\)"
        )
        await message.answer(prompt, reply_markup=markup, parse_mode="MarkdownV2")
    except ValueError:
        logger.warning(f"Админ {admin_id} ввел нечисловое кол-во активаций: {message.text}")
        await message.answer("❌ Ошибка: Введите целое число для количества активаций.")
        # Не сбрасываем состояние

@dp.message(AdminState.waiting_for_expire_time)
@admin_only
@log_execution_time()
async def admin_process_ref_expire_time(message: types.Message, state: FSMContext):
    expire_time_str = message.text.strip().lower()
    admin_id = message.from_user.id
    logger.info(f"Админ {admin_id} ввел срок действия реф. ссылки: {expire_time_str}")

    if expire_time_str != "0" and not re.fullmatch(r"\d+[mhd]", expire_time_str):
        logger.warning(f"Неверный формат срока действия '{expire_time_str}' от админа {admin_id}")
        await message.answer(
             "❌ Неверный формат времени\\. Используйте число \\+ `m`, `h` или `d`, либо `0` для бессрочной\\.\n"
             "*Примеры:* `30m`, `2h`, `1d`, `0`", parse_mode="MarkdownV2"
             )
        return # Не сбрасываем состояние

    user_data = await state.get_data()
    activations = user_data.get("ref_activations")
    if not activations:
        logger.error(f"Отсутствует кол-во активаций в состоянии FSM для админа {admin_id} при создании реф. ссылки.")
        await message.answer("❌ Внутренняя ошибка (нет данных об активациях). Попробуйте снова.")
        await state.clear()
        await show_admin_panel(admin_id)
        return

    try:
        bot_info = await bot.get_me()
        ref_link, ref_code = generate_referral_link(activations, expire_time_str, bot_info.username)
        if ref_link and ref_code:
             link_esc = escape_markdown_v2(ref_link)
             code_esc = escape_markdown_v2(ref_code)
             await message.answer(f"✅ Реферальная ссылка создана:\n`{link_esc}`\nКод: `{code_esc}`", parse_mode="MarkdownV2")
        else:
             raise Exception("generate_referral_link вернула None")
    except Exception as e:
        logger.error(f"Ошибка генерации реф. ссылки админом {admin_id}: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при создании ссылки.")

    await state.clear()
    await show_admin_panel(admin_id)

@dp.callback_query(F.data == "admin_show_refs")
@admin_only
@log_execution_time()
async def admin_show_active_refs(callback: types.CallbackQuery):
    logger.info(f"Админ {callback.from_user.id} запросил список активных реф. ссылок.")
    active_refs = get_active_referrals() # Функция теперь возвращает экранированный список

    if not active_refs:
        text = "ℹ️ Активных реферальных ссылок нет."
    else:
        # get_active_referrals уже возвращает строки с MarkdownV2
        text = "👀 *Активные реферальные ссылки:*\n\n" + "\n".join(active_refs)

    kb = [[types.InlineKeyboardButton(text="🔙 Назад в админку", callback_data="admin_panel")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="MarkdownV2")
    except TelegramBadRequest as e:
         if "message is not modified" not in str(e):
             logger.error(f"Ошибка Telegram BadRequest при показе реф ссылок: {e}")
             # Попробуем без Markdown
             try:
                 raw_text = "Активные реферальные ссылки:\n\n" + "\n".join([re.sub(r'\\([_*\[\]()~`>#+\-=|{}.!])', r'\1', line) for line in active_refs]) if active_refs else "ℹ️ Активных реферальных ссылок нет."
                 await callback.message.edit_text(raw_text, reply_markup=markup)
             except Exception as fallback_e:
                 logger.error(f"Ошибка отправки реф ссылок без Markdown: {fallback_e}")
    except Exception as e:
         logger.error(f"Неизвестная ошибка при показе реф ссылок: {e}")

    await callback.answer()


@dp.callback_query(F.data == "admin_remove_ref")
@admin_only
@log_execution_time()
async def admin_request_remove_ref(callback: types.CallbackQuery, state: FSMContext):
    logger.info(f"Админ {callback.from_user.id} инициировал удаление реф. ссылки.")
    await state.set_state(AdminState.waiting_for_deactivate_ref_code)
    kb = [[types.InlineKeyboardButton(text="🔙 Отмена", callback_data="admin_panel")]]
    markup = types.InlineKeyboardMarkup(inline_keyboard=kb)
    await callback.message.edit_text("🗑 Введите код реферальной ссылки для удаления:", reply_markup=markup, parse_mode="HTML")
    await callback.answer()

@dp.message(AdminState.waiting_for_deactivate_ref_code)
@admin_only
@log_execution_time()
async def admin_process_remove_ref(message: types.Message, state: FSMContext):
    code = message.text.strip()
    admin_id = message.from_user.id
    logger.info(f"Админ {admin_id} ввел код для удаления реф. ссылки: {code}")
    code_esc = escape_markdown_v2(code)
    if deactivate_referral(code):
        await message.answer(f"✅ Реферальная ссылка с кодом `{code_esc}` удалена\\.", parse_mode="MarkdownV2")
    else:
        await message.answer(f"⚠️ Реферальная ссылка с кодом `{code_esc}` не найдена\\.", parse_mode="MarkdownV2")

    await state.clear()
    await show_admin_panel(admin_id)


# -------------------- Запуск бота --------------------
async def main():
    logger.info("--- Инициализация бота ---")
    if not await check_api_auth():
        logger.critical("ОШИБКА: Не удалось подключиться или авторизоваться в API. Бот может работать некорректно.")

    load_whitelist(); load_banlist(); load_referrals()

    # Запуск фоновой задачи обновления описания
    description_task = asyncio.create_task(
        description.run_description_updater(bot, API_BASE_URL, API_AUTH_HEADER)
    )

    logger.info("--- Запуск поллинга ---")
    try:
         await dp.start_polling(bot)
    except Exception as e:
         logger.critical(f"Критическая ошибка в start_polling: {e}", exc_info=True)
    finally:
         logger.info("--- Остановка бота ---")
         if not description_task.done():
             description_task.cancel()
             try: await description_task # Ждем завершения задачи
             except asyncio.CancelledError: logger.info("Задача обновления описания отменена.")
         await bot.session.close()
         logger.info("Сессия бота закрыта.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен вручную (KeyboardInterrupt/SystemExit)")
    except Exception as global_error:
        logger.critical(f"Глобальная неперехваченная ошибка в __main__: {global_error}", exc_info=True)