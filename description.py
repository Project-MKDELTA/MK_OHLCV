import asyncio
import aiohttp
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramAPIError

logger = logging.getLogger(__name__)

# Используем N/A по умолчанию, чтобы главное меню могло их импортировать
BTC_PRICE = "N/A"
ETH_PRICE = "N/A"

async def update_bot_description(bot: Bot, api_base_url: str, api_auth_header: dict):
    """
    Функция для обновления описания бота и глобальных переменных с актуальными ценами BTC и ETH
    """
    global BTC_PRICE, ETH_PRICE
    logger.debug("Начало обновления описания и цен...")

    btc_url = f"{api_base_url}/candles/latest/btcusdt/5"
    eth_url = f"{api_base_url}/candles/latest/ethusdt/5"

    # Получаем текущие значения перед запросом API
    current_btc = BTC_PRICE
    current_eth = ETH_PRICE
    new_btc_price = None
    new_eth_price = None

    try:
        async with aiohttp.ClientSession() as session:
            # --- Запрос BTC ---
            try:
                logger.debug(f"Запрос BTC: {btc_url}")
                async with session.get(btc_url, headers=api_auth_header, timeout=10) as btc_response:
                    if btc_response.status == 200:
                        btc_data = await btc_response.json()
                        # Используем get с проверкой типа для большей надежности
                        close_price = btc_data.get("close") if isinstance(btc_data, dict) else None
                        high_price = btc_data.get("high") if isinstance(btc_data, dict) else None

                        if isinstance(close_price, (int, float)) and close_price > 0:
                            new_btc_price = float(close_price)
                            logger.debug(f"Получена цена BTC (close): {new_btc_price}")
                        elif isinstance(high_price, (int, float)) and high_price > 0: # Fallback на high
                             new_btc_price = float(high_price)
                             logger.debug(f"Получена цена BTC (high): {new_btc_price}")
                        else:
                             logger.warning(f"Некорректные или нулевые данные BTC: {btc_data}")
                    else:
                        logger.error(f"Ошибка при запросе BTC: {btc_response.status}, Ответ: {await btc_response.text()}")
            except asyncio.TimeoutError:
                logger.error("Таймаут при запросе BTC.")
            except Exception as e:
                logger.error(f"Исключение при запросе BTC: {e}", exc_info=True)

            # --- Запрос ETH ---
            try:
                logger.debug(f"Запрос ETH: {eth_url}")
                async with session.get(eth_url, headers=api_auth_header, timeout=10) as eth_response:
                    if eth_response.status == 200:
                        eth_data = await eth_response.json()
                        close_price = eth_data.get("close") if isinstance(eth_data, dict) else None
                        high_price = eth_data.get("high") if isinstance(eth_data, dict) else None

                        if isinstance(close_price, (int, float)) and close_price > 0:
                            new_eth_price = float(close_price)
                            logger.debug(f"Получена цена ETH (close): {new_eth_price}")
                        elif isinstance(high_price, (int, float)) and high_price > 0: # Fallback на high
                             new_eth_price = float(high_price)
                             logger.debug(f"Получена цена ETH (high): {new_eth_price}")
                        else:
                             logger.warning(f"Некорректные или нулевые данные ETH: {eth_data}")
                    else:
                        logger.error(f"Ошибка при запросе ETH: {eth_response.status}, Ответ: {await eth_response.text()}")
            except asyncio.TimeoutError:
                logger.error("Таймаут при запросе ETH.")
            except Exception as e:
                logger.error(f"Исключение при запросе ETH: {e}", exc_info=True)

        # --- Обновляем глобальные переменные ---
        update_description_needed = False
        btc_updated = False
        eth_updated = False

        if new_btc_price is not None and new_btc_price > 0:
            formatted_btc = "{:.2f}".format(new_btc_price)
            if formatted_btc != BTC_PRICE:
                BTC_PRICE = formatted_btc
                update_description_needed = True
                btc_updated = True
                logger.info(f"Глобальная переменная BTC_PRICE обновлена: {BTC_PRICE}")
        elif BTC_PRICE == "N/A":
             logger.warning("Не удалось получить валидную цену BTC, оставляем N/A.")
        else:
             logger.warning(f"Не удалось получить новую валидную цену BTC, используется старое значение: {BTC_PRICE}")


        if new_eth_price is not None and new_eth_price > 0:
            formatted_eth = "{:.2f}".format(new_eth_price)
            if formatted_eth != ETH_PRICE:
                 ETH_PRICE = formatted_eth
                 update_description_needed = True
                 eth_updated = True
                 logger.info(f"Глобальная переменная ETH_PRICE обновлена: {ETH_PRICE}")
        elif ETH_PRICE == "N/A":
             logger.warning("Не удалось получить валидную цену ETH, оставляем N/A.")
        else:
             logger.warning(f"Не удалось получить новую валидную цену ETH, используется старое значение: {ETH_PRICE}")

        # --- Обновляем описание бота ---
        # Обновляем, если хотя бы одна цена изменилась, ИЛИ если хотя бы одна цена все еще N/A (первый запуск/ошибка)
        should_update_tg_description = update_description_needed or (BTC_PRICE == "N/A") or (ETH_PRICE == "N/A")

        if should_update_tg_description:
            # Формируем описание бота (просто текст, без Markdown)
            # Убедимся, что даже если одна цена N/A, другая отображается
            description_text = f"BTC/USDT - {BTC_PRICE}$\nETH/USDT - {ETH_PRICE}$"
            logger.info(f"Попытка обновить описание бота на: '{description_text.replace(chr(10), ' ')}'") # Логируем перед вызовом
            try:
                # Установка пустого описания может сбросить его к значению по умолчанию у BotFather
                # Если обе цены N/A, возможно, лучше не обновлять или ставить заглушку?
                # Пока оставляем как есть: будет "BTC/USDT - N/A$\nETH/USDT - N/A$"
                if BTC_PRICE == "N/A" and ETH_PRICE == "N/A" and current_btc == "N/A" and current_eth == "N/A":
                     logger.info("Обе цены N/A, описание не обновляется для предотвращения сброса.")
                else:
                     result = await bot.set_my_description(description=description_text)
                     if result:
                         logger.info(f"Описание бота успешно обновлено Telegram API.")
                     else:
                          logger.warning("bot.set_my_description вернул False.")

            except TelegramBadRequest as e:
                 # Игнорируем ошибку "description is not modified"
                 if "description is not modified" in str(e).lower():
                     logger.info("Описание бота не изменилось (уже актуально).")
                 else:
                     logger.error(f"Ошибка Telegram BadRequest при обновлении описания бота: {e}")
            except TelegramAPIError as e:
                 logger.error(f"Ошибка Telegram API при обновлении описания бота: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Неизвестная ошибка при обновлении описания бота: {e}", exc_info=True)
        else:
            logger.debug(f"Цены не изменились (BTC: {BTC_PRICE}, ETH: {ETH_PRICE}), описание бота не требует обновления.")

    except Exception as e:
        logger.error(f"Глобальная ошибка в update_bot_description: {e}", exc_info=True)

    # Возвращать значения не обязательно, т.к. main.py будет импортировать их
    # return BTC_PRICE, ETH_PRICE


async def run_description_updater(bot: Bot, api_base_url: str, api_auth_header: dict):
    """
    Функция для запуска обновления описания в отдельной задаче
    """
    logger.info("Запуск цикла run_description_updater...")
    # Сделаем первый запуск с небольшой задержкой, чтобы бот успел инициализироваться
    await asyncio.sleep(5)
    while True:
        try:
            await update_bot_description(bot, api_base_url, api_auth_header)
            # Пауза перед следующим обновлением (5 минут)
            await asyncio.sleep(300)
        except asyncio.CancelledError:
             logger.info("Задача run_description_updater отменена.")
             break # Выходим из цикла при отмене
        except Exception as e:
             logger.error(f"Критическая ошибка в цикле run_description_updater: {e}", exc_info=True)
             # Ждем дольше перед повторной попыткой в случае серьезной ошибки
             await asyncio.sleep(300)
