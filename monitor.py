# -*- coding: utf-8 -*-
"""
Мониторинг Telegram: отслеживает все чаты в папке "Батуми" в реальном времени.
При появлении сообщения с запросом фотографа шлёт уведомление через бота —
обычное сообщение со звуком в ваш основной аккаунт.

Два режима работы:
  * локально  — config.json рядом, работает, пока открыто окно (run.bat);
  * в облаке  — MONITOR_MODE=cloud, настройки из переменных окружения TG_*,
                работа идёт «сменами» (GitHub Actions перезапускает скрипт),
                а state.json запоминает, что уже обработано, чтобы ничего
                не потерять и не дублировать при перезапусках.
"""
import asyncio
import base64
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque

from telethon import TelegramClient, events, utils
from telethon.tl.functions.messages import GetDialogFiltersRequest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
SESSION_PATH = os.path.join(BASE_DIR, "session")
LOG_PATH = os.path.join(BASE_DIR, "monitor.log")
STATE_PATH = os.path.join(BASE_DIR, "state.json")

# --- Логирование ---
# Форсируем UTF-8 для вывода в консоль, чтобы не падать на эмодзи/кириллице
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("batumi-monitor")

# --- Режим работы ---
CLOUD = os.environ.get("MONITOR_MODE", "").strip().lower() == "cloud"


def _env(name: str, default=None):
    """Значение переменной окружения, иначе default (пустая строка = не задано)."""
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def _flag(v, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _words(env_name: str, cfg_key: str, default: list[str]) -> list[str]:
    raw = _env(env_name)
    items = raw.split(",") if raw else CFG.get(cfg_key, default)
    return [str(w).strip().lower() for w in items if str(w).strip()]


# --- Конфигурация ---
# В облаке значения приходят из переменных окружения (секреты GitHub),
# локально — из config.json. Переменные окружения имеют приоритет.
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, encoding="utf-8-sig") as f:
        CFG = json.load(f)
else:
    CFG = {}

API_ID = int(_env("TG_API_ID", CFG.get("api_id", 0)) or 0)
API_HASH = str(_env("TG_API_HASH", CFG.get("api_hash", "")))
FOLDER_NAME = str(_env("TG_FOLDER", CFG.get("folder", "Батуми")))
MONITOR_OUTGOING = _flag(_env("TG_OUTGOING"), CFG.get("monitor_outgoing", False))
HEARTBEAT_MINUTES = int(_env("TG_HEARTBEAT", CFG.get("heartbeat_minutes", 30)) or 30)

# --- Правила поиска (сочетания слов) ---
# Совпадение считается, если:
#   1) встречается любое слово из KEYWORD_ALONE (например «фотографа»), ИЛИ
#   2) встречается корень KEYWORD_ROOT («фотограф») И хотя бы один триггер («ищу», «нужен»...)
KEYWORD_ALONE = _words("TG_ALONE", "keyword_alone", ["фотографа"])
KEYWORD_ROOT = str(_env("TG_ROOT", CFG.get("keyword_root", "фотограф"))).lower()
KEYWORD_TRIGGERS = _words(
    "TG_TRIGGERS",
    "keyword_triggers",
    ["ищу", "нужен", "нужен", "требуется", "ищем", "посовет", "порекоменд"],
)

# --- Доставка уведомлений через бота ---
# Бот пишет вам в личку как обычный отправитель — поэтому звук и вибрация работают.
# Настройка: см. bot_setup.py (создать бота у @BotFather, вставить токен, написать ему /start).
NOTIFY_BOT_TOKEN = str(_env("TG_BOT_TOKEN", CFG.get("notify_bot_token", ""))).strip()
NOTIFY_BOT_CHAT_ID = int(_env("TG_CHAT_ID", CFG.get("notify_bot_chat_id", 0)) or 0)

# --- Резервный канал доставки ---
# Если Bot API недоступен (блокировка провайдера/сбой), уведомление уйдёт
# сюда через основную сессию Telegram. Обычно там без звука, но зато доходит.
# Формат: «me», @username, ID или точное НАЗВАНИЕ чата.
NOTIFY_FALLBACK = str(_env("TG_FALLBACK", CFG.get("notify_fallback", ""))).strip()

# --- Для облачного режима: сохранение состояния в репозиторий ---
# GITHUB_TOKEN и GITHUB_REPOSITORY подставляет сам GitHub при запуске.
GITHUB_TOKEN = _env("GITHUB_TOKEN")
GITHUB_REPOSITORY = _env("GITHUB_REPOSITORY")  # вид: owner/repo

# Счётчики для «пульса»
STATS = {"seen": 0, "matches": 0}

# ID чатов папки (пустое множество = все чаты, если папка не найдена)
folder_peer_ids: set[int] = set()
# id -> «input peer» из фильтра папки (с access_hash). Нужен для дозабора
# истории: по «голому» id Telethon не всегда может найти чат (особенно личку).
folder_peers: dict = {}

# Кэш разобранного резервного чата (int/str) — резолвим один раз при старте
fallback_target: "int | str | None" = None
fallback_resolved = False

# Защита от повторной обработки апдейтов
seen: deque[tuple[int, int]] = deque(maxlen=2000)
seen_set: set[tuple[int, int]] = set()

# Состояние обработки: {chat_id: {"last_id": N, "last_ts": N}, "__meta__": {...}}
state: dict = {}
_state_sha: str | None = None
_last_state_push = 0.0


def find_match(text: str) -> str | None:
    """Ищет совпадение по правилам. Возвращает описание правила или None."""
    t = (text or "").lower()
    if not t:
        return None
    # 1) Слово-«одиночка» (например «фотографа») — матчится само по себе
    for w in KEYWORD_ALONE:
        if w and w in t:
            return w
    # 2) Корень + хотя бы один триггер
    if KEYWORD_ROOT and KEYWORD_ROOT in t:
        for w in KEYWORD_TRIGGERS:
            if w and w in t:
                return f"{w}+{KEYWORD_ROOT}"
    return None


def bot_send_sync(text: str) -> dict:
    """
    Отправка сообщения через Bot API с повторами.
    Telegram иногда отдаёт 502/таймауты — такие сбои временные, повторяем.
    """
    url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode(
        {"chat_id": NOTIFY_BOT_CHAT_ID, "text": text}
    ).encode()

    attempts = 4
    last_err = "неизвестно"
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            if body.get("ok"):
                return body
            last_err = f"Bot API: {body}"
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:200]
            except Exception:
                pass
            last_err = f"HTTP {e.code}: {detail or e.reason}"
            # 4xx (кроме 429) повторять бессмысленно — например, неверный chat_id
            if 400 <= e.code < 500 and e.code != 429:
                break
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        if attempt < attempts:
            wait = attempt * 2
            log.warning(
                "Уведомление не доставлено (%s). Повтор %d/%d через %d сек...",
                last_err, attempt, attempts, wait,
            )
            time.sleep(wait)

    raise RuntimeError(f"не удалось отправить за {attempts} попыток: {last_err}")


async def resolve_fallback(client: TelegramClient):
    """Определяет резервный чат для уведомлений (один раз, результат кэшируется)."""
    global fallback_target, fallback_resolved
    if fallback_resolved:
        return fallback_target

    fallback_resolved = True
    t = NOTIFY_FALLBACK
    if not t or t.lower() == "me":
        fallback_target = "me"
        return fallback_target
    if t.startswith("@"):
        fallback_target = t
        return fallback_target
    try:
        fallback_target = int(t)
        return fallback_target
    except ValueError:
        pass

    # Ищем чат по точному названию среди диалогов аккаунта
    lowered = t.lower()
    async for d in client.iter_dialogs():
        if d.name and d.name.strip().lower() == lowered:
            fallback_target = d.id
            log.info("Резервный чат уведомлений: «%s» (id %s).", d.name, d.id)
            return fallback_target

    log.warning(
        "Резервный чат «%s» не найден среди диалогов — резерв отключён. "
        "Проверьте notify_fallback в config.json.",
        t,
    )
    fallback_target = None
    return fallback_target


async def send_notification(client: TelegramClient, text: str) -> str:
    """
    Отправляет уведомление. Основной путь — бот (со звуком).
    Если Bot API недоступен — резервный путь через основную сессию (но без звука).
    """
    bot_error = None
    try:
        body = await asyncio.to_thread(bot_send_sync, text)
        if body.get("ok"):
            return "через бота (со звуком)"
        raise RuntimeError(f"Bot API: {body}")
    except Exception as e:
        bot_error = e

    # --- Резерв: основная сессия Telegram ---
    target = await resolve_fallback(client)
    if target is None:
        raise RuntimeError(f"бот недоступен ({bot_error}), резервный чат не настроен")

    sent = await client.send_message(target, text)
    log.warning(
        "⚠️ Бот недоступен (%s) — уведомление доставлено в резервный чат "
        "(id сообщения %s). Там может быть без звука.",
        bot_error,
        sent.id,
    )
    return f"в резервный чат (id {sent.id}; бот был недоступен)"


# ==================== Состояние (для облачных перезапусков) ====================


def load_state() -> None:
    """Читает state.json — что уже обработано (чтобы не дублировать)."""
    global state
    if not os.path.exists(STATE_PATH):
        state = {}
        return
    try:
        with open(STATE_PATH, encoding="utf-8-sig") as f:
            state = json.load(f)
    except Exception as e:
        log.warning("state.json повреждён (%s) — начинаю с чистого листа.", e)
        state = {}


def save_state(push: bool = False, force: bool = False) -> None:
    """Сохраняет состояние локально; в облаке (по нажатию push) — и в репозиторий."""
    state["__meta__"] = {"last_save": int(time.time())}
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=1)
    except Exception as e:
        log.warning("Не удалось сохранить state.json: %s", e)
        return

    if push and GITHUB_TOKEN and GITHUB_REPOSITORY:
        # force=True — для важных случаев (найдено совпадение), без задержки
        if force or time.time() - _last_state_push >= 240:
            _push_state_remote()


def _push_state_remote() -> None:
    """Кладёт state.json обратно в репозиторий — чтобы работа не терялась при отмене смены."""
    global _state_sha, _last_state_push
    api = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/contents/state.json"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "batumi-monitor",
        "Content-Type": "application/json",
    }
    try:
        sha = _state_sha
        if sha is None:
            try:
                req = urllib.request.Request(api, headers=headers)
                with urllib.request.urlopen(req, timeout=15) as r:
                    sha = json.loads(r.read().decode()).get("sha")
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise
                sha = None  # файла ещё нет — создаём
        payload = {
            "message": f"state {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "content": base64.b64encode(
                open(STATE_PATH, "rb").read()
            ).decode(),
        }
        if sha:
            payload["sha"] = sha
        req = urllib.request.Request(
            api,
            data=json.dumps(payload).encode(),
            headers=headers,
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            body = json.loads(r.read().decode())
        _state_sha = body.get("content", {}).get("sha")
        _last_state_push = time.time()
        log.info("Состояние сохранено в репозиторий.")
    except Exception as e:
        log.warning("Не удалось обновить состояние в репозитории: %s", e)


# ==================== Обработка сообщений ====================


async def process_message(client: TelegramClient, msg, chat_id) -> None:
    """
    Разбирает одно сообщение: фильтры, правила, уведомление, отметка в state.
    Работает и для «живых» событий, и для дозабора пропущенного.
    """
    if chat_id is None:
        return
    # Исходящие по умолчанию не отслеживаем
    if msg.out and not MONITOR_OUTGOING:
        return
    # Фильтр по папке (если папка найдена)
    if folder_peer_ids and chat_id not in folder_peer_ids:
        return

    st = state.setdefault(str(chat_id), {"last_id": 0, "last_ts": 0})
    msg_id = int(msg.id or 0)
    if msg_id <= int(st.get("last_id") or 0):
        return  # уже обработано раньше

    STATS["seen"] += 1

    text = msg.message or ""
    rule = find_match(text)
    if not rule:
        # Не совпало — просто запоминаем, что дошли до этого сообщения
        st["last_id"] = msg_id
        if msg.date:
            st["last_ts"] = int(msg.date.timestamp())
        save_state()
        return

    # Дедупликация внутри текущей смены
    key = (chat_id, msg_id)
    if key in seen_set:
        return
    seen.append(key)
    seen_set.add(key)

    chat = await msg.get_chat()
    sender = await msg.get_sender()
    chat_title = (
        getattr(chat, "title", None)
        or " ".join(
            filter(None, [getattr(chat, "first_name", None), getattr(chat, "last_name", None)])
        )
        or "Неизвестный чат"
    )
    sender_name = (
        getattr(sender, "title", None)
        or " ".join(
            filter(None, [getattr(sender, "first_name", None), getattr(sender, "last_name", None)])
        )
        or "Неизвестный отправитель"
    )
    link = build_link(chat, chat_id, msg_id)

    notice = (
        f"🔍 Срабатывание: «{rule}»\n"
        f"Чат: {chat_title}\n"
        f"От: {sender_name}\n"
        f"Сообщение: {text[:1000] or '(без текста, вложение)'}\n"
        f"Ссылка: {link}"
    )
    log.info("СОВПАДЕНИЕ «%s» в «%s» от %s: %s", rule, chat_title, sender_name, text[:200])
    STATS["matches"] += 1
    try:
        where = await send_notification(client, notice)
        log.info("Уведомление отправлено %s.", where)
        # Уведомление доставлено — теперь можно запомнить, что сообщение обработано
        st["last_id"] = msg_id
        if msg.date:
            st["last_ts"] = int(msg.date.timestamp())
        save_state(push=True, force=True)
    except Exception as e:
        log.error("Не удалось отправить уведомление: %s", e)
        # last_id НЕ двигаем: в следующей смене попробуем ещё раз


async def catch_up(client: TelegramClient) -> int:
    """
    Дозабирает сообщения, пропущенные пока скрипт не работал
    (в облаке работа идёт «сменами», между ними есть паузы).
    Для чатов, которых ещё не было в state, просто запоминает текущее состояние.
    """
    total = 0
    for chat_id, peer in sorted(folder_peers.items()):
        key = str(chat_id)
        st = state.get(key)
        first = None
        fresh: list = []
        try:
            async for m in client.iter_messages(peer, limit=25):
                if first is None:
                    first = m
                if st is None:
                    break  # новый чат — базовая линия, историю не трогаем
                if int(m.id or 0) > int(st.get("last_id") or 0):
                    fresh.append(m)
                else:
                    break
        except Exception as e:
            log.warning("Не удалось прочитать историю чата id %s: %s", chat_id, e)
            continue

        if st is None:
            if first is not None:
                state[key] = {
                    "last_id": int(first.id or 0),
                    "last_ts": int(first.date.timestamp()) if first.date else int(time.time()),
                }
            continue

        for m in reversed(fresh):  # от старых к новым
            await process_message(client, m, chat_id)
            total += 1

    if total:
        log.info("Дозабрано пропущенных сообщений: %d.", total)
    save_state(push=True)
    return total


def dialog_filter_title(f) -> str:
    """Текст названия папки (совместимо с разными версиями API)."""
    title = getattr(f, "title", None)
    if not title:
        return ""
    # Новый слой: TextWithEntities с полем .text
    if hasattr(title, "text"):
        return title.text or ""
    # Старый слой: список TextWithEntities в .strings
    if hasattr(title, "strings"):
        return "".join(s.text for s in title.strings or [])
    if isinstance(title, str):
        return title
    return ""


async def refresh_folders(client: TelegramClient) -> None:
    """Загружает список чатов в папке FOLDER_NAME."""
    global folder_peer_ids, folder_peers
    try:
        result = await client(GetDialogFiltersRequest())
    except Exception as e:
        log.error("Не удалось получить фильтры (папки): %s", e)
        return

    new_peers: dict = {}
    found = False
    filters = getattr(result, "filters", None) or getattr(result, "dialogs", None) or []
    for f in filters:
        # Обычная папка (DialogFilter) или папка папок (DialogFilterChatlist)
        if not hasattr(f, "include_peers") or not hasattr(f, "title"):
            continue
        title = dialog_filter_title(f)
        if title.strip().lower() == FOLDER_NAME.strip().lower():
            found = True
            for peer in f.include_peers:
                new_peers[utils.get_peer_id(peer)] = peer
            break

    if found:
        if set(new_peers) != folder_peer_ids:
            log.info("Папка «%s»: %d чат(ов).", FOLDER_NAME, len(new_peers))
        folder_peer_ids = set(new_peers)
        folder_peers = new_peers
    else:
        folder_peer_ids = set()
        folder_peers = {}
        log.warning(
            "Папка «%s» не найдена в аккаунте — отслеживаются ВСЕ чаты. "
            "Создайте папку в Telegram или проверьте название в config.json.",
            FOLDER_NAME,
        )


def build_link(chat, chat_id: int, msg_id: int) -> str:
    """Ссылка на сообщение, если она возможна."""
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}/{msg_id}"
    # Для супергрупп/каналов: https://t.me/c/<internal_id>/<msg_id>
    if chat_id < 0:
        internal = chat_id & 0xFFFFFFFF
        return f"https://t.me/c/{internal}/{msg_id}"
    return "(приватный чат — ссылки нет)"


async def main() -> None:
    if not API_ID or "СЮДА" in API_HASH:
        log.error(
            "Заполните api_id и api_hash (локально — в config.json, "
            "в облаке — секреты TG_API_ID/TG_API_HASH; "
            "получить: https://my.telegram.org → API development tools)."
        )
        return

    if not NOTIFY_BOT_TOKEN or not NOTIFY_BOT_CHAT_ID:
        log.error(
            "Не настроен бот для уведомлений. Нужно: 1) создать бота у @BotFather, "
            "2) вписать токен в config.json → notify_bot_token, "
            "3) запустить bot_setup.py и написать боту /start."
        )
        return

    # Состояние: что уже обработано (в облаке переживает перезапуски смен)
    load_state()
    first_run = not any(k != "__meta__" for k in state)

    client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        import qrcode as qrc

        log.info("QR-вход: покажите QR-код камерой телефона (Telegram должен быть открыт).")
        print()
        print("=" * 64)
        print("  ВХОД В АККАУНТ")
        print("  1. Откройте Telegram на телефоне")
        print("  2. Наведите камеру на QR-код ниже")
        print("  (или: Настройки -> Устройства -> Подключить десктопное устройство)")
        print("=" * 64)
        qr = await client.qr_login()
        while True:
            qr_gen = qrc.QRCode(border=1)
            qr_gen.add_data(qr.url)
            qr_gen.make()
            for row in qr_gen.get_matrix():
                print("  " + "".join("██" if cell else "  " for cell in row))
            try:
                await qr.wait()
                break
            except asyncio.TimeoutError:
                log.info("QR-код истёк, генерирую новый...")
                await qr.recreate()
        print()

    me = await client.get_me()
    log.info("Подключено как: %s (+%s)", me.first_name, me.phone)
    log.info("Режим: %s.", "облако (смена)" if CLOUD else "локально")
    log.info(
        "Уведомления: основной путь — через бота в чат %s (со звуком).",
        NOTIFY_BOT_CHAT_ID,
    )
    if NOTIFY_FALLBACK:
        log.info("Резервный путь при недоступности бота: «%s».", NOTIFY_FALLBACK)

    # Разбираем резервный чат заранее — чтобы не тратить время при сбое
    await resolve_fallback(client)

    # Проверочное сообщение: локально — при каждом запуске,
    # в облаке — только при самом первом запуске (чтобы не спамить каждые 5 минут)
    if (not CLOUD) or first_run:
        try:
            where = await send_notification(
                client,
                f"✅ Мониторинг запущен.\n"
                f"Папка: «{FOLDER_NAME}»\n"
                f"Ищем: «{KEYWORD_ROOT}» + {', '.join(KEYWORD_TRIGGERS)}\n"
                f"Также само по себе: {', '.join(KEYWORD_ALONE)}\n"
                f"Найденные совпадения будут приходить сюда.",
            )
            log.info("Проверочное сообщение отправлено %s.", where)
        except Exception as e:
            log.error("Не удалось отправить проверочное сообщение: %s", e)
            log.warning("Это не критично — сам мониторинг продолжит работать.")
    else:
        log.info("Проверочное сообщение пропущено (уже работали ранее).")

    log.info(
        "Правила поиска: «%s» + [%s] либо само по себе [%s].",
        KEYWORD_ROOT,
        ", ".join(KEYWORD_TRIGGERS),
        ", ".join(KEYWORD_ALONE),
    )

    await refresh_folders(client)

    # Обработчик «живых» сообщений — регистрируем ДО дозабора,
    # чтобы ничего не потерялось в промежутке.
    @client.on(events.NewMessage)
    async def handler(event: events.NewMessage.Event):
        await process_message(client, event.message, event.chat_id)

    # Дозабираем то, что пропустили, пока не работали (пауза между сменами и т.п.)
    await catch_up(client)

    # Периодически обновляем состав папки (чтобы учитывать новые чаты в ней)
    async def folder_refresher():
        while True:
            await asyncio.sleep(600)
            await refresh_folders(client)

    asyncio.create_task(folder_refresher())

    # «Пульс»: периодический отчёт, что скрипт жив
    async def heartbeat():
        while True:
            await asyncio.sleep(max(1, HEARTBEAT_MINUTES) * 60)
            log.info(
                "Пульс: работаю. Чатов в папке «%s»: %d, проверено сообщений: %d, "
                "совпадений: %d.",
                FOLDER_NAME,
                len(folder_peer_ids),
                STATS["seen"],
                STATS["matches"],
            )
            save_state(push=True)

    asyncio.create_task(heartbeat())

    log.info(
        "Мониторинг запущен. Папка: «%s».%s",
        FOLDER_NAME,
        " Остановка: Ctrl+C в этом окне." if not CLOUD else "",
    )
    log.info("Пульс каждые %d мин.", HEARTBEAT_MINUTES)

    # Держим соединение живым: при обрыве сети — переподключаемся,
    # вместо того чтобы скрипт просто завершался.
    while True:
        try:
            await client.run_until_disconnected()
            log.warning(
                "Соединение разорвано (сеть/сервер Telegram). "
                "Переподключение через 15 секунд..."
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Сбой в работе: %s: %s", type(e).__name__, e)
            log.warning("Переподключение через 15 секунд...")

        await asyncio.sleep(15)
        for attempt in range(1, 31):
            try:
                if not client.is_connected():
                    await client.connect()
                if await client.is_user_authorized():
                    log.info("Переподключено (попытка %d). Продолжаю мониторинг.", attempt)
                    break
                log.error("Сессия разлогинилась — нужен повторный вход (QR).")
                return
            except Exception as e:
                log.warning(
                    "Попытка %d переподключения не удалась: %s. Жду 10 сек...",
                    attempt,
                    e,
                )
                await asyncio.sleep(10)
        else:
            log.error("Не удалось переподключиться за 30 попыток — жду 60 сек и дальше.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
