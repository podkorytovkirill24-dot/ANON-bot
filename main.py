import logging
import os
import sqlite3
from csv import writer
from dataclasses import dataclass
from datetime import datetime
from html import escape
from io import BytesIO, StringIO
from typing import Optional

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    bot_token: str
    admin_chat_ids: tuple[int, ...]
    channel_id: str
    channel_name: str = "Лицей"
    db_path: str = "queue.db"

    @property
    def primary_admin_id(self) -> int:
        return self.admin_chat_ids[0]

    def is_admin(self, user_id: int) -> bool:
        return user_id in self.admin_chat_ids


CAPTION_SUPPORTED_TYPES = {"photo", "video", "audio", "document", "animation"}


class SubmissionStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_chat_id INTEGER NOT NULL,
                    user_message_id INTEGER NOT NULL,
                    user_username TEXT,
                    user_full_name TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    message_text TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    admin_chat_id INTEGER,
                    admin_message_id INTEGER,
                    published_message_id INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    handled_at TEXT
                )
                """
            )
            self._ensure_columns(conn)
            self._normalize_pending_queue(conn)
            conn.commit()

    def _ensure_columns(self, conn: sqlite3.Connection) -> None:
        table_info = conn.execute("PRAGMA table_info(submissions)").fetchall()
        existing_columns = {row["name"] for row in table_info}

        if "message_text" not in existing_columns:
            conn.execute("ALTER TABLE submissions ADD COLUMN message_text TEXT")

    def _normalize_pending_queue(self, conn: sqlite3.Connection) -> None:
        # Keep only one active moderation card after restarts/code updates.
        active_rows = conn.execute(
            """
            SELECT id
            FROM submissions
            WHERE status = 'pending' AND admin_message_id IS NOT NULL
            ORDER BY id ASC
            """
        ).fetchall()

        if len(active_rows) <= 1:
            return

        keep_id = active_rows[0]["id"]
        conn.execute(
            """
            UPDATE submissions
            SET admin_chat_id = NULL,
                admin_message_id = NULL
            WHERE status = 'pending'
              AND admin_message_id IS NOT NULL
              AND id != ?
            """,
            (keep_id,),
        )

    def add_submission(
        self,
        user_chat_id: int,
        user_message_id: int,
        user_username: Optional[str],
        user_full_name: str,
        content_type: str,
        message_text: str,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO submissions (
                    user_chat_id,
                    user_message_id,
                    user_username,
                    user_full_name,
                    content_type,
                    message_text
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_chat_id, user_message_id, user_username, user_full_name, content_type, message_text),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def claim_next_submission_for_admin(self, admin_chat_id: int) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")

            active = conn.execute(
                """
                SELECT id
                FROM submissions
                WHERE status = 'pending' AND admin_message_id IS NOT NULL
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()
            if active is not None:
                conn.commit()
                return None

            row = conn.execute(
                """
                SELECT *
                FROM submissions
                WHERE status = 'pending' AND admin_message_id IS NULL
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                conn.commit()
                return None

            claim = conn.execute(
                """
                UPDATE submissions
                SET admin_chat_id = ?, admin_message_id = -1
                WHERE id = ?
                  AND status = 'pending'
                  AND admin_message_id IS NULL
                """,
                (admin_chat_id, row["id"]),
            )

            if claim.rowcount != 1:
                conn.commit()
                return None

            claimed_row = conn.execute(
                "SELECT * FROM submissions WHERE id = ?",
                (row["id"],),
            ).fetchone()
            conn.commit()
            return claimed_row

    def release_claim(self, submission_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE submissions
                SET admin_chat_id = NULL,
                    admin_message_id = NULL
                WHERE id = ?
                  AND status = 'pending'
                  AND admin_message_id = -1
                """,
                (submission_id,),
            )
            conn.commit()

    def set_admin_message(self, submission_id: int, admin_chat_id: int, admin_message_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE submissions
                SET admin_chat_id = ?, admin_message_id = ?
                WHERE id = ?
                  AND status = 'pending'
                  AND admin_message_id = -1
                """,
                (admin_chat_id, admin_message_id, submission_id),
            )
            conn.commit()
            return cursor.rowcount == 1

    def get_submission(self, submission_id: int) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute("SELECT * FROM submissions WHERE id = ?", (submission_id,))
            return cursor.fetchone()

    def list_submissions_for_export(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                SELECT
                    id,
                    created_at,
                    handled_at,
                    status,
                    user_chat_id,
                    user_username,
                    user_full_name,
                    content_type,
                    message_text,
                    published_message_id
                FROM submissions
                ORDER BY id DESC
                """
            )
            return cursor.fetchall()

    def mark_deleted(self, submission_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE submissions
                SET status = 'deleted', handled_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'pending'
                """,
                (submission_id,),
            )
            conn.commit()
            return cursor.rowcount == 1

    def mark_published(self, submission_id: int, published_message_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE submissions
                SET status = 'published',
                    published_message_id = ?,
                    handled_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'pending'
                """,
                (published_message_id, submission_id),
            )
            conn.commit()
            return cursor.rowcount == 1


def load_config() -> Config:
    # Support .env files saved with UTF-8 BOM (common on Windows editors).
    load_dotenv(encoding="utf-8-sig")

    token = os.getenv("BOT_TOKEN", "").strip()
    admin_chat_ids_raw = os.getenv("ADMIN_CHAT_IDS", "").strip()
    admin_chat_raw = os.getenv("ADMIN_CHAT_ID", "").strip()
    channel_id = os.getenv("CHANNEL_ID", "").strip()
    channel_name = os.getenv("CHANNEL_NAME", "Лицей").strip() or "Лицей"
    db_path = os.getenv("DB_PATH", "queue.db").strip() or "queue.db"

    missing = []
    if not token:
        missing.append("BOT_TOKEN")
    if not admin_chat_ids_raw and not admin_chat_raw:
        missing.append("ADMIN_CHAT_IDS")
    if not channel_id:
        missing.append("CHANNEL_ID")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    admin_chat_ids: list[int] = []
    seen_admin_ids: set[int] = set()

    if admin_chat_ids_raw:
        raw_parts = [part.strip() for part in admin_chat_ids_raw.split(",") if part.strip()]
        if not raw_parts:
            raise RuntimeError("ADMIN_CHAT_IDS must contain one or more integer IDs separated by commas")
        for part in raw_parts:
            try:
                parsed_id = int(part)
            except ValueError as exc:
                raise RuntimeError("ADMIN_CHAT_IDS must contain only integer IDs") from exc
            if parsed_id not in seen_admin_ids:
                seen_admin_ids.add(parsed_id)
                admin_chat_ids.append(parsed_id)
    else:
        try:
            parsed_id = int(admin_chat_raw)
        except ValueError as exc:
            raise RuntimeError("ADMIN_CHAT_ID must be an integer") from exc
        admin_chat_ids.append(parsed_id)

    return Config(
        bot_token=token,
        admin_chat_ids=tuple(admin_chat_ids),
        channel_id=channel_id,
        channel_name=channel_name,
        db_path=db_path,
    )


def get_store(context: ContextTypes.DEFAULT_TYPE) -> SubmissionStore:
    return context.application.bot_data["store"]


def get_config(context: ContextTypes.DEFAULT_TYPE) -> Config:
    return context.application.bot_data["config"]


def choose_dispatch_admin(config: Config, preferred_admin_id: Optional[int] = None) -> int:
    if preferred_admin_id is not None and config.is_admin(preferred_admin_id):
        return preferred_admin_id
    return config.primary_admin_id


def detect_content_type(message: Message) -> str:
    if message.text:
        return "text"
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.audio:
        return "audio"
    if message.document:
        return "document"
    if message.voice:
        return "voice"
    if message.video_note:
        return "video_note"
    if message.animation:
        return "animation"
    if message.sticker:
        return "sticker"
    if message.location:
        return "location"
    if message.contact:
        return "contact"
    if message.poll:
        return "poll"
    if message.venue:
        return "venue"
    return "unknown"


def extract_message_text(message: Message) -> str:
    if message.text:
        return message.text.strip()
    if message.caption:
        return message.caption.strip()
    return ""


def trim_text(value: str, limit: int) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"


def build_sender_line_from_submission(submission: sqlite3.Row) -> str:
    username = submission["user_username"]
    username_part = f"@{escape(username)}" if username else "без username"
    full_name = escape(submission["user_full_name"])
    return f"{full_name} ({username_part}, id={submission['user_chat_id']})"


def build_channel_card(text: str, payload_limit: int) -> str:
    header = "📩 <b>Анонимное сообщение</b>"
    trimmed = trim_text(text, payload_limit)

    if not trimmed:
        return header

    return f"{header}\n\n<pre>{escape(trimmed)}</pre>"


def build_control_text(submission: sqlite3.Row) -> str:
    content_type = submission["content_type"]
    content_text = trim_text(submission["message_text"] or "", 900)

    lines = [
        f"🆕 <b>Новая заявка #{submission['id']}</b>",
        f"🧩 Тип: <code>{escape(content_type)}</code>",
        f"👤 Отправитель: {build_sender_line_from_submission(submission)}",
        "",
    ]
    if content_text:
        lines.append("<b>Текст / подпись:</b>")
        lines.append(f"<pre>{escape(content_text)}</pre>")
    else:
        lines.append("📎 Текста нет. Смотри вложение выше.")

    return "\n".join(lines)


def build_keyboard(submission_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🗑 Удалить", callback_data=f"delete:{submission_id}"),
                InlineKeyboardButton("✅ Опубликовать", callback_data=f"publish:{submission_id}"),
            ]
        ]
    )


async def publish_submission(context: ContextTypes.DEFAULT_TYPE, config: Config, submission: sqlite3.Row) -> int:
    content_type = submission["content_type"]
    text = (submission["message_text"] or "").strip()

    if content_type == "text":
        channel_text = build_channel_card(text, payload_limit=3400)
        published = await context.bot.send_message(
            chat_id=config.channel_id,
            text=channel_text,
            parse_mode=ParseMode.HTML,
        )
        return published.message_id

    if content_type in CAPTION_SUPPORTED_TYPES:
        caption = build_channel_card(text, payload_limit=700)
        published = await context.bot.copy_message(
            chat_id=config.channel_id,
            from_chat_id=submission["user_chat_id"],
            message_id=submission["user_message_id"],
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
        return published.message_id

    # For message types where custom captions are not supported.
    await context.bot.send_message(
        chat_id=config.channel_id,
        text=build_channel_card(text, payload_limit=3400),
        parse_mode=ParseMode.HTML,
    )
    published = await context.bot.copy_message(
        chat_id=config.channel_id,
        from_chat_id=submission["user_chat_id"],
        message_id=submission["user_message_id"],
    )
    return published.message_id


async def dispatch_next_submission(
    context: ContextTypes.DEFAULT_TYPE,
    preferred_admin_id: Optional[int] = None,
) -> None:
    store = get_store(context)
    config = get_config(context)
    dispatch_admin_id = choose_dispatch_admin(config, preferred_admin_id)

    submission = store.claim_next_submission_for_admin(dispatch_admin_id)
    if submission is None:
        return

    preview_message_id: Optional[int] = None
    try:
        preview = await context.bot.copy_message(
            chat_id=dispatch_admin_id,
            from_chat_id=submission["user_chat_id"],
            message_id=submission["user_message_id"],
        )
        preview_message_id = preview.message_id
    except TelegramError:
        logger.exception("Failed to copy submission #%s preview to admin chat", submission["id"])

    try:
        control_message = await context.bot.send_message(
            chat_id=dispatch_admin_id,
            text=build_control_text(submission),
            parse_mode=ParseMode.HTML,
            reply_to_message_id=preview_message_id,
            reply_markup=build_keyboard(submission["id"]),
        )
    except TelegramError:
        logger.exception("Failed to send moderation card for submission #%s", submission["id"])
        store.release_claim(submission["id"])
        return

    updated = store.set_admin_message(submission["id"], dispatch_admin_id, control_message.message_id)
    if not updated:
        logger.warning("Lost claim while setting admin message for submission #%s", submission["id"])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.effective_user is None:
        return

    config = get_config(context)
    if config.is_admin(update.effective_user.id):
        await update.message.reply_text(
            "🛠 Панель админа активна.\n"
            "Команды:\n"
            "• /csv - скачать CSV со всеми заявками"
        )
        await dispatch_next_submission(context, preferred_admin_id=update.effective_user.id)
        return

    await update.message.reply_text(
        f"💌 Отправьте текст, фото, видео, аудио или файл.\n\n"
        f"Сообщение будет опубликовано анонимно в канале {config.channel_name}."
    )


async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    config = get_config(context)
    if not config.is_admin(user.id):
        await message.reply_text("⛔ Команда доступна только админу.")
        return

    store = get_store(context)
    rows = store.list_submissions_for_export()
    if not rows:
        await message.reply_text("📭 Пока нет заявок для выгрузки.")
        return

    csv_stream = StringIO()
    csv_writer = writer(csv_stream, delimiter=";")
    csv_writer.writerow(
        [
            "id",
            "created_at",
            "handled_at",
            "status",
            "sender_id",
            "sender_username",
            "sender_name",
            "content_type",
            "message_text",
            "published_message_id",
        ]
    )

    for row in rows:
        username = f"@{row['user_username']}" if row["user_username"] else ""
        message_text = (row["message_text"] or "").strip() or f"[{row['content_type']}]"
        csv_writer.writerow(
            [
                row["id"],
                row["created_at"],
                row["handled_at"] or "",
                row["status"],
                row["user_chat_id"],
                username,
                row["user_full_name"],
                row["content_type"],
                message_text,
                row["published_message_id"] or "",
            ]
        )

    csv_bytes = BytesIO(csv_stream.getvalue().encode("utf-8-sig"))
    csv_bytes.name = f"anon_submissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    await message.reply_document(
        document=csv_bytes,
        caption=f"📊 Готово: {len(rows)} записей.",
    )


async def handle_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat

    if message is None or chat is None or message.from_user is None:
        return

    if chat.type != "private":
        return

    config = get_config(context)
    if config.is_admin(message.from_user.id):
        return

    store = get_store(context)
    content_type = detect_content_type(message)
    message_text = extract_message_text(message)

    store.add_submission(
        user_chat_id=chat.id,
        user_message_id=message.message_id,
        user_username=message.from_user.username,
        user_full_name=message.from_user.full_name,
        content_type=content_type,
        message_text=message_text,
    )

    await dispatch_next_submission(context)

    await message.reply_text(
        f"✅ Принято! Ожидайте, скоро сообщение появится в канале {config.channel_name}."
    )


async def moderate_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or query.from_user is None or query.data is None or query.message is None:
        return

    config = get_config(context)
    if not config.is_admin(query.from_user.id):
        await query.answer("Кнопки доступны только админу.", show_alert=True)
        return

    action, _, raw_id = query.data.partition(":")
    if action not in {"delete", "publish"} or not raw_id.isdigit():
        await query.answer("Некорректное действие.", show_alert=True)
        return

    submission_id = int(raw_id)
    store = get_store(context)
    submission = store.get_submission(submission_id)

    if submission is None:
        await query.answer("Заявка не найдена.", show_alert=True)
        return

    if submission["status"] != "pending":
        await query.answer("Эта заявка уже обработана.", show_alert=True)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError:
            logger.exception("Failed to remove keyboard for processed submission #%s", submission_id)
        return

    if submission["admin_message_id"] != query.message.message_id:
        await query.answer("Сейчас не очередь этой заявки. Дождитесь показа в очереди.", show_alert=True)
        return

    if action == "delete":
        updated = store.mark_deleted(submission_id)
        if not updated:
            await query.answer("Не удалось обновить статус.", show_alert=True)
            return

        await query.edit_message_text(
            text=(
                f"🗑 Заявка #{submission_id} удалена.\n"
                f"👤 Отправитель: {escape(submission['user_full_name'])} (id={submission['user_chat_id']})"
            ),
            parse_mode=ParseMode.HTML,
        )
        await query.answer("Удалено")
        await dispatch_next_submission(context, preferred_admin_id=query.from_user.id)
        return

    try:
        published_message_id = await publish_submission(context, config, submission)
    except TelegramError:
        logger.exception("Failed to publish submission #%s", submission_id)
        await query.answer("Публикация не удалась. Проверьте права бота в канале.", show_alert=True)
        return

    updated = store.mark_published(submission_id, published_message_id)
    if not updated:
        await query.answer("Ошибка обновления статуса после публикации.", show_alert=True)
        return

    await query.edit_message_text(
        text=(
            f"✅ Заявка #{submission_id} опубликована в канале.\n"
            f"👤 Отправитель: {escape(submission['user_full_name'])} (id={submission['user_chat_id']})"
        ),
        parse_mode=ParseMode.HTML,
    )
    await query.answer("Опубликовано")
    await dispatch_next_submission(context, preferred_admin_id=query.from_user.id)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error while processing update: %s", update, exc_info=context.error)


def main() -> None:
    config = load_config()
    store = SubmissionStore(config.db_path)
    store.init()

    application = Application.builder().token(config.bot_token).build()
    application.bot_data["config"] = config
    application.bot_data["store"] = store

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler(["csv", "export"], export_csv))
    application.add_handler(CallbackQueryHandler(moderate_submission, pattern=r"^(delete|publish):\d+$"))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_submission))
    application.add_error_handler(error_handler)

    logger.info("Bot started")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
