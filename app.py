"""
Авеню: Система Заказов
v4.4 — безопасный рефакторинг без изменения UI/логики.
Сверх-осторожная версия: точечные обновления только нужных ячеек, время записи по МСК без авто-конвертации Google Sheets.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import extra_streamlit_components as stx
import gspread
import pandas as pd
import streamlit as st
from google.oauth2.credentials import Credentials
from streamlit_autorefresh import st_autorefresh


# ── КОНСТАНТЫ ─────────────────────────────────────────────────────────────────
SHEET_ID                = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME                = "Заказы ИМ Авеню"
STATE_TAB_NAME          = "avenue_state"

PZ_LIST                 = frozenset(["ПЗ Пекин", "ПЗ Горбушка"])
START_ROW               = 26596

TRUE_VAL                = "TRUE"
FALSE_VAL               = "FALSE"
CANCELLED_VAL           = "Отменён"

COOKIE_NAME             = "avenue_auth_status"
COOKIE_DAYS             = 30

REFRESH_MS              = 600_000
PREVIEW_ORDERS          = 50
STATE_SYNC_TTL          = 15

REPORT_PAGE_KEY         = "report_page_open"
REPORT_DATE_COL         = "Дата и время сбора заказа"

STORE_GORB              = "Горбушка"
STORE_PEKIN             = "Пекин"
STORE_TIK               = "ТИК"

MSK_TZ                  = ZoneInfo("Europe/Moscow")

_PEKIN_KEYWORDS = ("пек", "пкн", "pekin")
_GORB_KEYWORDS  = ("горб", "грб", "gorb")

MENU_OPTIONS = [
    "🏪 Магазин: ГОРБУШКА",
    "🏪 Магазин: ПЕКИН",
    "🚚 Перемещения (Активные)",
    "⏳ Товар Под заказ",
    "✅ Выполненные сборки",
    "🚫 Отмененные заказы",
]

STATE_HEADERS = ["local_in_work", "reviewed_changes", "confirmed_cancels", "completed_log"]


# ── НАСТРОЙКА СТРАНИЦЫ ────────────────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")


# ── СТРУКТУРЫ ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ColumnMap:
    ORDER: int
    PRODUCT: int
    QTY: int
    WH: int
    COMMENT: int
    EDIT: int
    INWORK: int
    MOVE: int
    DONE: int
    STATUS: int
    REPORT_DT: int

    def as_dict(self) -> dict[str, int]:
        return {
            "ORDER": self.ORDER,
            "PRODUCT": self.PRODUCT,
            "QTY": self.QTY,
            "WH": self.WH,
            "COMMENT": self.COMMENT,
            "EDIT": self.EDIT,
            "INWORK": self.INWORK,
            "MOVE": self.MOVE,
            "DONE": self.DONE,
            "STATUS": self.STATUS,
            "REPORT_DT": self.REPORT_DT,
        }


# ── УТИЛИТЫ ───────────────────────────────────────────────────────────────────
def _now() -> datetime:
    return datetime.now(MSK_TZ)


def _sheet_datetime_now() -> str:
    return _now().strftime("%d.%m.%Y %H:%M:%S")


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _normalized_str(value: Any) -> str:
    return _safe_str(value).strip()


def _get_secret(name: str, default: str | None = None) -> str:
    try:
        return str(st.secrets[name])
    except Exception:
        if default is not None:
            return default
        st.error(f"Критическая ошибка: отсутствует secret «{name}».")
        st.stop()


def _ensure_row_width(row: list[str], width: int) -> list[str]:
    if len(row) >= width:
        return row[:width]
    return row + [""] * (width - len(row))


def _a1_col(col_num_1based: int) -> str:
    result = ""
    n = col_num_1based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _review_key(oid: Any, edit_text: str) -> str:
    return f"{_safe_str(oid)}||{_normalized_str(edit_text)}"


# ── АВТОРИЗАЦИЯ ───────────────────────────────────────────────────────────────
def _cookie_signing_key() -> str:
    raw = _get_secret("password")
    pepper = _get_secret("cookie_sign_salt", "avenue_cookie_default_salt")
    return hashlib.sha256(f"{raw}::{pepper}".encode("utf-8")).hexdigest()


def _make_signed_cookie() -> str:
    expires_at = int((_now() + timedelta(days=COOKIE_DAYS)).timestamp())
    payload = {"exp": expires_at, "v": "authorized"}
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("utf-8")
    sig = hmac.new(
        _cookie_signing_key().encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload_b64}.{sig}"


def _verify_signed_cookie(token: str | None) -> bool:
    if not token or "." not in str(token):
        return False
    try:
        payload_b64, sig = str(token).rsplit(".", 1)
        expected_sig = hmac.new(
            _cookie_signing_key().encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return False

        payload_json = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        payload = json.loads(payload_json)
        if payload.get("v") != "authorized":
            return False
        if int(payload.get("exp", 0)) < int(_now().timestamp()):
            return False
        return True
    except Exception:
        return False


if "cookie_manager" not in st.session_state:
    st.session_state.cookie_manager = stx.CookieManager(key="avenue_auth_manager_v4")

cookie_manager: stx.CookieManager = st.session_state.cookie_manager


def check_password() -> bool:
    if st.session_state.get("password_correct"):
        return True

    cookies = cookie_manager.get_all()
    cookie_value = cookies.get(COOKIE_NAME) if cookies else None
    if _verify_signed_cookie(cookie_value):
        st.session_state.password_correct = True
        return True

    st.title("🔐 Вход в систему")

    if "password" not in st.secrets:
        st.error("Критическая ошибка: Пароль не настроен в Secrets.")
        st.stop()

    pwd = st.text_input("Введите код доступа:", type="password", key="login_input")
    remember = st.checkbox("Запомнить меня на этом устройстве", value=True)

    if st.button("Войти", key="login_btn", type="primary"):
        if hmac.compare_digest(pwd, str(st.secrets["password"])):
            st.session_state.password_correct = True
            if remember:
                cookie_manager.set(
                    COOKIE_NAME,
                    _make_signed_cookie(),
                    expires_at=_now() + timedelta(days=COOKIE_DAYS),
                )
            st.rerun()
        else:
            st.error("❌ Неверный код")
    return False


if not check_password():
    st.stop()


# ── GOOGLE SHEETS: РЕСУРСЫ ────────────────────────────────────────────────────
@st.cache_resource
def get_gspread_client() -> gspread.Client:
    try:
        gs = st.secrets["connections"]["gsheets"]
        required = ("client_id", "client_secret", "refresh_token")
        missing = [k for k in required if k not in gs or not gs[k]]
        if missing:
            st.error(f"В Secrets отсутствуют параметры gsheets: {', '.join(missing)}")
            st.stop()

        creds = Credentials.from_authorized_user_info(
            {
                "client_id": gs["client_id"],
                "client_secret": gs["client_secret"],
                "refresh_token": gs["refresh_token"],
                "type": "authorized_user",
            },
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        return gspread.authorize(creds)

    except KeyError as e:
        st.error(f"Отсутствует ключ в Secrets: {e}")
        st.stop()
    except Exception as e:
        st.error(f"Ошибка авторизации Google: {e}")
        st.stop()


@st.cache_resource
def get_spreadsheet():
    try:
        return get_gspread_client().open_by_key(SHEET_ID)
    except Exception as e:
        st.error(f"Не удалось открыть таблицу Google Sheets: {e}")
        st.stop()


@st.cache_resource
def get_orders_worksheet() -> gspread.Worksheet:
    ss = get_spreadsheet()
    try:
        return ss.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return ss.get_worksheet(0)


@st.cache_resource
def get_state_worksheet() -> gspread.Worksheet:
    ss = get_spreadsheet()
    try:
        return ss.worksheet(STATE_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=STATE_TAB_NAME, rows=5000, cols=4)
        ws.update([STATE_HEADERS], value_input_option="RAW")
        return ws


# ── СОСТОЯНИЕ В GOOGLE SHEETS ─────────────────────────────────────────────────
@st.cache_data(ttl=STATE_SYNC_TTL)
def load_state_from_sheets() -> dict[str, Any]:
    try:
        rows = get_state_worksheet().get_all_values()
        if len(rows) < 2:
            return {"in_work": set(), "reviewed": set(), "confirmed": set(), "log": []}

        data = rows[1:]
        result = {
            "in_work": set(),
            "reviewed": set(),
            "confirmed": set(),
            "log": [],
        }

        for row in data:
            row = _ensure_row_width(row, 4)
            if _normalized_str(row[0]):
                result["in_work"].add(_normalized_str(row[0]))
            if _normalized_str(row[1]):
                result["reviewed"].add(_normalized_str(row[1]))
            if _normalized_str(row[2]):
                result["confirmed"].add(_normalized_str(row[2]))
            if _normalized_str(row[3]):
                result["log"].append(_normalized_str(row[3]))

        return result
    except Exception as e:
        st.warning(f"Не удалось загрузить состояние из Sheets: {e}")
        return {"in_work": set(), "reviewed": set(), "confirmed": set(), "log": []}


def save_state_to_sheets() -> None:
    try:
        in_work   = sorted(_safe_str(x) for x in st.session_state.local_in_work if _normalized_str(x))
        reviewed  = sorted(_safe_str(x) for x in st.session_state.reviewed_changes if _normalized_str(x))
        confirmed = sorted(_safe_str(x) for x in st.session_state.confirmed_cancels if _normalized_str(x))
        log       = [_safe_str(x) for x in st.session_state.completed_log if _normalized_str(x)]

        max_len = max(len(in_work), len(reviewed), len(confirmed), len(log), 1)

        def pad(lst: list[str]) -> list[str]:
            return lst + [""] * (max_len - len(lst))

        rows = [STATE_HEADERS]
        rows += list(map(list, zip(pad(in_work), pad(reviewed), pad(confirmed), pad(log))))

        ws = get_state_worksheet()
        end_row = len(rows)

        ws.update(f"A1:D{end_row}", rows, value_input_option="RAW")

        existing_rows = ws.row_count
        if existing_rows > end_row:
            ws.batch_clear([f"A{end_row + 1}:D{existing_rows}"])

        load_state_from_sheets.clear()
    except Exception as e:
        st.warning(f"Не удалось сохранить состояние в Sheets: {e}")


def _sync_runtime_state() -> None:
    state = load_state_from_sheets()
    st.session_state.local_in_work = set(_safe_str(x) for x in state["in_work"])
    st.session_state.reviewed_changes = set(_safe_str(x) for x in state["reviewed"])
    st.session_state.confirmed_cancels = set(_safe_str(x) for x in state["confirmed"])
    st.session_state.completed_log = list(state["log"])


# ── ЗАГРУЗКА ДАННЫХ ЗАКАЗОВ ───────────────────────────────────────────────────
def _find_header_row(raw_data: list[list[str]]) -> int:
    return next(
        (
            i for i, row in enumerate(raw_data[:100])
            if "Наименование" in row and "Склад" in row
        ),
        -1,
    )


def _build_column_map(headers: list[str]) -> ColumnMap:
    def col_idx(name: str) -> int:
        try:
            return headers.index(name)
        except ValueError:
            raise ValueError(f"Колонка «{name}» не найдена в таблице.")

    status_idx = headers.index("Статус") if "Статус" in headers else len(headers) - 1

    col_map = ColumnMap(
        ORDER=col_idx("Наименование") - 1,
        PRODUCT=col_idx("Наименование"),
        QTY=col_idx("Кол-во"),
        WH=col_idx("Склад"),
        COMMENT=col_idx("Комментарий"),
        EDIT=col_idx("Изменения заказа"),
        INWORK=col_idx("Под ЗАКАЗ"),
        MOVE=col_idx("Перемещение"),
        DONE=col_idx("Собрано"),
        STATUS=status_idx,
        REPORT_DT=col_idx(REPORT_DATE_COL),
    )

    if col_map.ORDER < 0:
        raise ValueError("Ошибка структуры таблицы: колонка заказа должна стоять перед «Наименование».")

    return col_map


@st.cache_data(ttl=3600)
def load_data() -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Загружает данные из листа заказов.
    Интерфейс и бизнес-логика сохранены как были.
    """
    try:
        sheet = get_orders_worksheet()
        raw_data = sheet.get_all_values()
    except Exception as e:
        st.error(f"Ошибка чтения листа заказов: {e}")
        return pd.DataFrame(), {}

    if not raw_data:
        return pd.DataFrame(), {}

    header_idx = _find_header_row(raw_data)
    if header_idx == -1:
        return pd.DataFrame(), {}

    headers = [_safe_str(h).strip().replace("\n", " ") for h in raw_data[header_idx]]
    headers_len = len(headers)

    try:
        col_map = _build_column_map(headers)
    except ValueError as e:
        st.error(str(e))
        return pd.DataFrame(), {}

    data_start = max(START_ROW - 1, header_idx + 1)
    rows = [_ensure_row_width(row, headers_len) for row in raw_data[data_start:]]

    if not rows:
        return pd.DataFrame(), {}

    df = pd.DataFrame(rows, columns=headers)
    df["_sheet_row"] = range(data_start + 1, data_start + 1 + len(df))

    product_col = headers[col_map.PRODUCT]
    df = df[df[product_col].fillna("").astype(str).str.strip().astype(bool)].copy()

    st.session_state.last_sync = _now().strftime("%H:%M:%S")
    return df, col_map.as_dict()


# ── ТОЧЕЧНЫЕ ОБНОВЛЕНИЯ В SHEETS ──────────────────────────────────────────────
def _build_batch_payload_for_rows(
    row_numbers: list[int],
    col_num_0based: int,
    value: str,
) -> list[dict[str, Any]]:
    col_letter = _a1_col(col_num_0based + 1)
    return [{"range": f"{col_letter}{row_num}", "values": [[value]]} for row_num in row_numbers]


def update_sheet_cells(group: pd.DataFrame, col_map: dict[str, int], updates: dict[str, str]) -> None:
    """
    Аккуратно обновляет только целевые ячейки.
    Не трогает другие диапазоны.
    """
    try:
        sheet = get_orders_worksheet()
        row_numbers = [int(x) for x in group["_sheet_row"].tolist()]
        batch_payload: list[dict[str, Any]] = []

        for key, val in updates.items():
            if key not in col_map:
                raise KeyError(f"Неизвестный ключ обновления: {key}")

            col_num_0based = int(col_map[key])
            batch_payload.extend(_build_batch_payload_for_rows(row_numbers, col_num_0based, val))

        if batch_payload:
            sheet.batch_update(batch_payload, value_input_option="RAW")
            load_data.clear()

    except Exception as e:
        st.warning(f"Не удалось обновить данные в Sheets: {e}")


def write_report_datetime(group: pd.DataFrame, col_map: dict[str, int], dt_value: str) -> None:
    """
    Записывает дату/время строго в найденную по заголовку колонку отчёта.
    Время всегда по МСК и сохраняется БЕЗ авто-конвертации Google Sheets.
    """
    try:
        sheet = get_orders_worksheet()
        row_numbers = [int(x) for x in group["_sheet_row"].tolist()]
        report_col = int(col_map["REPORT_DT"])

        batch_payload = _build_batch_payload_for_rows(row_numbers, report_col, dt_value)

        if batch_payload:
            sheet.batch_update(batch_payload, value_input_option="RAW")
            load_data.clear()

    except Exception as e:
        st.warning(f"Не удалось записать дату сборки: {e}")


# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────────────────
def identify_target_store(comment: str) -> str:
    c = _safe_str(comment).lower()
    if "d" in c:
        return STORE_GORB
    if any(k in c for k in _PEKIN_KEYWORDS):
        return STORE_PEKIN
    if any(k in c for k in _GORB_KEYWORDS):
        return STORE_GORB
    return "Общий"


def is_delivery(comment: str) -> bool:
    return "d" in _safe_str(comment).lower()


def _build_tags(
    comment_str: str,
    is_move_needed: bool,
    has_edit: bool,
    is_pz_item: bool,
    incoming: bool,
    is_cancelled: bool = False,
    in_work_section: bool = False,
    is_tik_pending: bool = False,
) -> str:
    tags: list[str] = []
    if is_cancelled:
        tags.append("🚫 ОТМЕНА")
    if is_delivery(comment_str):
        tags.append("📦 ДОСТАВКА")
    if is_tik_pending:
        tags.append("🏭 ТИК → ждёт отправки")
    if is_move_needed:
        tags.append("🚚 ПЕРЕМЕЩЕНИЕ")
    if has_edit:
        tags.append("⚠️ ПРАВКА" if in_work_section else "⚠️ ИЗМЕНЕНИЕ")
    if is_pz_item:
        tags.append("⏳ ПЗ")
    if incoming:
        tags.append("🚚 ЕДЕТ")
    return " | ".join(tags)


def _log_action(oid: Any, group: pd.DataFrame, store: str, action: str) -> None:
    ts = _now().strftime("%Y-%m-%d %H:%M")
    products = ";".join(group[C_PRODUCT].astype(str))
    qtys = ";".join(group[C_QTY].astype(str))
    st.session_state.completed_log.append(f"{ts}|{oid}|{products}|{qtys}|{store}|{action}")


def render_order_table(group: pd.DataFrame) -> None:
    st.table(group[TABLE_COLS].rename(columns=COL_RENAME))


def _to_numeric_qty(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace("\xa0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)


def _prepare_sales_report_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Для отчёта берём только собранные и не отменённые позиции.
    Дата продажи = 'Дата и время сбора заказа'.
    """
    if REPORT_DATE_COL not in df.columns:
        return pd.DataFrame()

    report_df = df.copy()
    report_df["_qty_num"] = _to_numeric_qty(report_df[C_QTY])
    report_df["_dt"] = pd.to_datetime(
        report_df[REPORT_DATE_COL].fillna("").astype(str).str.strip(),
        errors="coerce",
        dayfirst=True,
    )

    report_df = report_df[
        (report_df[C_DONE] == TRUE_VAL)
        & ~report_df["_is_cancelled"]
        & report_df["_dt"].notna()
        & report_df[C_PRODUCT].fillna("").astype(str).str.strip().astype(bool)
    ].copy()

    return report_df


def _build_period_report(report_df: pd.DataFrame, start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> pd.DataFrame:
    period_df = report_df[
        (report_df["_dt"] >= start_dt)
        & (report_df["_dt"] < end_dt)
    ].copy()

    if period_df.empty:
        return pd.DataFrame(columns=["Товар", "Продано"])

    result = (
        period_df.groupby(C_PRODUCT, dropna=False)["_qty_num"]
        .sum()
        .reset_index()
        .rename(columns={C_PRODUCT: "Товар", "_qty_num": "Продано"})
        .sort_values(["Продано", "Товар"], ascending=[False, True])
        .reset_index(drop=True)
    )

    result["Продано"] = result["Продано"].apply(
        lambda x: int(x) if float(x).is_integer() else round(float(x), 3)
    )
    return result


def render_report() -> None:
    st.title("📊 Отчёт по продажам")

    report_df = _prepare_sales_report_df(work_base)

    if REPORT_DATE_COL not in work_base.columns:
        st.warning(f"В таблице не найден столбец «{REPORT_DATE_COL}».")
        return

    st.caption(f"Отчёт строится по столбцу: **{REPORT_DATE_COL}**")

    now_ts = pd.Timestamp(_now().replace(tzinfo=None))
    today_start = now_ts.normalize()
    tomorrow_start = today_start + pd.Timedelta(days=1)

    week_start = today_start - pd.Timedelta(days=today_start.weekday())
    next_week_start = week_start + pd.Timedelta(days=7)

    month_start = today_start.replace(day=1)
    next_month_start = month_start + pd.offsets.MonthBegin(1)

    day_report = _build_period_report(report_df, today_start, tomorrow_start)
    week_report = _build_period_report(report_df, week_start, next_week_start)
    month_report = _build_period_report(report_df, month_start, next_month_start)

    tabs = st.tabs(["За день", "За неделю", "За месяц"])

    with tabs[0]:
        st.subheader("Продано за сегодня")
        if day_report.empty:
            st.info("За сегодня продаж нет.")
        else:
            st.dataframe(day_report, use_container_width=True, hide_index=True)

    with tabs[1]:
        st.subheader("Продано за текущую неделю")
        if week_report.empty:
            st.info("За текущую неделю продаж нет.")
        else:
            st.dataframe(week_report, use_container_width=True, hide_index=True)

    with tabs[2]:
        st.subheader("Продано за текущий месяц")
        if month_report.empty:
            st.info("За текущий месяц продаж нет.")
        else:
            st.dataframe(month_report, use_container_width=True, hide_index=True)


# ── АВТООБНОВЛЕНИЕ ────────────────────────────────────────────────────────────
st.session_state.setdefault("auto_refresh_enabled", True)

refresh_count = (
    st_autorefresh(interval=REFRESH_MS, key="data_refresh")
    if st.session_state.auto_refresh_enabled
    else 0
)


# ── ИНИЦИАЛИЗАЦИЯ СЕССИИ ──────────────────────────────────────────────────────
if "session_initialized" not in st.session_state:
    _sync_runtime_state()
    st.session_state.prev_order_ids = set()
    st.session_state.new_orders_alert = set()
    st.session_state.new_orders_alert_time = None
    st.session_state.last_sync = "Не обновлялось"
    st.session_state[REPORT_PAGE_KEY] = False
    st.session_state.session_initialized = True
else:
    _sync_runtime_state()

if refresh_count > 0:
    load_data.clear()


# ── ЗАГРУЗКА И ПОДГОТОВКА ДАННЫХ ──────────────────────────────────────────────
df_mem, C = load_data()

if df_mem.empty or not C:
    st.error("Не удалось загрузить данные. Проверьте таблицу и настройки.")
    st.stop()

df_mem = df_mem.copy()

C_ORDER   = "__order__"
C_PRODUCT = "__product__"
C_QTY     = "__qty__"
C_WH      = "__wh__"
C_COMMENT = "__comment__"
C_DONE    = "__done__"
C_MOVE    = "__move__"
C_STATUS  = "__status__"
C_EDIT    = "__edit__"
C_INWORK  = "__inwork__"

df_mem[C_ORDER]   = df_mem.iloc[:, C["ORDER"]].fillna("").astype(str)
df_mem[C_PRODUCT] = df_mem.iloc[:, C["PRODUCT"]].fillna("").astype(str)
df_mem[C_QTY]     = df_mem.iloc[:, C["QTY"]].fillna("").astype(str)
df_mem[C_WH]      = df_mem.iloc[:, C["WH"]].fillna("").astype(str)
df_mem[C_COMMENT] = df_mem.iloc[:, C["COMMENT"]].fillna("").astype(str)
df_mem[C_DONE]    = df_mem.iloc[:, C["DONE"]].fillna("").astype(str)
df_mem[C_MOVE]    = df_mem.iloc[:, C["MOVE"]].fillna("").astype(str)
df_mem[C_STATUS]  = df_mem.iloc[:, C["STATUS"]].fillna("").astype(str)
df_mem[C_EDIT]    = df_mem.iloc[:, C["EDIT"]].fillna("").astype(str)
df_mem[C_INWORK]  = df_mem.iloc[:, C["INWORK"]].fillna("").astype(str)

TABLE_COLS = [C_ORDER, C_PRODUCT, C_QTY, C_WH, C_COMMENT]
COL_RENAME = {
    C_ORDER: "Заказ",
    C_PRODUCT: "Товар",
    C_QTY: "Кол",
    C_WH: "Склад",
    C_COMMENT: "Коммент",
}

current_order_ids = set(df_mem[C_ORDER].dropna().astype(str).unique())
if st.session_state.prev_order_ids:
    new_ids = current_order_ids - st.session_state.prev_order_ids
    if new_ids:
        st.session_state.new_orders_alert = new_ids
        st.session_state.new_orders_alert_time = _now()
st.session_state.prev_order_ids = current_order_ids

work_base = df_mem.copy()
work_base["_target_store"] = work_base[C_COMMENT].apply(identify_target_store)
work_base["_is_cancelled"] = (
    work_base[C_STATUS].astype(str).str.strip().str.lower() == CANCELLED_VAL.lower()
)


# ── САЙДБАР ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏢 Меню Авеню")

st.session_state.auto_refresh_enabled = st.sidebar.toggle(
    "🔄 Автообновление (10 мин)",
    value=st.session_state.auto_refresh_enabled,
    key="auto_refresh_toggle",
)

menu = st.sidebar.selectbox("Выберите раздел:", MENU_OPTIONS)

if st.sidebar.button("🚪 Выйти"):
    cookie_manager.delete(COOKIE_NAME)
    st.session_state.password_correct = False
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"🔄 Последняя синхронизация: **{st.session_state.last_sync}**")

if st.sidebar.button("🔃 Обновить данные сейчас"):
    load_data.clear()
    load_state_from_sheets.clear()
    st.rerun()

st.sidebar.markdown("---")
if st.sidebar.button("📊 Отчёт", use_container_width=True):
    st.session_state[REPORT_PAGE_KEY] = True
    st.rerun()


# ── ЛОГИКА МАГАЗИНА ───────────────────────────────────────────────────────────
def render_store(current_store: str) -> None:
    st.title(f"🏪 Заказы: {current_store}")

    wh_pattern = "Горб|Сток" if current_store == STORE_GORB else "Пекин"

    store_order_ids = set(
        work_base[
            work_base[C_WH].str.contains(wh_pattern, case=False, na=False)
            & ~work_base[C_WH].isin(PZ_LIST)
        ][C_ORDER].astype(str).unique()
    )

    store_new_alert = st.session_state.new_orders_alert & store_order_ids
    alert_time = st.session_state.get("new_orders_alert_time")
    if store_new_alert and alert_time and (_now() - alert_time).total_seconds() < 10:
        st.success(f"🆕 Новые заказы: {', '.join(sorted(str(o) for o in store_new_alert))}")

    is_pz_row = work_base[C_WH].isin(PZ_LIST)
    is_move = work_base[C_MOVE] == TRUE_VAL
    wh_match = work_base[C_WH].str.contains(wh_pattern, case=False, na=False)

    is_f_match = wh_match & ~is_pz_row
    is_pz_match = (
        (work_base[C_WH] == f"ПЗ {current_store}")
        & (work_base[C_INWORK] == TRUE_VAL)
    )
    is_incoming = is_move & (work_base["_target_store"] == current_store)
    is_tik_pending = (
        (work_base[C_WH].str.strip() == STORE_TIK)
        & ~is_move
        & (work_base["_target_store"] == current_store)
    )

    confirmed_set = {str(x) for x in st.session_state.confirmed_cancels}

    is_cancelled_unconfirmed = (
        work_base["_is_cancelled"]
        & (is_f_match | is_pz_match | is_incoming | is_tik_pending)
        & ~work_base[C_ORDER].astype(str).isin(confirmed_set)
    )

    base_mask = ((is_f_match | is_pz_match) & ~is_move) | is_incoming | is_tik_pending

    reviewed = st.session_state.reviewed_changes
    edit_series = work_base[C_EDIT].fillna("").astype(str).str.strip()
    order_series = work_base[C_ORDER].astype(str)

    review_keys = pd.Series(
        (_review_key(oid, et) for oid, et in zip(order_series, edit_series)),
        index=work_base.index,
    )
    has_unrev = edit_series.astype(bool) & ~review_keys.isin(reviewed)

    not_confirmed_cancelled = ~(
        work_base["_is_cancelled"]
        & work_base[C_ORDER].astype(str).isin(confirmed_set)
    )

    display_df = work_base[
        (base_mask & ((work_base[C_DONE] != TRUE_VAL) | has_unrev) & not_confirmed_cancelled)
        | is_cancelled_unconfirmed
    ].copy()
    display_df["_is_tik_pending"] = is_tik_pending.reindex(display_df.index, fill_value=False)

    in_work_ids = {str(x) for x in st.session_state.local_in_work}

    def _render_order(oid: Any, group: pd.DataFrame, in_work_section: bool) -> None:
        oid_str = _safe_str(oid)
        comment_str = _safe_str(group[C_COMMENT].iloc[0])
        target = _safe_str(group["_target_store"].iloc[0])
        incoming = _safe_str(group[C_MOVE].iloc[0]) == TRUE_VAL
        is_pz_item = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
        edit_text = _safe_str(group[C_EDIT].iloc[0])
        rk = _review_key(oid_str, edit_text)
        has_edit = bool(edit_text.strip()) and rk not in reviewed
        is_move_needed = target != current_store and target != "Общий" and not incoming
        cancelled = bool(group["_is_cancelled"].iloc[0])
        tik_pending = bool(group["_is_tik_pending"].iloc[0])

        tag_str = _build_tags(
            comment_str=comment_str,
            is_move_needed=is_move_needed,
            has_edit=has_edit,
            is_pz_item=is_pz_item,
            incoming=incoming,
            is_cancelled=cancelled,
            in_work_section=in_work_section,
            is_tik_pending=tik_pending,
        )
        label = f"Заказ №{oid_str}{f'  [{tag_str}]' if tag_str else ''}"

        with st.expander(label):
            if cancelled:
                st.error("🚫 Этот заказ отменён")
            if tik_pending:
                st.info(f"🏭 Товар на складе ТИК — требуется отправка перемещения в {current_store}")
            if has_edit:
                prefix = "Правка" if in_work_section else "Изменение"
                st.error(f"{prefix}: {edit_text}")

            render_order_table(group)

            if cancelled:
                if st.button(
                    "✅ Подтвердить отмену",
                    key=f"confirm_cancel_{oid_str}",
                    type="primary",
                    use_container_width=True,
                ):
                    update_sheet_cells(group, C, {"DONE": TRUE_VAL})
                    st.session_state.confirmed_cancels.add(oid_str)
                    st.session_state.local_in_work.discard(oid_str)
                    _log_action(oid_str, group, current_store, "cancel_confirmed")
                    save_state_to_sheets()
                    st.rerun()

            elif has_edit:
                btn = "Учесть правку" if in_work_section else "Учесть Изменение"
                if st.button(btn, key=f"rev_{'w' if in_work_section else 'n'}_{oid_str}"):
                    st.session_state.reviewed_changes.add(rk)
                    save_state_to_sheets()
                    st.rerun()

            elif tik_pending:
                if st.button(
                    "🚛 Подтвердить перемещение с ТИК",
                    key=f"tik_mv_{oid_str}",
                    type="primary",
                    use_container_width=True,
                ):
                    update_sheet_cells(group, C, {"MOVE": TRUE_VAL})
                    st.rerun()

                st.markdown("---")
                if not in_work_section:
                    if st.button("В работу", key=f"w_{oid_str}", use_container_width=True):
                        st.session_state.local_in_work.add(oid_str)
                        save_state_to_sheets()
                        st.rerun()
                else:
                    if st.button(
                        "✅ ПРИНЯТО И СОБРАНО",
                        key=f"dn_tik_{oid_str}",
                        type="primary",
                        use_container_width=True,
                    ):
                        dt_now = _sheet_datetime_now()
                        update_sheet_cells(
                            group,
                            C,
                            {
                                "DONE": TRUE_VAL,
                                "MOVE": FALSE_VAL,
                            },
                        )
                        write_report_datetime(group, C, dt_now)
                        st.session_state.local_in_work.discard(oid_str)
                        _log_action(oid_str, group, current_store, "done")
                        save_state_to_sheets()
                        st.rerun()

            elif in_work_section:
                if is_move_needed:
                    if st.button(
                        "🚛 ОТПРАВИТЬ ПЕРЕМЕЩЕНИЕ",
                        key=f"mv_{oid_str}",
                        type="primary",
                        use_container_width=True,
                    ):
                        update_sheet_cells(group, C, {"MOVE": TRUE_VAL})
                        st.session_state.local_in_work.discard(oid_str)
                        save_state_to_sheets()
                        st.rerun()
                else:
                    action_label = "✅ ПРИНЯТО И СОБРАНО" if incoming else "✅ ЗАВЕРШИТЬ СБОРКУ"
                    if st.button(
                        action_label,
                        key=f"dn_{oid_str}",
                        type="primary",
                        use_container_width=True,
                    ):
                        dt_now = _sheet_datetime_now()
                        update_sheet_cells(
                            group,
                            C,
                            {
                                "DONE": TRUE_VAL,
                                "MOVE": FALSE_VAL,
                            },
                        )
                        write_report_datetime(group, C, dt_now)
                        st.session_state.local_in_work.discard(oid_str)
                        st.session_state.reviewed_changes.discard(rk)
                        _log_action(oid_str, group, current_store, "done")
                        save_state_to_sheets()
                        st.rerun()

                    st.markdown("---")
                    if st.button("🚫 Отменить заказ", key=f"cancel_{oid_str}", use_container_width=True):
                        update_sheet_cells(group, C, {"STATUS": CANCELLED_VAL})
                        st.session_state.local_in_work.discard(oid_str)
                        save_state_to_sheets()
                        st.rerun()

            else:
                if st.button("В работу", key=f"w_{oid_str}", use_container_width=True):
                    st.session_state.local_in_work.add(oid_str)
                    save_state_to_sheets()
                    st.rerun()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🆕 Новые / Изменения")
        left_df = display_df[~display_df[C_ORDER].astype(str).isin(in_work_ids)]
        for oid, group in left_df.groupby(C_ORDER, sort=False):
            _render_order(oid, group, in_work_section=False)

    with col2:
        st.subheader("🛠 В сборке")
        right_df = display_df[display_df[C_ORDER].astype(str).isin(in_work_ids)]
        for oid, group in right_df.groupby(C_ORDER, sort=False):
            _render_order(oid, group, in_work_section=True)


# ── МАРШРУТИЗАЦИЯ ─────────────────────────────────────────────────────────────
if st.session_state.get(REPORT_PAGE_KEY):
    render_report()
    if st.sidebar.button("⬅️ Назад", use_container_width=True):
        st.session_state[REPORT_PAGE_KEY] = False
        st.rerun()

elif "Магазин" in menu:
    render_store(STORE_GORB if "ГОРБУШКА" in menu else STORE_PEKIN)

elif menu == "🚚 Перемещения (Активные)":
    st.title("🚚 В пути")
    for oid, group in work_base[work_base[C_MOVE] == TRUE_VAL].groupby(C_ORDER, sort=False):
        with st.expander(f"Перемещение №{oid} ⮕ {group['_target_store'].iloc[0]}"):
            render_order_table(group)
            if st.button("Сбросить статус перемещения", key=f"cl_mv_{oid}"):
                update_sheet_cells(group, C, {"MOVE": FALSE_VAL})
                st.rerun()

elif menu == "⏳ Товар Под заказ":
    st.title("⏳ Ожидание поступления (ПЗ)")
    pz = work_base[
        work_base[C_WH].isin(PZ_LIST)
        & (work_base[C_INWORK] != TRUE_VAL)
        & (work_base[C_DONE] != TRUE_VAL)
        & ~work_base["_is_cancelled"]
    ]
    st.dataframe(
        pz[TABLE_COLS].rename(columns=COL_RENAME),
        use_container_width=True,
        hide_index=True,
    )

elif menu == "✅ Выполненные сборки":
    st.title("✅ Последние собранные")
    done = work_base[
        (work_base[C_DONE] == TRUE_VAL) & ~work_base["_is_cancelled"]
    ].iloc[::-1].head(PREVIEW_ORDERS)
    st.dataframe(
        done[TABLE_COLS].rename(columns=COL_RENAME),
        use_container_width=True,
        hide_index=True,
    )

elif menu == "🚫 Отмененные заказы":
    st.title("🚫 Отменённые (подтверждённые)")
    confirmed_set = {str(x) for x in st.session_state.confirmed_cancels}
    cancelled = work_base[
        work_base["_is_cancelled"]
        & work_base[C_ORDER].astype(str).isin(confirmed_set)
    ]
    if cancelled.empty:
        st.info("Подтверждённых отмен пока нет.")
    else:
        st.dataframe(
            cancelled[TABLE_COLS + [C_STATUS]].rename(columns=COL_RENAME),
            use_container_width=True,
            hide_index=True,
        )
