from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import extra_streamlit_components as stx
import gspread
import pandas as pd
import streamlit as st
from google.oauth2.credentials import Credentials
from streamlit_autorefresh import st_autorefresh


# ── КОНСТАНТЫ ─────────────────────────────────────────────────────────────────
SHEET_ID = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME = "Заказы ИМ Авеню"
STATE_TAB_NAME = "avenue_state"

PZ_LIST = frozenset(["ПЗ Пекин", "ПЗ Горбушка"])
START_ROW = 26596

TRUE_VAL = "TRUE"
FALSE_VAL = "FALSE"
CANCELLED_VAL = "Отменён"

COOKIE_NAME = "avenue_auth_status"
COOKIE_DAYS = 30

REFRESH_MS = 600_000
PREVIEW_ORDERS = 50
STATE_SYNC_TTL = 15

REPORT_PAGE_KEY = "report_page_open"
REPORT_DATE_COL = "Дата и время сбора заказа"

STORE_GORB = "Горбушка"
STORE_PEKIN = "Пекин"
STORE_TIK = "ТИК"

MSK_TZ = ZoneInfo("Europe/Moscow")

_PEKIN_KEYWORDS = ("пек", "пкн", "pekin")
_GORB_KEYWORDS = ("горб", "грб", "gorb")

MENU_OPTIONS = [
    "🏪 Магазин: ГОРБУШКА",
    "🏪 Магазин: ПЕКИН",
    "🚚 Перемещения (Активные)",
    "⏳ Товар Под заказ",
    "✅ Выполненные сборки",
    "🚫 Отмененные заказы",
]

STATE_HEADERS = ["local_in_work", "reviewed_changes", "confirmed_cancels", "completed_log"]

REPORT_LOG_HEADERS = [
    "report_id",
    "created_at",
    "period_name",
    "date_from",
    "date_to",
    "search",
    "total_items",
    "total_sold",
]

REPORT_ITEM_HEADERS = [
    "report_id",
    "sort_order",
    "product",
    "sold",
    "row_type",
]

REPORT_CATEGORY_ORDER = {
    "iPhone": 1,
    "iPad": 2,
    "MacBook": 3,
    "AirPods": 4,
    "AppleWatch": 5,
    "Прочее Apple": 6,
    "Аксессуары": 7,
    "Dyson": 8,
    "Samsung": 9,
    "Прочее": 10,
}

ACCESSORY_KEYWORDS = (
    "чехол",
    "накладка",
    "держатель",
    "стекло",
    "кабель",
    "блок",
    "блок питания",
    "сетевой адаптер",
    "ремешок",
    "браслет",
    "зарядное устройство",
    "зарядка",
)

REPORT_EXPORT_SHEET = "Отчёт"
REPORT_META_SHEET = "Параметры"

CHECKBOX_KEYS = {"INWORK", "MOVE", "DONE"}


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


# ── БАЗОВЫЕ УТИЛИТЫ ───────────────────────────────────────────────────────────
def _now() -> datetime:
    return datetime.now(MSK_TZ)


def _sheet_datetime_now() -> str:
    return _now().strftime("%d.%m.%Y %H:%M:%S")


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value)


def _normalized_str(value: Any) -> str:
    return _safe_str(value).strip()


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


def _get_secret(name: str, default: str | None = None) -> str:
    try:
        return str(st.secrets[name])
    except Exception:
        if default is not None:
            return default
        st.error(f"Критическая ошибка: отсутствует secret «{name}».")
        st.stop()


def _bool_series_eq(series: pd.Series, expected: str) -> pd.Series:
    return series.fillna("").astype(str) == expected


def _safe_unique_str_set(series: pd.Series) -> set[str]:
    return set(series.dropna().astype(str).unique())


def _non_empty_str_list(values: Iterable[Any]) -> list[str]:
    return [_safe_str(x) for x in values if _normalized_str(x)]


def _next_data_row(values: list[list[str]]) -> int:
    if not values:
        return 2
    non_empty_rows = [row for row in values if any(_normalized_str(cell) for cell in row)]
    return len(non_empty_rows) + 2


def _clear_order_cache() -> None:
    load_data.clear()


def _clear_state_cache() -> None:
    load_state_from_sheets.clear()


def _clear_report_caches() -> None:
    load_saved_reports.clear()
    load_report_items.clear()


def _clear_all_runtime_caches() -> None:
    _clear_order_cache()
    _clear_state_cache()
    _clear_report_caches()


def _sheet_value_for_cell(col_key: str, value: Any) -> Any:
    if col_key in CHECKBOX_KEYS:
        if value in (TRUE_VAL, True, "true", "True", 1):
            return True
        if value in (FALSE_VAL, False, "false", "False", 0):
            return False
    return value


# ── COOKIE / АВТОРИЗАЦИЯ ──────────────────────────────────────────────────────
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


# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
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


def _init_state_sheet_layout(ws: gspread.Worksheet) -> None:
    ws.update("A1:D1", [STATE_HEADERS], value_input_option="RAW")
    ws.update("G1:N1", [REPORT_LOG_HEADERS], value_input_option="RAW")
    ws.update("P1:T1", [REPORT_ITEM_HEADERS], value_input_option="RAW")


@st.cache_resource
def get_state_worksheet() -> gspread.Worksheet:
    ss = get_spreadsheet()
    try:
        ws = ss.worksheet(STATE_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=STATE_TAB_NAME, rows=10000, cols=20)
        _init_state_sheet_layout(ws)
        return ws

    try:
        values = ws.get("A1:T3")
        flat = " ".join(" ".join(row) for row in values if row)
        if "report_id" not in flat:
            _init_state_sheet_layout(ws)
    except Exception:
        pass

    return ws


# ── STATE В SHEETS ────────────────────────────────────────────────────────────
def _empty_state() -> dict[str, Any]:
    return {
        "in_work": set(),
        "reviewed": set(),
        "confirmed": set(),
        "log": [],
    }


@st.cache_data(ttl=STATE_SYNC_TTL)
def load_state_from_sheets() -> dict[str, Any]:
    try:
        rows = get_state_worksheet().get("A1:D")
        if len(rows) < 2:
            return _empty_state()

        result = _empty_state()
        for row in rows[1:]:
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
        return _empty_state()


def save_state_to_sheets() -> None:
    try:
        in_work = sorted(_non_empty_str_list(st.session_state.local_in_work))
        reviewed = sorted(_non_empty_str_list(st.session_state.reviewed_changes))
        confirmed = sorted(_non_empty_str_list(st.session_state.confirmed_cancels))
        log = _non_empty_str_list(st.session_state.completed_log)

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

        _clear_state_cache()

    except Exception as e:
        st.warning(f"Не удалось сохранить состояние в Sheets: {e}")


def _sync_runtime_state() -> None:
    state = load_state_from_sheets()
    st.session_state.local_in_work = set(_safe_str(x) for x in state["in_work"])
    st.session_state.reviewed_changes = set(_safe_str(x) for x in state["reviewed"])
    st.session_state.confirmed_cancels = set(_safe_str(x) for x in state["confirmed"])
    st.session_state.completed_log = list(state["log"])


# ── ЗАГРУЗКА ДАННЫХ ───────────────────────────────────────────────────────────
def _find_header_row(raw_data: list[list[str]]) -> int:
    return next(
        (
            i
            for i, row in enumerate(raw_data[:100])
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
    try:
        raw_data = get_orders_worksheet().get_all_values()
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
    row_numbers: Iterable[int],
    col_num_0based: int,
    value: Any,
) -> list[dict[str, Any]]:
    col_letter = _a1_col(col_num_0based + 1)
    return [
        {"range": f"{col_letter}{int(row_num)}", "values": [[value]]}
        for row_num in row_numbers
    ]


def update_sheet_cells(group: pd.DataFrame, col_map: dict[str, int], updates: dict[str, Any]) -> None:
    try:
        sheet = get_orders_worksheet()
        row_numbers = [int(x) for x in group["_sheet_row"].tolist()]
        batch_payload: list[dict[str, Any]] = []

        for key, val in updates.items():
            if key not in col_map:
                raise KeyError(f"Неизвестный ключ обновления: {key}")

            prepared_val = _sheet_value_for_cell(key, val)
            batch_payload.extend(
                _build_batch_payload_for_rows(
                    row_numbers,
                    int(col_map[key]),
                    prepared_val,
                )
            )

        if batch_payload:
            sheet.batch_update(batch_payload, value_input_option="USER_ENTERED")
            _clear_order_cache()

    except Exception as e:
        st.warning(f"Не удалось обновить данные в Sheets: {e}")


def write_report_datetime(group: pd.DataFrame, col_map: dict[str, int], dt_value: str) -> None:
    try:
        sheet = get_orders_worksheet()
        row_numbers = [int(x) for x in group["_sheet_row"].tolist()]
        report_col = int(col_map["REPORT_DT"])

        batch_payload = _build_batch_payload_for_rows(row_numbers, report_col, dt_value)
        if batch_payload:
            sheet.batch_update(batch_payload, value_input_option="RAW")
            _clear_order_cache()

    except Exception as e:
        st.warning(f"Не удалось записать дату сборки: {e}")


def backfill_report_datetime_for_manual_done(df: pd.DataFrame, col_map: dict[str, int]) -> bool:
    """
    Если галочка "Собрано" была выставлена вручную напрямую в Google Sheets,
    а поле "Дата и время сбора заказа" пустое — дозаполняем его текущим временем.
    Уже заполненные даты не трогаем.
    """
    try:
        if REPORT_DATE_COL not in df.columns:
            return False

        report_col_name = df.columns[int(col_map["REPORT_DT"])]

        done_mask = _bool_series_eq(df[C_DONE], TRUE_VAL)
        report_dt_empty_mask = df[report_col_name].fillna("").astype(str).str.strip().eq("")

        missing_dt_df = df[done_mask & report_dt_empty_mask].copy()
        if missing_dt_df.empty:
            return False

        dt_now = _sheet_datetime_now()
        sheet = get_orders_worksheet()
        row_numbers = [int(x) for x in missing_dt_df["_sheet_row"].tolist()]
        report_col = int(col_map["REPORT_DT"])

        batch_payload = _build_batch_payload_for_rows(row_numbers, report_col, dt_now)
        if batch_payload:
            sheet.batch_update(batch_payload, value_input_option="RAW")
            _clear_order_cache()
            return True

        return False

    except Exception as e:
        st.warning(f"Не удалось дозаполнить дату сборки для вручную отмеченных заказов: {e}")
        return False


# ── ПРЕДМЕТНАЯ ЛОГИКА ─────────────────────────────────────────────────────────
def is_delivery(comment: str) -> bool:
    c = _safe_str(comment).lower()
    return (
        "доставка" in c
        or "курьер" in c
        or "delivery" in c
        or c.strip() == "d"
        or " d " in f" {c} "
    )


def _comment_wants_gorb(comment: str) -> bool:
    c = _safe_str(comment).lower()
    return (
        "самовывоз горб" in c
        or "самовывоз с горбушки" in c
        or "на горб" in c
        or "горбушка" in c
        or any(k in c for k in _GORB_KEYWORDS)
    )


def _comment_wants_pekin(comment: str) -> bool:
    c = _safe_str(comment).lower()
    return (
        "самовывоз пекин" in c
        or "на пекин" in c
        or any(k in c for k in _PEKIN_KEYWORDS)
    )


def identify_target_store(comment: str, wh: str = "") -> str:
    c = _safe_str(comment).lower().strip()
    w = _safe_str(wh).lower().strip()

    if "самовывоз горб" in c or "самовывоз с горбушки" in c or "на горб" in c:
        return STORE_GORB

    if "самовывоз пекин" in c or "на пекин" in c:
        return STORE_PEKIN

    if _comment_wants_gorb(c):
        return STORE_GORB

    if _comment_wants_pekin(c):
        return STORE_PEKIN

    if is_delivery(c):
        return "Общий"

    if w == STORE_TIK.lower():
        return "Общий"

    return "Общий"


def _source_store_from_wh(wh: str) -> str:
    w = _safe_str(wh).lower()
    if "пекин" in w:
        return STORE_PEKIN
    if "горб" in w or "сток" in w:
        return STORE_GORB
    return ""


def _is_pending_move_from_store(wh: str, comment: str, current_store: str) -> bool:
    source_store = _source_store_from_wh(wh)
    if source_store != current_store:
        return False

    if current_store == STORE_PEKIN:
        return _comment_wants_gorb(comment)

    if current_store == STORE_GORB:
        return _comment_wants_pekin(comment)

    return False


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


def _product_text_variants(product_name: Any) -> tuple[str, str]:
    raw = _safe_str(product_name).strip().lower()
    compact = raw.replace(" ", "").replace("-", "").replace("_", "")
    return raw, compact


def _is_accessory_product(product_name: Any) -> bool:
    raw, _ = _product_text_variants(product_name)
    return any(keyword in raw for keyword in ACCESSORY_KEYWORDS)


def _detect_product_category(product_name: Any) -> str:
    raw, compact = _product_text_variants(product_name)

    if _is_accessory_product(product_name):
        return "Аксессуары"

    if "iphone" in compact:
        return "iPhone"

    if "ipad" in compact:
        return "iPad"

    if "macbook" in compact:
        return "MacBook"

    if "airpods" in compact or "earpod" in compact:
        return "AirPods"

    if "applewatch" in compact or "apple watch" in raw:
        return "AppleWatch"

    if "dyson" in raw or "dyson" in compact:
        return "Dyson"

    if "samsung" in raw or "samsung" in compact:
        return "Samsung"

    if "apple" in raw or "apple" in compact:
        return "Прочее Apple"

    return "Прочее"


def _prepare_sales_report_df(df: pd.DataFrame) -> pd.DataFrame:
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
    period_df = report_df[(report_df["_dt"] >= start_dt) & (report_df["_dt"] < end_dt)].copy()

    if period_df.empty:
        return pd.DataFrame(columns=["Категория", "Товар", "Продано"])

    period_df["Категория"] = period_df[C_PRODUCT].apply(_detect_product_category)

    result = (
        period_df.groupby(["Категория", C_PRODUCT], dropna=False)["_qty_num"]
        .sum()
        .reset_index()
        .rename(columns={C_PRODUCT: "Товар", "_qty_num": "Продано"})
    )

    result["_cat_order"] = result["Категория"].map(REPORT_CATEGORY_ORDER).fillna(999)

    result = (
        result.sort_values(
            ["_cat_order", "Продано", "Товар"],
            ascending=[True, False, True],
        )
        .drop(columns=["_cat_order"])
        .reset_index(drop=True)
    )

    result["Продано"] = result["Продано"].apply(
        lambda x: int(x) if float(x).is_integer() else round(float(x), 3)
    )
    return result


def _format_period_caption(start_dt: pd.Timestamp, end_dt_exclusive: pd.Timestamp) -> str:
    end_inclusive = end_dt_exclusive - pd.Timedelta(days=1)
    return f"{start_dt.strftime('%d.%m.%Y')} — {end_inclusive.strftime('%d.%m.%Y')}"


def _get_report_period_bounds(
    period_mode: str,
    custom_start: Any | None = None,
    custom_end: Any | None = None,
) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    now_ts = pd.Timestamp(_now().replace(tzinfo=None))
    today_start = now_ts.normalize()
    tomorrow_start = today_start + pd.Timedelta(days=1)

    if period_mode == "За сегодня":
        return today_start, tomorrow_start, "За сегодня"

    if period_mode == "За неделю":
        week_start = today_start - pd.Timedelta(days=today_start.weekday())
        next_week_start = week_start + pd.Timedelta(days=7)
        return week_start, next_week_start, "За неделю"

    if period_mode == "За месяц":
        month_start = today_start.replace(day=1)
        next_month_start = month_start + pd.offsets.MonthBegin(1)
        return month_start, next_month_start, "За месяц"

    start_dt = pd.Timestamp(custom_start).normalize()
    end_dt = pd.Timestamp(custom_end).normalize() + pd.Timedelta(days=1)

    if end_dt <= start_dt:
        end_dt = start_dt + pd.Timedelta(days=1)

    return start_dt, end_dt, "Произвольный период"


def _apply_report_search(report_df: pd.DataFrame, search_text: str) -> pd.DataFrame:
    query = _safe_str(search_text).strip()
    if not query:
        return report_df

    return report_df[
        report_df["Товар"].fillna("").astype(str).str.contains(query, case=False, na=False)
    ].copy()


def _build_report_filename(period_name: str, start_dt: pd.Timestamp, end_dt_exclusive: pd.Timestamp) -> str:
    period_slug = {
        "За сегодня": "today",
        "За неделю": "week",
        "За месяц": "month",
        "Произвольный период": "custom",
    }.get(period_name, "report")

    end_inclusive = end_dt_exclusive - pd.Timedelta(days=1)
    ts = _now().strftime("%Y%m%d_%H%M%S")

    return (
        f"avenue_report_{period_slug}_"
        f"{start_dt.strftime('%Y%m%d')}_{end_inclusive.strftime('%Y%m%d')}_{ts}.xlsx"
    )


def _build_report_meta_df(
    *,
    period_name: str,
    start_dt: pd.Timestamp,
    end_dt_exclusive: pd.Timestamp,
    search_text: str,
    created_at: str | None = None,
    report_id: str | None = None,
    total_items: Any = 0,
    total_sold: Any = 0,
) -> pd.DataFrame:
    rows: list[list[Any]] = []

    if report_id is not None:
        rows.append(["ID отчёта", report_id])
    if created_at is not None:
        rows.append(["Создан", created_at])

    rows.extend(
        [
            ["Период", period_name],
            ["Дата начала", start_dt.strftime("%d.%m.%Y")],
            ["Дата конца", (end_dt_exclusive - pd.Timedelta(days=1)).strftime("%d.%m.%Y")],
            ["Поиск", _safe_str(search_text).strip() or "—"],
            ["Сформирован", _sheet_datetime_now()],
            ["Всего позиций", total_items],
            ["Всего продано", total_sold],
        ]
    )

    return pd.DataFrame(rows, columns=["Параметр", "Значение"])


def _write_excel_bytes(report_df: pd.DataFrame, meta_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        report_df.to_excel(writer, index=False, sheet_name=REPORT_EXPORT_SHEET)
        meta_df.to_excel(writer, index=False, sheet_name=REPORT_META_SHEET)

        ws = writer.sheets[REPORT_EXPORT_SHEET]
        ws.freeze_panes = "A2"
        ws.column_dimensions["A"].width = 22
        ws.column_dimensions["B"].width = 60
        ws.column_dimensions["C"].width = 14

        ws_meta = writer.sheets[REPORT_META_SHEET]
        ws_meta.column_dimensions["A"].width = 24
        ws_meta.column_dimensions["B"].width = 30

    output.seek(0)
    return output.getvalue()


def _report_to_excel_bytes(
    report_df: pd.DataFrame,
    period_name: str,
    start_dt: pd.Timestamp,
    end_dt_exclusive: pd.Timestamp,
    search_text: str,
) -> bytes:
    total_sold = report_df["Продано"].sum() if not report_df.empty else 0
    meta_df = _build_report_meta_df(
        period_name=period_name,
        start_dt=start_dt,
        end_dt_exclusive=end_dt_exclusive,
        search_text=search_text,
        total_items=len(report_df),
        total_sold=total_sold,
    )
    return _write_excel_bytes(report_df, meta_df)


def _make_report_id() -> str:
    return f"RPT_{_now().strftime('%Y%m%d_%H%M%S')}"


def save_report_to_state_sheet(
    report_df: pd.DataFrame,
    period_name: str,
    start_dt: pd.Timestamp,
    end_dt_exclusive: pd.Timestamp,
    search_text: str,
) -> str:
    try:
        ws = get_state_worksheet()
        report_id = _make_report_id()
        created_at = _sheet_datetime_now()
        end_inclusive = end_dt_exclusive - pd.Timedelta(days=1)

        total_items = len(report_df)
        total_sold = report_df["Продано"].sum() if not report_df.empty else 0

        log_row = [
            report_id,
            created_at,
            period_name,
            start_dt.strftime("%d.%m.%Y"),
            end_inclusive.strftime("%d.%m.%Y"),
            _safe_str(search_text).strip(),
            total_items,
            total_sold,
        ]

        next_log_row = _next_data_row(ws.get("G2:N"))

        ws.update(
            f"G{next_log_row}:N{next_log_row}",
            [log_row],
            value_input_option="RAW",
        )

        item_rows: list[list[Any]] = []
        for idx, row in enumerate(report_df.itertuples(index=False), start=1):
            item_rows.append([
                report_id,
                idx,
                _safe_str(row.Товар),
                row.Продано,
                _safe_str(row.Категория),
            ])

        if item_rows:
            next_item_row = _next_data_row(ws.get("P2:T"))
            end_row = next_item_row + len(item_rows) - 1
            ws.update(
                f"P{next_item_row}:T{end_row}",
                item_rows,
                value_input_option="RAW",
            )

        return report_id

    except Exception as e:
        st.warning(f"Не удалось сохранить отчёт в Sheets: {e}")
        return ""


@st.cache_data(ttl=60)
def load_saved_reports() -> pd.DataFrame:
    try:
        ws = get_state_worksheet()
        rows = ws.get("G1:N")
        if not rows or len(rows) < 2:
            return pd.DataFrame(columns=REPORT_LOG_HEADERS)

        headers = rows[0]
        data = [row + [""] * (len(headers) - len(row)) for row in rows[1:] if any(str(x).strip() for x in row)]
        df = pd.DataFrame(data, columns=headers)

        if df.empty:
            return df

        for col in ("total_items", "total_sold"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "created_at" in df.columns:
            df["_created_at_dt"] = pd.to_datetime(
                df["created_at"].astype(str).str.strip(),
                errors="coerce",
                dayfirst=True,
            )
        else:
            df["_created_at_dt"] = pd.NaT

        return df

    except Exception:
        return pd.DataFrame(columns=REPORT_LOG_HEADERS)


@st.cache_data(ttl=60)
def load_report_items(report_id: str) -> pd.DataFrame:
    try:
        ws = get_state_worksheet()
        rows = ws.get("P1:T")
        if not rows or len(rows) < 2:
            return pd.DataFrame(columns=REPORT_ITEM_HEADERS)

        headers = rows[0]
        data = [row + [""] * (len(headers) - len(row)) for row in rows[1:] if any(str(x).strip() for x in row)]
        df = pd.DataFrame(data, columns=headers)

        if df.empty:
            return df

        df = df[df["report_id"].astype(str) == str(report_id)].copy()
        if df.empty:
            return df

        df["sort_order"] = pd.to_numeric(df["sort_order"], errors="coerce").fillna(0)
        df["sold"] = pd.to_numeric(df["sold"], errors="coerce").fillna(0)
        df = df.sort_values(["sort_order"], ascending=[True]).reset_index(drop=True)
        return df

    except Exception:
        return pd.DataFrame(columns=REPORT_ITEM_HEADERS)


def build_saved_report_excel(report_meta: pd.Series, items_df: pd.DataFrame) -> bytes:
    export_df = items_df.copy()
    export_df["Категория"] = export_df["row_type"].apply(
        lambda x: _safe_str(x).strip() if _safe_str(x).strip() and _safe_str(x).strip().lower() != "item" else "Прочее"
    )
    export_df = export_df[["Категория", "product", "sold"]].rename(
        columns={"product": "Товар", "sold": "Продано"}
    )

    export_df["_cat_order"] = export_df["Категория"].map(REPORT_CATEGORY_ORDER).fillna(999)
    export_df = export_df.sort_values(
        ["_cat_order", "Продано", "Товар"],
        ascending=[True, False, True],
    ).drop(columns=["_cat_order"]).reset_index(drop=True)

    try:
        start_dt = pd.to_datetime(_safe_str(report_meta.get("date_from", "")), dayfirst=True, errors="coerce")
        end_dt = pd.to_datetime(_safe_str(report_meta.get("date_to", "")), dayfirst=True, errors="coerce")
    except Exception:
        start_dt = pd.NaT
        end_dt = pd.NaT

    if pd.notna(start_dt) and pd.notna(end_dt):
        end_dt_exclusive = end_dt + pd.Timedelta(days=1)
    else:
        start_dt = pd.Timestamp(_now().replace(tzinfo=None)).normalize()
        end_dt_exclusive = start_dt + pd.Timedelta(days=1)

    meta_df = _build_report_meta_df(
        report_id=_safe_str(report_meta.get("report_id", "")),
        created_at=_safe_str(report_meta.get("created_at", "")),
        period_name=_safe_str(report_meta.get("period_name", "")),
        start_dt=start_dt,
        end_dt_exclusive=end_dt_exclusive,
        search_text=_safe_str(report_meta.get("search", "")),
        total_items=report_meta.get("total_items", 0),
        total_sold=report_meta.get("total_sold", 0),
    )

    return _write_excel_bytes(export_df, meta_df)


def render_report() -> None:
    st.title("📊 Отчёт по продажам")

    if REPORT_DATE_COL not in work_base.columns:
        st.warning(f"В таблице не найден столбец «{REPORT_DATE_COL}».")
        return

    report_df = _prepare_sales_report_df(work_base)

    st.caption(f"Отчёт строится по столбцу: **{REPORT_DATE_COL}**")

    ctrl_col1, ctrl_col2 = st.columns([2, 1])

    with ctrl_col1:
        period_mode = st.radio(
            "Период отчёта",
            ["За сегодня", "За неделю", "За месяц", "Произвольный период"],
            horizontal=True,
        )

    custom_start = None
    custom_end = None

    if period_mode == "Произвольный период":
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            custom_start = st.date_input("Дата начала", value=_now().date(), key="report_custom_start")
        with date_col2:
            custom_end = st.date_input("Дата конца", value=_now().date(), key="report_custom_end")

    with ctrl_col2:
        search_text = st.text_input(
            "🔎 Поиск позиции",
            placeholder="Введите название товара",
            key="report_search_text",
        )

    start_dt, end_dt_exclusive, period_name = _get_report_period_bounds(
        period_mode,
        custom_start,
        custom_end,
    )

    result_df = _build_period_report(report_df, start_dt, end_dt_exclusive)
    result_df = _apply_report_search(result_df, search_text)
    result_df["_cat_order"] = result_df["Категория"].map(REPORT_CATEGORY_ORDER).fillna(999)
    result_df = result_df.sort_values(
        ["_cat_order", "Продано", "Товар"],
        ascending=[True, False, True],
    ).drop(columns=["_cat_order"]).reset_index(drop=True)

    total_sold = result_df["Продано"].sum() if not result_df.empty else 0
    total_items = len(result_df)
    top_item = result_df.iloc[0]["Товар"] if not result_df.empty else "—"

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Продано всего", total_sold)
    metric2.metric("Позиций", total_items)
    metric3.metric("Топ позиция", top_item)

    st.info(f"Период: **{_format_period_caption(start_dt, end_dt_exclusive)}**")

    action_col1, action_col2 = st.columns(2)

    with action_col1:
        if st.button("💾 Сохранить отчёт в архив", use_container_width=True, type="primary"):
            report_id = save_report_to_state_sheet(
                report_df=result_df,
                period_name=period_name,
                start_dt=start_dt,
                end_dt_exclusive=end_dt_exclusive,
                search_text=search_text,
            )
            if report_id:
                _clear_report_caches()
                st.success(f"Отчёт сохранён в архив: {report_id}")

    with action_col2:
        excel_bytes = _report_to_excel_bytes(
            report_df=result_df,
            period_name=period_name,
            start_dt=start_dt,
            end_dt_exclusive=end_dt_exclusive,
            search_text=search_text,
        )
        st.download_button(
            "⬇️ Скачать текущий Excel",
            data=excel_bytes,
            file_name=_build_report_filename(period_name, start_dt, end_dt_exclusive),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    if result_df.empty:
        st.warning("По выбранному периоду и фильтру данных нет.")
    else:
        st.dataframe(
            result_df,
            use_container_width=True,
            hide_index=True,
            height=650,
            column_config={
                "Категория": st.column_config.TextColumn("Категория", width="medium"),
                "Товар": st.column_config.TextColumn("Товар", width="large"),
                "Продано": st.column_config.NumberColumn("Продано", format="%.3f"),
            },
        )

    st.markdown("---")
    st.subheader("🗂 Архив отчётов")

    saved_reports = load_saved_reports()

    if saved_reports.empty:
        st.info("Архив отчётов пока пуст.")
        return

    if "_created_at_dt" in saved_reports.columns:
        saved_reports = saved_reports.sort_values(
            by=["_created_at_dt", "created_at"],
            ascending=[False, False],
            na_position="last",
        ).reset_index(drop=True)
    else:
        saved_reports = saved_reports.sort_values(["created_at"], ascending=[False]).reset_index(drop=True)

    display_options = [
        f"{row['report_id']} | {row['created_at']} | {row['period_name']} | {row['date_from']} - {row['date_to']}"
        for _, row in saved_reports.iterrows()
    ]

    selected_label = st.selectbox("Выберите сохранённый отчёт", display_options)
    selected_idx = display_options.index(selected_label)
    selected_meta = saved_reports.iloc[selected_idx]

    items_df = load_report_items(selected_meta["report_id"])

    if items_df.empty:
        st.warning("Для выбранного отчёта не найдены строки товаров.")
        return

    preview_df = items_df.copy()
    preview_df["Категория"] = preview_df["row_type"].apply(
        lambda x: _safe_str(x).strip() if _safe_str(x).strip() and _safe_str(x).strip().lower() != "item" else "Прочее"
    )
    preview_df = preview_df[["Категория", "product", "sold"]].rename(
        columns={"product": "Товар", "sold": "Продано"}
    )
    preview_df["_cat_order"] = preview_df["Категория"].map(REPORT_CATEGORY_ORDER).fillna(999)
    preview_df = preview_df.sort_values(
        ["_cat_order", "Продано", "Товар"],
        ascending=[True, False, True],
    ).drop(columns=["_cat_order"]).reset_index(drop=True)

    st.dataframe(preview_df, use_container_width=True, hide_index=True, height=400)

    saved_excel = build_saved_report_excel(selected_meta, items_df)
    st.download_button(
        "⬇️ Скачать сохранённый отчёт",
        data=saved_excel,
        file_name=f"{selected_meta['report_id']}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=f"download_saved_{selected_meta['report_id']}",
    )


# ── АВТООБНОВЛЕНИЕ ────────────────────────────────────────────────────────────
st.session_state.setdefault("auto_refresh_enabled", True)

refresh_count = (
    st_autorefresh(interval=REFRESH_MS, key="data_refresh")
    if st.session_state.auto_refresh_enabled
    else 0
)


# ── ИНИЦИАЛИЗАЦИЯ SESSION_STATE ───────────────────────────────────────────────
def _init_session_state() -> None:
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


_init_session_state()

if refresh_count > 0:
    _clear_order_cache()


# ── ПОДГОТОВКА DF ─────────────────────────────────────────────────────────────
df_mem, C = load_data()

if df_mem.empty or not C:
    st.error("Не удалось загрузить данные. Проверьте таблицу и настройки.")
    st.stop()

df_mem = df_mem.copy()

C_ORDER = "__order__"
C_PRODUCT = "__product__"
C_QTY = "__qty__"
C_WH = "__wh__"
C_COMMENT = "__comment__"
C_DONE = "__done__"
C_MOVE = "__move__"
C_STATUS = "__status__"
C_EDIT = "__edit__"
C_INWORK = "__inwork__"

df_mem[C_ORDER] = df_mem.iloc[:, C["ORDER"]].fillna("").astype(str)
df_mem[C_PRODUCT] = df_mem.iloc[:, C["PRODUCT"]].fillna("").astype(str)
df_mem[C_QTY] = df_mem.iloc[:, C["QTY"]].fillna("").astype(str)
df_mem[C_WH] = df_mem.iloc[:, C["WH"]].fillna("").astype(str)
df_mem[C_COMMENT] = df_mem.iloc[:, C["COMMENT"]].fillna("").astype(str)
df_mem[C_DONE] = df_mem.iloc[:, C["DONE"]].fillna("").astype(str)
df_mem[C_MOVE] = df_mem.iloc[:, C["MOVE"]].fillna("").astype(str)
df_mem[C_STATUS] = df_mem.iloc[:, C["STATUS"]].fillna("").astype(str)
df_mem[C_EDIT] = df_mem.iloc[:, C["EDIT"]].fillna("").astype(str)
df_mem[C_INWORK] = df_mem.iloc[:, C["INWORK"]].fillna("").astype(str)

if backfill_report_datetime_for_manual_done(df_mem, C):
    st.rerun()

TABLE_COLS = [C_ORDER, C_PRODUCT, C_QTY, C_WH, C_COMMENT]
COL_RENAME = {
    C_ORDER: "Заказ",
    C_PRODUCT: "Товар",
    C_QTY: "Кол",
    C_WH: "Склад",
    C_COMMENT: "Коммент",
}

current_order_ids = _safe_unique_str_set(df_mem[C_ORDER])
if st.session_state.prev_order_ids:
    new_ids = current_order_ids - st.session_state.prev_order_ids
    if new_ids:
        st.session_state.new_orders_alert = new_ids
        st.session_state.new_orders_alert_time = _now()
st.session_state.prev_order_ids = current_order_ids

work_base = df_mem.copy()
work_base["_target_store"] = work_base.apply(
    lambda row: identify_target_store(row[C_COMMENT], row[C_WH]),
    axis=1,
)
work_base["_is_cancelled"] = (
    work_base[C_STATUS].astype(str).str.strip().str.lower() == CANCELLED_VAL.lower()
)


# ── SIDEBAR ───────────────────────────────────────────────────────────────────
def render_sidebar() -> str:
    st.sidebar.title("🏢 Меню Авеню")

    st.session_state.auto_refresh_enabled = st.sidebar.toggle(
        "🔄 Автообновление (10 мин)",
        value=st.session_state.auto_refresh_enabled,
        key="auto_refresh_toggle",
    )

    menu_value = st.sidebar.selectbox("Выберите раздел:", MENU_OPTIONS)

    if st.sidebar.button("🚪 Выйти"):
        cookie_manager.delete(COOKIE_NAME)
        st.session_state.password_correct = False
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.caption(f"🔄 Последняя синхронизация: **{st.session_state.last_sync}**")

    if st.sidebar.button("🔃 Обновить данные сейчас"):
        _clear_all_runtime_caches()
        st.rerun()

    st.sidebar.markdown("---")
    if st.sidebar.button("📊 Отчёт", use_container_width=True):
        st.session_state[REPORT_PAGE_KEY] = True
        st.rerun()

    return menu_value


menu = render_sidebar()


# ── STORE VIEW ────────────────────────────────────────────────────────────────
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
    is_move = _bool_series_eq(work_base[C_MOVE], TRUE_VAL)
    wh_match = work_base[C_WH].str.contains(wh_pattern, case=False, na=False)
    wh_clean = work_base[C_WH].fillna("").astype(str).str.strip()

    is_f_match = wh_match & ~is_pz_row
    is_pz_match = (work_base[C_WH] == f"ПЗ {current_store}") & _bool_series_eq(work_base[C_INWORK], TRUE_VAL)

    is_incoming = is_move & (work_base["_target_store"] == current_store)

    order_series = work_base[C_ORDER].astype(str)
    in_work_ids = {str(x) for x in st.session_state.local_in_work}

    is_outgoing_move_from_current_store = (
        is_move
        & pd.Series(
            [
                _source_store_from_wh(wh) == current_store
                for wh in work_base[C_WH]
            ],
            index=work_base.index,
        )
        & (work_base["_target_store"] != current_store)
    )

    is_tik_pending = (
        (wh_clean == STORE_TIK)
        & ~is_move
        & (work_base["_target_store"] == current_store)
    )

    is_waiting_move_from_current_store = (
        ~is_move
        & ~is_pz_row
        & ~is_tik_pending
        & pd.Series(
            [
                _is_pending_move_from_store(wh, comment, current_store)
                for wh, comment in zip(work_base[C_WH], work_base[C_COMMENT])
            ],
            index=work_base.index,
        )
    )

    confirmed_set = {str(x) for x in st.session_state.confirmed_cancels}

    reviewed = st.session_state.reviewed_changes
    edit_series = work_base[C_EDIT].fillna("").astype(str).str.strip()

    review_keys = pd.Series(
        (_review_key(oid, et) for oid, et in zip(order_series, edit_series)),
        index=work_base.index,
    )
    has_unrev = edit_series.astype(bool) & ~review_keys.isin(reviewed)

    is_outgoing_move_visible_here = (
        is_outgoing_move_from_current_store
        & (
            order_series.isin(in_work_ids)
            | has_unrev
        )
    )

    is_cancelled_unconfirmed = (
        work_base["_is_cancelled"]
        & (
            is_f_match
            | is_pz_match
            | is_incoming
            | is_tik_pending
            | is_waiting_move_from_current_store
            | is_outgoing_move_visible_here
        )
        & ~work_base[C_ORDER].astype(str).isin(confirmed_set)
    )

    base_mask = (
        ((is_f_match | is_pz_match) & ~is_move)
        | is_incoming
        | is_tik_pending
        | is_waiting_move_from_current_store
        | is_outgoing_move_visible_here
    )

    not_confirmed_cancelled = ~(
        work_base["_is_cancelled"]
        & work_base[C_ORDER].astype(str).isin(confirmed_set)
    )

    display_df = work_base[
        (base_mask & ((_bool_series_eq(work_base[C_DONE], TRUE_VAL) == False) | has_unrev) & not_confirmed_cancelled)
        | is_cancelled_unconfirmed
    ].copy()
    display_df["_is_tik_pending"] = is_tik_pending.reindex(display_df.index, fill_value=False)

    def _handle_mark_in_work(oid_str: str) -> None:
        st.session_state.local_in_work.add(oid_str)
        save_state_to_sheets()
        st.rerun()

    def _handle_review_change(review_key: str) -> None:
        st.session_state.reviewed_changes.add(review_key)
        save_state_to_sheets()
        st.rerun()

    def _handle_confirm_cancel(oid_str: str, group: pd.DataFrame) -> None:
        update_sheet_cells(group, C, {"DONE": TRUE_VAL})
        st.session_state.confirmed_cancels.add(oid_str)
        st.session_state.local_in_work.discard(oid_str)
        _log_action(oid_str, group, current_store, "cancel_confirmed")
        save_state_to_sheets()
        st.rerun()

    def _handle_send_move(oid_str: str, group: pd.DataFrame) -> None:
        update_sheet_cells(group, C, {"MOVE": TRUE_VAL})
        save_state_to_sheets()
        st.rerun()

    def _handle_complete_order(oid_str: str, group: pd.DataFrame, review_key: str | None = None) -> None:
        dt_now = _sheet_datetime_now()
        update_sheet_cells(group, C, {"DONE": TRUE_VAL})
        write_report_datetime(group, C, dt_now)
        st.session_state.local_in_work.discard(oid_str)
        if review_key:
            st.session_state.reviewed_changes.discard(review_key)
        _log_action(oid_str, group, current_store, "done")
        save_state_to_sheets()
        st.rerun()

    def _handle_cancel_order(oid_str: str, group: pd.DataFrame) -> None:
        update_sheet_cells(group, C, {"STATUS": CANCELLED_VAL})
        st.session_state.local_in_work.discard(oid_str)
        save_state_to_sheets()
        st.rerun()

    def _handle_tik_move(group: pd.DataFrame) -> None:
        update_sheet_cells(group, C, {"MOVE": TRUE_VAL})
        st.rerun()

    def _render_order(oid: Any, group: pd.DataFrame, in_work_section: bool) -> None:
        oid_str = _safe_str(oid)
        comment_str = _safe_str(group[C_COMMENT].iloc[0])
        incoming = _safe_str(group[C_MOVE].iloc[0]) == TRUE_VAL
        is_pz_item = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
        edit_text = _safe_str(group[C_EDIT].iloc[0])
        review_key = _review_key(oid_str, edit_text)
        has_edit = bool(edit_text.strip()) and review_key not in reviewed
        cancelled = bool(group["_is_cancelled"].iloc[0])
        tik_pending = bool(group["_is_tik_pending"].iloc[0])

        is_move_needed = (
            not incoming
            and _is_pending_move_from_store(
                _safe_str(group[C_WH].iloc[0]),
                _safe_str(group[C_COMMENT].iloc[0]),
                current_store,
            )
        )

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
                    _handle_confirm_cancel(oid_str, group)

            elif has_edit:
                btn = "Учесть правку" if in_work_section else "Учесть Изменение"
                if st.button(btn, key=f"rev_{'w' if in_work_section else 'n'}_{oid_str}"):
                    _handle_review_change(review_key)

            elif tik_pending:
                if st.button(
                    "🚛 Подтвердить перемещение с ТИК",
                    key=f"tik_mv_{oid_str}",
                    type="primary",
                    use_container_width=True,
                ):
                    _handle_tik_move(group)

                st.markdown("---")
                if not in_work_section:
                    if st.button("В работу", key=f"w_{oid_str}", use_container_width=True):
                        _handle_mark_in_work(oid_str)
                else:
                    if st.button(
                        "✅ ПРИНЯТО И СОБРАНО",
                        key=f"dn_tik_{oid_str}",
                        type="primary",
                        use_container_width=True,
                    ):
                        _handle_complete_order(oid_str, group)

            elif in_work_section:
                if is_move_needed:
                    if st.button(
                        "🚛 ОТПРАВИТЬ ПЕРЕМЕЩЕНИЕ",
                        key=f"mv_{oid_str}",
                        type="primary",
                        use_container_width=True,
                    ):
                        _handle_send_move(oid_str, group)
                else:
                    action_label = "✅ ПРИНЯТО И СОБРАНО" if incoming else "✅ ЗАВЕРШИТЬ СБОРКУ"
                    if st.button(
                        action_label,
                        key=f"dn_{oid_str}",
                        type="primary",
                        use_container_width=True,
                    ):
                        _handle_complete_order(oid_str, group, review_key)

                    st.markdown("---")
                    if st.button("🚫 Отменить заказ", key=f"cancel_{oid_str}", use_container_width=True):
                        _handle_cancel_order(oid_str, group)

            else:
                if st.button("В работу", key=f"w_{oid_str}", use_container_width=True):
                    _handle_mark_in_work(oid_str)

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


# ── РОУТИНГ ───────────────────────────────────────────────────────────────────
if st.session_state.get(REPORT_PAGE_KEY):
    render_report()
    if st.sidebar.button("⬅️ Назад", use_container_width=True):
        st.session_state[REPORT_PAGE_KEY] = False
        st.rerun()

elif "Магазин" in menu:
    render_store(STORE_GORB if "ГОРБУШКА" in menu else STORE_PEKIN)

elif menu == "🚚 Перемещения (Активные)":
    st.title("🚚 В пути")
    for oid, group in work_base[_bool_series_eq(work_base[C_MOVE], TRUE_VAL)].groupby(C_ORDER, sort=False):
        with st.expander(f"Перемещение №{oid} ⮕ {group['_target_store'].iloc[0]}"):
            render_order_table(group)
            if st.button("Сбросить статус перемещения", key=f"cl_mv_{oid}"):
                update_sheet_cells(group, C, {"MOVE": FALSE_VAL})
                st.rerun()

elif menu == "⏳ Товар Под заказ":
    st.title("⏳ Ожидание поступления (ПЗ)")
    pz = work_base[
        work_base[C_WH].isin(PZ_LIST)
        & (_bool_series_eq(work_base[C_INWORK], TRUE_VAL) == False)
        & (_bool_series_eq(work_base[C_DONE], TRUE_VAL) == False)
        & ~work_base["_is_cancelled"]
    ]
    st.dataframe(
        pz[TABLE_COLS].rename(columns=COL_RENAME),
        use_container_width=True,
        hide_index=True,
    )

elif menu == "✅ Выполненные сборки":
    st.title("✅ Последние собранные")
    done = work_base[(_bool_series_eq(work_base[C_DONE], TRUE_VAL)) & ~work_base["_is_cancelled"]].iloc[::-1].head(PREVIEW_ORDERS)
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
