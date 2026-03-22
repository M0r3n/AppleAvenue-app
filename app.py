"""
Авеню: Система Заказов — оптимизированная версия
Изменения: производительность, безопасность, стабильность, читаемость.
Функционал и интерфейс полностью сохранены.

ИСПРАВЛЕНИЕ: хранение local_in_work и reviewed_changes перенесено
в Google Sheets (лист avenue_state), чтобы состояние не терялось
при перезапуске Streamlit Cloud.

ИСПРАВЛЕНИЕ 2: reviewed_changes теперь хранит ключи вида "oid||текст_изменения",
чтобы повторное изменение в той же графе снова показывалось в интерфейсе.
"""

import streamlit as st
import gspread
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Optional
from streamlit_autorefresh import st_autorefresh
from google.oauth2.credentials import Credentials
import extra_streamlit_components as stx

# ── КОНСТАНТЫ ────────────────────────────────────────────────────────────────
SHEET_ID       = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME       = "Заказы ИМ Авеню"
STATE_TAB_NAME = "avenue_state"          # ← лист для хранения состояния
PZ_LIST        = frozenset(["ПЗ Пекин", "ПЗ Горбушка"])
START_ROW      = 26596
TRUE_VAL       = "TRUE"
FALSE_VAL      = "FALSE"
COOKIE_NAME    = "avenue_auth_status"
COOKIE_VALUE   = "authorized"
COOKIE_DAYS    = 30
REFRESH_MS     = 600_000  # 10 минут
PREVIEW_ORDERS = 50

STORE_GORB  = "Горбушка"
STORE_PEKIN = "Пекин"

_PEKIN_KEYWORDS  = ("пек", "пкн", "pekin")
_GORB_KEYWORDS   = ("горб", "грб", "gorb")

# ── НАСТРОЙКА СТРАНИЦЫ ───────────────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")

# ── АВТОРИЗАЦИЯ ──────────────────────────────────────────────────────────────
if "cookie_manager" not in st.session_state:
    st.session_state.cookie_manager = stx.CookieManager(key="avenue_auth_manager_v4")

cookie_manager: stx.CookieManager = st.session_state.cookie_manager


def check_password() -> bool:
    if st.session_state.get("password_correct"):
        return True

    all_cookies = cookie_manager.get_all()
    if all_cookies and str(all_cookies.get(COOKIE_NAME)) == COOKIE_VALUE:
        st.session_state.password_correct = True
        return True

    st.title("🔐 Вход в систему")

    if "password" not in st.secrets:
        st.error("Критическая ошибка: Пароль не настроен в Secrets.")
        st.stop()

    pwd      = st.text_input("Введите код доступа:", type="password", key="login_input")
    remember = st.checkbox("Запомнить меня на этом устройстве", value=True)

    if st.button("Войти", key="login_btn", type="primary"):
        if pwd == st.secrets["password"]:
            st.session_state.password_correct = True
            if remember:
                cookie_manager.set(
                    COOKIE_NAME,
                    COOKIE_VALUE,
                    expires_at=datetime.now() + timedelta(days=COOKIE_DAYS),
                )
            st.rerun()
        else:
            st.error("❌ Неверный код")
    return False


if not check_password():
    st.stop()

# ── GOOGLE SHEETS CLIENT ─────────────────────────────────────────────────────

@st.cache_resource
def get_client() -> gspread.Client:
    try:
        gs = st.secrets["connections"]["gsheets"]
        creds = Credentials.from_authorized_user_info(
            {
                "client_id":     gs["client_id"],
                "client_secret": gs["client_secret"],
                "refresh_token": gs["refresh_token"],
                "type":          "authorized_user",
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


# ── ПОСТОЯННОЕ СОСТОЯНИЕ ЧЕРЕЗ GOOGLE SHEETS ──────────────────────────────────
# Streamlit Cloud сбрасывает файловую систему при каждом перезапуске,
# поэтому JSON-файл ненадёжен. Храним состояние в отдельном листе той же таблицы.

def _get_state_worksheet() -> gspread.Worksheet:
    """Возвращает лист avenue_state, создаёт его при отсутствии."""
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        return spreadsheet.worksheet(STATE_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(STATE_TAB_NAME, rows=5000, cols=2)
        ws.update([["local_in_work", "reviewed_changes"]], value_input_option="RAW")
        return ws


def load_persistent_state() -> tuple[set, set]:
    """Загружает состояние из листа Google Sheets."""
    try:
        ws   = _get_state_worksheet()
        rows = ws.get_all_values()   # [[header_col1, header_col2], [val, val], ...]

        if len(rows) < 2:
            return set(), set()

        in_work  = {r[0] for r in rows[1:] if r[0].strip()}
        # reviewed хранит ключи вида "oid||текст_изменения"
        reviewed = {r[1] for r in rows[1:] if len(r) > 1 and r[1].strip()}
        return in_work, reviewed

    except Exception as e:
        st.warning(f"Не удалось загрузить состояние из Sheets: {e}")
        return set(), set()


def save_persistent_state() -> None:
    """Сохраняет текущее состояние в лист Google Sheets (полная перезапись)."""
    try:
        ws = _get_state_worksheet()

        in_work  = list(st.session_state.local_in_work)
        reviewed = list(st.session_state.reviewed_changes)

        # Выравниваем оба списка до одинаковой длины
        max_len   = max(len(in_work), len(reviewed), 1)
        in_work  += [""] * (max_len - len(in_work))
        reviewed += [""] * (max_len - len(reviewed))

        rows = [["local_in_work", "reviewed_changes"]] + list(zip(in_work, reviewed))
        ws.clear()
        ws.update(rows, value_input_option="RAW")

    except Exception as e:
        st.warning(f"Не удалось сохранить состояние в Sheets: {e}")


# ── ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: КЛЮЧ ДЛЯ REVIEWED_CHANGES ───────────────────────

def _review_key(oid, edit_text: str) -> str:
    """
    Формирует уникальный ключ из номера заказа и текста изменения.
    При изменении текста ключ меняется — заказ снова становится «непросмотренным».
    """
    return f"{oid}||{str(edit_text).strip()}"


# ── АВТООБНОВЛЕНИЕ И ИНИЦИАЛИЗАЦИЯ СОСТОЯНИЯ ─────────────────────────────────
refresh_count = st_autorefresh(interval=REFRESH_MS, key="data_refresh")

if "local_in_work" not in st.session_state:
    saved_in_work, saved_reviewed = load_persistent_state()
    st.session_state.local_in_work    = saved_in_work
    st.session_state.reviewed_changes = saved_reviewed
    st.session_state.prev_order_ids   = set()
    st.session_state.new_orders_alert = set()
    st.session_state.last_sync        = "Не обновлялось"

# ── GOOGLE SHEETS DATA ────────────────────────────────────────────────────────

def get_worksheet() -> gspread.Worksheet:
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        return spreadsheet.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return spreadsheet.get_worksheet(0)


@st.cache_data(ttl=3600)
def load_data_integrated() -> tuple[pd.DataFrame, dict]:
    sheet    = get_worksheet()
    raw_data = sheet.get_all_values()
    if not raw_data:
        return pd.DataFrame(), {}

    header_idx = next(
        (i for i, row in enumerate(raw_data[:100])
         if "Наименование" in row and "Склад" in row),
        -1,
    )
    if header_idx == -1:
        return pd.DataFrame(), {}

    headers = [str(h).strip().replace("\n", " ") for h in raw_data[header_idx]]

    def col_idx(name: str) -> int:
        try:
            return headers.index(name)
        except ValueError:
            raise ValueError(f"Колонка «{name}» не найдена в заголовке таблицы.")

    col_map = {
        "ORDER":   col_idx("Наименование") - 1,
        "PRODUCT": col_idx("Наименование"),
        "QTY":     col_idx("Кол-во"),
        "WH":      col_idx("Склад"),
        "COMMENT": col_idx("Комментарий"),
        "EDIT":    col_idx("Изменения заказа"),
        "INWORK":  col_idx("Под ЗАКАЗ"),
        "MOVE":    col_idx("Перемещение"),
        "DONE":    col_idx("Собрано"),
        "STATUS":  col_idx("Статус") if "Статус" in headers else len(headers) - 1,
    }

    data_rows = raw_data[START_ROW - 1:]
    df = pd.DataFrame(data_rows, columns=headers)
    df["_sheet_row"] = range(START_ROW, START_ROW + len(df))

    product_col = headers[col_map["PRODUCT"]]
    df = df[df[product_col].str.strip().astype(bool)].copy()

    st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
    return df, col_map


if refresh_count > 0:
    load_data_integrated.clear()

# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────────────────

def update_google_cells(group: pd.DataFrame, col_map: dict, updates: dict) -> None:
    sheet = get_worksheet()

    cell_list = [
        gspread.Cell(row=int(row_num), col=col_map[key] + 1, value=val)
        for key, val in updates.items()
        for row_num in group["_sheet_row"]
    ]

    if cell_list:
        sheet.update_cells(cell_list, value_input_option="USER_ENTERED")

    load_data_integrated.clear()


def identify_target_store(comment: str) -> str:
    c = str(comment).lower()
    if "d" in c:
        return STORE_GORB
    if any(k in c for k in _PEKIN_KEYWORDS):
        return STORE_PEKIN
    if any(k in c for k in _GORB_KEYWORDS):
        return STORE_GORB
    return "Общий"


def render_order_table(group: pd.DataFrame, table_cols: list, col_rename: dict) -> None:
    st.table(group[table_cols].rename(columns=col_rename))


# ── ПОДГОТОВКА ДАННЫХ ─────────────────────────────────────────────────────────
df_mem, C = load_data_integrated()

if df_mem.empty or not C:
    st.error("Не удалось загрузить данные. Проверьте таблицу и настройки.")
    st.stop()

cols      = df_mem.columns
C_ORDER   = cols[C["ORDER"]]
C_PRODUCT = cols[C["PRODUCT"]]
C_QTY     = cols[C["QTY"]]
C_WH      = cols[C["WH"]]
C_COMMENT = cols[C["COMMENT"]]
C_DONE    = cols[C["DONE"]]
C_MOVE    = cols[C["MOVE"]]
C_STATUS  = cols[C["STATUS"]]
C_EDIT    = cols[C["EDIT"]]
C_INWORK  = cols[C["INWORK"]]

TABLE_COLS = [C_ORDER, C_PRODUCT, C_QTY, C_WH, C_COMMENT]
COL_RENAME = {
    C_ORDER:   "Заказ",
    C_PRODUCT: "Товар",
    C_QTY:     "Кол",
    C_WH:      "Склад",
    C_COMMENT: "Коммент",
}

current_order_ids = set(df_mem[C_ORDER].unique())
if st.session_state.prev_order_ids:
    new_ids = current_order_ids - st.session_state.prev_order_ids
    if new_ids:
        st.session_state.new_orders_alert = new_ids
st.session_state.prev_order_ids = current_order_ids

work_base = df_mem[
    ~df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)
].copy()

work_base["_target_store"] = work_base[C_COMMENT].apply(identify_target_store)

# ── САЙДБАР ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏢 Меню Авеню")
menu = st.sidebar.selectbox(
    "Выберите раздел:",
    [
        "🏪 Магазин: ГОРБУШКА",
        "🏪 Магазин: ПЕКИН",
        "🚚 Перемещения (Активные)",
        "⏳ Товар Под заказ",
        "✅ Выполненные сборки",
        "🚫 Отмененные заказы",
    ],
)

if st.sidebar.button("🚪 Выйти"):
    cookie_manager.delete(COOKIE_NAME)
    st.session_state.password_correct = False
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"🔄 Последняя синхронизация: **{st.session_state.last_sync}**")

if st.sidebar.button("🔃 Обновить данные сейчас"):
    load_data_integrated.clear()
    st.rerun()

# ── ЛОГИКА ОТОБРАЖЕНИЯ МАГАЗИНА ───────────────────────────────────────────────

def _build_tags(
    comment_str: str,
    is_move_needed: bool,
    has_edit: bool,
    is_pz_item: bool,
    incoming: bool,
    extra_tag: Optional[str] = None,
) -> str:
    tags = []
    if "d" in comment_str:    tags.append("📦 ДОСТАВКА")
    if is_move_needed:         tags.append("🚚 ПЕРЕМЕЩЕНИЕ")
    if has_edit:               tags.append("⚠️ ИЗМЕНЕНИЕ" if not extra_tag else extra_tag)
    if is_pz_item:             tags.append("⏳ ПЗ")
    if incoming:               tags.append("🚚 ЕДЕТ")
    return " | ".join(tags)


def render_store(current_store: str) -> None:
    st.title(f"🏪 Заказы: {current_store}")

    store_new_alert = {
        oid for oid in st.session_state.new_orders_alert
        if oid in work_base[C_ORDER].values
    }
    if store_new_alert:
        st.success(
            f"🆕 Обнаружены новые заказы: "
            f"{', '.join(str(o) for o in sorted(store_new_alert))}"
        )

    is_pz_row = work_base[C_WH].isin(PZ_LIST)
    is_move   = work_base[C_MOVE] == TRUE_VAL

    if current_store == STORE_GORB:
        wh_match = work_base[C_WH].str.contains("Горб|Сток", case=False, na=False)
    else:
        wh_match = work_base[C_WH].str.contains("Пекин", case=False, na=False)

    is_f_match  = wh_match & ~is_pz_row
    is_pz_match = (
        (work_base[C_WH] == f"ПЗ {current_store}")
        & (work_base[C_INWORK] == TRUE_VAL)
    )
    is_incoming = is_move & (work_base["_target_store"] == current_store)

    base_mask = ((is_f_match | is_pz_match) & ~is_move) | is_incoming
    reviewed  = st.session_state.reviewed_changes

    # has_unrev теперь проверяет по ключу "oid||текст_изменения"
    def row_has_unreviewed_edit(row) -> bool:
        edit_text = str(row[C_EDIT]).strip()
        if not edit_text:
            return False
        return _review_key(row[C_ORDER], edit_text) not in reviewed

    has_unrev = work_base.apply(row_has_unreviewed_edit, axis=1)

    display_df = work_base[
        base_mask & ((work_base[C_DONE] != TRUE_VAL) | has_unrev)
    ].copy()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🆕 Новые / Изменения")
        new_items = display_df[~display_df[C_ORDER].isin(st.session_state.local_in_work)]

        for oid, group in new_items.groupby(C_ORDER, sort=False):
            comment_str    = str(group[C_COMMENT].iloc[0]).lower()
            target         = group["_target_store"].iloc[0]
            incoming       = group[C_MOVE].iloc[0] == TRUE_VAL
            is_pz_item     = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
            edit_text      = group[C_EDIT].iloc[0]
            has_edit       = bool(str(edit_text).strip()) and _review_key(oid, edit_text) not in reviewed
            is_move_needed = target != current_store and target != "Общий" and not incoming

            tag_str = _build_tags(comment_str, is_move_needed, has_edit, is_pz_item, incoming)
            label   = f"Заказ №{oid}{f' [{tag_str}]' if tag_str else ''}"

            with st.expander(label):
                if has_edit:
                    st.error(f"Изменение: {edit_text}")
                render_order_table(group, TABLE_COLS, COL_RENAME)

                if has_edit:
                    if st.button("Учесть Изменение", key=f"rev_n_{oid}"):
                        st.session_state.reviewed_changes.add(_review_key(oid, edit_text))
                        save_persistent_state()
                        st.rerun()
                else:
                    if st.button("В работу", key=f"w_{oid}"):
                        st.session_state.local_in_work.add(oid)
                        save_persistent_state()
                        st.rerun()

    with col2:
        st.subheader("🛠 В сборке")
        in_work = display_df[display_df[C_ORDER].isin(st.session_state.local_in_work)]

        for oid, group in in_work.groupby(C_ORDER, sort=False):
            comment_str    = str(group[C_COMMENT].iloc[0]).lower()
            target         = group["_target_store"].iloc[0]
            incoming       = group[C_MOVE].iloc[0] == TRUE_VAL
            is_pz_item     = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
            edit_text      = group[C_EDIT].iloc[0]
            has_edit       = bool(str(edit_text).strip()) and _review_key(oid, edit_text) not in reviewed
            is_move_needed = target != current_store and target != "Общий" and not incoming

            tag_str = _build_tags(
                comment_str, is_move_needed, has_edit, is_pz_item, incoming,
                extra_tag="⚠️ ПРАВКА",
            )
            label = f"Заказ №{oid}{f' [{tag_str}]' if tag_str else ''}"

            with st.expander(label):
                if has_edit:
                    st.error(f"Правка: {edit_text}")
                render_order_table(group, TABLE_COLS, COL_RENAME)

                if has_edit:
                    if st.button("Учесть правку", key=f"rev_w_{oid}"):
                        st.session_state.reviewed_changes.add(_review_key(oid, edit_text))
                        save_persistent_state()
                        st.rerun()
                elif is_move_needed:
                    if st.button(
                        "🚛 ОТПРАВИТЬ ПЕРЕМЕЩЕНИЕ",
                        key=f"mv_{oid}",
                        type="primary",
                        use_container_width=True,
                    ):
                        update_google_cells(group, C, {"MOVE": TRUE_VAL})
                        st.session_state.local_in_work.discard(oid)
                        save_persistent_state()
                        st.rerun()
                else:
                    action_label = "✅ ПРИНЯТО И СОБРАНО" if incoming else "✅ ЗАВЕРШИТЬ СБОРКУ"
                    if st.button(
                        action_label,
                        key=f"dn_{oid}",
                        type="primary",
                        use_container_width=True,
                    ):
                        update_google_cells(group, C, {"DONE": TRUE_VAL, "MOVE": FALSE_VAL})
                        st.session_state.local_in_work.discard(oid)
                        # Чистим конкретный ключ текущего изменения
                        st.session_state.reviewed_changes.discard(_review_key(oid, edit_text))
                        save_persistent_state()
                        st.rerun()


# ── МАРШРУТИЗАЦИЯ ─────────────────────────────────────────────────────────────

if "Магазин" in menu:
    render_store(STORE_GORB if "ГОРБУШКА" in menu else STORE_PEKIN)

elif menu == "🚚 Перемещения (Активные)":
    st.title("🚚 В пути")
    moves = work_base[work_base[C_MOVE] == TRUE_VAL]
    for oid, group in moves.groupby(C_ORDER, sort=False):
        with st.expander(f"Перемещение №{oid} ⮕ {group['_target_store'].iloc[0]}"):
            render_order_table(group, TABLE_COLS, COL_RENAME)
            if st.button("Сбросить статус перемещения", key=f"cl_mv_{oid}"):
                update_google_cells(group, C, {"MOVE": FALSE_VAL})
                st.rerun()

elif menu == "⏳ Товар Под заказ":
    st.title("⏳ Ожидание поступления (ПЗ)")
    pz = work_base[
        work_base[C_WH].isin(PZ_LIST)
        & (work_base[C_INWORK] != TRUE_VAL)
        & (work_base[C_DONE] != TRUE_VAL)
    ]
    st.dataframe(
        pz[TABLE_COLS].rename(columns=COL_RENAME),
        use_container_width=True,
        hide_index=True,
    )

elif menu == "✅ Выполненные сборки":
    st.title("✅ Последние собранные")
    done = work_base[work_base[C_DONE] == TRUE_VAL].iloc[::-1].head(PREVIEW_ORDERS)
    st.dataframe(
        done[TABLE_COLS].rename(columns=COL_RENAME),
        use_container_width=True,
        hide_index=True,
    )

elif menu == "🚫 Отмененные заказы":
    st.title("🚫 Отмененные")
    cancelled = df_mem[
        df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)
    ]
    st.dataframe(
        cancelled[TABLE_COLS + [C_STATUS]].rename(columns=COL_RENAME),
        use_container_width=True,
        hide_index=True,
    )
