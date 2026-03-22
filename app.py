"""
Авеню: Система Заказов
Оптимизированная версия v2: производительность, безопасность, стабильность.
"""

from __future__ import annotations

import hmac
from datetime import datetime, timedelta

import extra_streamlit_components as stx
import gspread
import pandas as pd
import streamlit as st
from google.oauth2.credentials import Credentials
from streamlit_autorefresh import st_autorefresh

# ── КОНСТАНТЫ ────────────────────────────────────────────────────────────────
SHEET_ID       = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME       = "Заказы ИМ Авеню"
STATE_TAB_NAME = "avenue_state"
PZ_LIST        = frozenset(["ПЗ Пекин", "ПЗ Горбушка"])
START_ROW      = 26596
TRUE_VAL       = "TRUE"
FALSE_VAL      = "FALSE"
COOKIE_NAME    = "avenue_auth_status"
COOKIE_VALUE   = "authorized"
COOKIE_DAYS    = 30
REFRESH_MS     = 600_000
PREVIEW_ORDERS = 50
STORE_GORB     = "Горбушка"
STORE_PEKIN    = "Пекин"
CANCELLED_VAL  = "Отменён"

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

# ── НАСТРОЙКА СТРАНИЦЫ ───────────────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")

# ── АВТОРИЗАЦИЯ ──────────────────────────────────────────────────────────────
if "cookie_manager" not in st.session_state:
    st.session_state.cookie_manager = stx.CookieManager(key="avenue_auth_manager_v4")

cookie_manager: stx.CookieManager = st.session_state.cookie_manager


def check_password() -> bool:
    if st.session_state.get("password_correct"):
        return True

    cookies = cookie_manager.get_all()
    if cookies and str(cookies.get(COOKIE_NAME)) == COOKIE_VALUE:
        st.session_state.password_correct = True
        return True

    st.title("🔐 Вход в систему")

    if "password" not in st.secrets:
        st.error("Критическая ошибка: Пароль не настроен в Secrets.")
        st.stop()

    pwd      = st.text_input("Введите код доступа:", type="password", key="login_input")
    remember = st.checkbox("Запомнить меня на этом устройстве", value=True)

    if st.button("Войти", key="login_btn", type="primary"):
        if hmac.compare_digest(pwd, st.secrets["password"]):
            st.session_state.password_correct = True
            if remember:
                cookie_manager.set(
                    COOKIE_NAME, COOKIE_VALUE,
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
        gs    = st.secrets["connections"]["gsheets"]
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


@st.cache_resource
def get_spreadsheet() -> gspread.Spreadsheet:
    return get_client().open_by_key(SHEET_ID)


@st.cache_resource
def get_state_worksheet() -> gspread.Worksheet:
    """Кешируем объект листа состояний — создаём при необходимости."""
    ss = get_spreadsheet()
    try:
        return ss.worksheet(STATE_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(STATE_TAB_NAME, rows=5000, cols=4)
        ws.update(
            [["local_in_work", "reviewed_changes", "confirmed_cancels", "completed_log"]],
            value_input_option="RAW",
        )
        return ws


@st.cache_resource
def get_orders_worksheet() -> gspread.Worksheet:
    """Кешируем объект листа заказов."""
    ss = get_spreadsheet()
    try:
        return ss.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return ss.get_worksheet(0)


# ── СОСТОЯНИЕ В GOOGLE SHEETS ─────────────────────────────────────────────────
# Единый кеш для всего состояния с коротким TTL (синхронизация между устройствами)

@st.cache_data(ttl=15)
def load_full_state_from_sheets() -> dict:
    """
    Загружает всё состояние за один вызов API.
    TTL=15 сек — быстрая синхронизация между устройствами.
    """
    try:
        rows = get_state_worksheet().get_all_values()
        if len(rows) < 2:
            return {"in_work": set(), "reviewed": set(), "confirmed": set(), "log": []}
        data_rows = rows[1:]
        return {
            "in_work":   {r[0] for r in data_rows if r[0].strip()},
            "reviewed":  {r[1] for r in data_rows if len(r) > 1 and r[1].strip()},
            "confirmed": {r[2] for r in data_rows if len(r) > 2 and r[2].strip()},
            "log":       [r[3] for r in data_rows if len(r) > 3 and r[3].strip()],
        }
    except Exception as e:
        st.warning(f"Не удалось загрузить состояние из Sheets: {e}")
        return {"in_work": set(), "reviewed": set(), "confirmed": set(), "log": []}


def save_state_to_sheets() -> None:
    """Сохраняет всё состояние одним запросом. Сбрасывает кеш для синхронизации."""
    try:
        ws        = get_state_worksheet()
        in_work   = list(st.session_state.local_in_work)
        reviewed  = list(st.session_state.reviewed_changes)
        confirmed = list(st.session_state.confirmed_cancels)
        log       = list(st.session_state.completed_log)
        max_len   = max(len(in_work), len(reviewed), len(confirmed), len(log), 1)

        def pad(lst: list) -> list:
            return lst + [""] * (max_len - len(lst))

        ws.clear()
        ws.update(
            [["local_in_work", "reviewed_changes", "confirmed_cancels", "completed_log"]]
            + list(zip(pad(in_work), pad(reviewed), pad(confirmed), pad(log))),
            value_input_option="RAW",
        )
        # Сбрасываем кеш — все устройства получат свежие данные через ≤15 сек
        load_full_state_from_sheets.clear()
    except Exception as e:
        st.warning(f"Не удалось сохранить состояние в Sheets: {e}")


def _log_action(oid, group: pd.DataFrame, store: str, action: str) -> None:
    ts       = datetime.now().strftime("%Y-%m-%d %H:%M")
    products = ";".join(group[C_PRODUCT].astype(str).tolist())
    qtys     = ";".join(group[C_QTY].astype(str).tolist())
    st.session_state.completed_log.append(
        f"{ts}|{oid}|{products}|{qtys}|{store}|{action}"
    )


# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────────────────

def _review_key(oid, edit_text: str) -> str:
    return f"{oid}||{str(edit_text).strip()}"


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


def _build_tags(
    comment_str: str,
    is_move_needed: bool,
    has_edit: bool,
    is_pz_item: bool,
    incoming: bool,
    is_cancelled: bool = False,
    extra_tag: str | None = None,
) -> str:
    tags = []
    if is_cancelled:       tags.append("🚫 ОТМЕНА")
    if "d" in comment_str: tags.append("📦 ДОСТАВКА")
    if is_move_needed:     tags.append("🚚 ПЕРЕМЕЩЕНИЕ")
    if has_edit:           tags.append(extra_tag if extra_tag else "⚠️ ИЗМЕНЕНИЕ")
    if is_pz_item:         tags.append("⏳ ПЗ")
    if incoming:           tags.append("🚚 ЕДЕТ")
    return " | ".join(tags)


# ── АВТООБНОВЛЕНИЕ ────────────────────────────────────────────────────────────
st.session_state.setdefault("auto_refresh_enabled", True)

if st.session_state.auto_refresh_enabled:
    refresh_count = st_autorefresh(interval=REFRESH_MS, key="data_refresh")
else:
    refresh_count = 0

# ── ИНИЦИАЛИЗАЦИЯ СЕССИИ ─────────────────────────────────────────────────────
# Все состояния загружаем одним запросом к Sheets
if "reviewed_changes" not in st.session_state:
    state = load_full_state_from_sheets()
    st.session_state.local_in_work         = state["in_work"]
    st.session_state.reviewed_changes      = state["reviewed"]
    st.session_state.confirmed_cancels     = state["confirmed"]
    st.session_state.completed_log         = state["log"]
    st.session_state.prev_order_ids        = set()
    st.session_state.new_orders_alert      = set()
    st.session_state.new_orders_alert_time = None
    st.session_state.last_sync             = "Не обновлялось"
else:
    # При каждом рендере синхронизируем только in_work (меняется чаще всего)
    # reviewed/confirmed/log синхронизируются при явных действиях пользователя
    fresh = load_full_state_from_sheets()
    st.session_state.local_in_work = fresh["in_work"]

# ── ЗАГРУЗКА ДАННЫХ ───────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_data() -> tuple[pd.DataFrame, dict]:
    sheet    = get_orders_worksheet()
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

    status_idx = headers.index("Статус") if "Статус" in headers else len(headers) - 1

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
        "STATUS":  status_idx,
    }

    df = pd.DataFrame(raw_data[START_ROW - 1:], columns=headers)
    df["_sheet_row"] = range(START_ROW, START_ROW + len(df))

    product_col = headers[col_map["PRODUCT"]]
    df = df[df[product_col].str.strip().astype(bool)].copy()

    st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
    return df, col_map


if refresh_count > 0:
    load_data.clear()


def update_google_cells(group: pd.DataFrame, col_map: dict, updates: dict) -> None:
    sheet     = get_orders_worksheet()
    cell_list = [
        gspread.Cell(row=int(row_num), col=col_map[key] + 1, value=val)
        for key, val in updates.items()
        for row_num in group["_sheet_row"]
    ]
    if cell_list:
        sheet.update_cells(cell_list, value_input_option="USER_ENTERED")
    load_data.clear()


# ── ПОДГОТОВКА ДАННЫХ ─────────────────────────────────────────────────────────
df_mem, C = load_data()

if df_mem.empty or not C:
    st.error("Не удалось загрузить данные. Проверьте таблицу и настройки.")
    st.stop()

cols = df_mem.columns
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

# Оповещения о новых заказах
current_order_ids = set(df_mem[C_ORDER].unique())
if st.session_state.prev_order_ids:
    new_ids = current_order_ids - st.session_state.prev_order_ids
    if new_ids:
        st.session_state.new_orders_alert      = new_ids
        st.session_state.new_orders_alert_time = datetime.now()
st.session_state.prev_order_ids = current_order_ids

# Основной датафрейм с вычисленными служебными колонками
work_base = df_mem.copy()
work_base["_target_store"] = work_base[C_COMMENT].apply(identify_target_store)
work_base["_is_cancelled"] = (
    work_base[C_STATUS].str.strip().str.lower() == CANCELLED_VAL.lower()
)

# ── САЙДБАР ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏢 Меню Авеню")

auto_refresh = st.sidebar.toggle(
    "🔄 Автообновление (10 мин)",
    value=st.session_state.auto_refresh_enabled,
    key="auto_refresh_toggle",
)
st.session_state.auto_refresh_enabled = auto_refresh

menu = st.sidebar.selectbox("Выберите раздел:", MENU_OPTIONS)

if st.sidebar.button("🚪 Выйти"):
    cookie_manager.delete(COOKIE_NAME)
    st.session_state.password_correct = False
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.caption(f"🔄 Последняя синхронизация: **{st.session_state.last_sync}**")

if st.sidebar.button("🔃 Обновить данные сейчас"):
    load_data.clear()
    load_full_state_from_sheets.clear()
    st.rerun()

# ── ЛОГИКА ОТОБРАЖЕНИЯ МАГАЗИНА ───────────────────────────────────────────────

def render_store(current_store: str) -> None:
    st.title(f"🏪 Заказы: {current_store}")

    wh_pattern_alert = "Горб|Сток" if current_store == STORE_GORB else "Пекин"
    store_order_ids = set(
        work_base[
            work_base[C_WH].str.contains(wh_pattern_alert, case=False, na=False)
            & ~work_base[C_WH].isin(PZ_LIST)
        ][C_ORDER].unique()
    )
    store_new_alert = {
        oid for oid in st.session_state.new_orders_alert
        if oid in store_order_ids
    }

    alert_time = st.session_state.get("new_orders_alert_time")
    if store_new_alert and alert_time and (datetime.now() - alert_time).total_seconds() < 10:
        st.success(
            f"🆕 Обнаружены новые заказы: "
            f"{', '.join(str(o) for o in sorted(store_new_alert))}"
        )

    is_pz_row = work_base[C_WH].isin(PZ_LIST)
    is_move   = work_base[C_MOVE] == TRUE_VAL

    wh_pattern = "Горб|Сток" if current_store == STORE_GORB else "Пекин"
    wh_match   = work_base[C_WH].str.contains(wh_pattern, case=False, na=False)

    is_f_match  = wh_match & ~is_pz_row
    is_pz_match = (
        (work_base[C_WH] == f"ПЗ {current_store}")
        & (work_base[C_INWORK] == TRUE_VAL)
    )
    is_incoming = is_move & (work_base["_target_store"] == current_store)

    _confirmed = {str(x) for x in st.session_state.confirmed_cancels}

    is_cancelled_store = (
        work_base["_is_cancelled"]
        & (is_f_match | is_pz_match | is_incoming)
        & (~work_base[C_ORDER].astype(str).isin(_confirmed))
    )

    base_mask = ((is_f_match | is_pz_match) & ~is_move) | is_incoming

    reviewed     = st.session_state.reviewed_changes
    edit_series  = work_base[C_EDIT].fillna("").str.strip()
    order_series = work_base[C_ORDER]
    has_unrev    = edit_series.astype(bool) & ~pd.Series(
        [
            _review_key(oid, et) in reviewed
            for oid, et in zip(order_series, edit_series)
        ],
        index=work_base.index,
    )

    not_confirmed_cancelled = ~(
        work_base["_is_cancelled"]
        & work_base[C_ORDER].astype(str).isin(_confirmed)
    )

    display_df = work_base[
        (base_mask & ((work_base[C_DONE] != TRUE_VAL) | has_unrev) & not_confirmed_cancelled)
        | is_cancelled_store
    ].copy()

    col1, col2 = st.columns(2)

    def _render_order_group(oid, group: pd.DataFrame, in_work_section: bool) -> None:
        comment_str = str(group[C_COMMENT].iloc[0]).lower()
        target      = group["_target_store"].iloc[0]
        incoming    = group[C_MOVE].iloc[0] == TRUE_VAL
        is_pz_item  = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
        edit_text   = group[C_EDIT].iloc[0]
        has_edit    = bool(str(edit_text).strip()) and _review_key(oid, edit_text) not in reviewed
        is_move_needed = target != current_store and target != "Общий" and not incoming
        cancelled   = group["_is_cancelled"].iloc[0]

        extra_tag = "⚠️ ПРАВКА" if in_work_section else None
        tag_str   = _build_tags(
            comment_str, is_move_needed, has_edit,
            is_pz_item, incoming, cancelled, extra_tag,
        )
        label = f"Заказ №{oid}{f' [{tag_str}]' if tag_str else ''}"

        with st.expander(label):
            if cancelled:
                st.error("🚫 Этот заказ отменён")

            if has_edit:
                prefix = "Правка" if in_work_section else "Изменение"
                st.error(f"{prefix}: {edit_text}")

            render_order_table(group, TABLE_COLS, COL_RENAME)

            if cancelled:
                if st.button(
                    "✅ Подтвердить отмену", key=f"confirm_cancel_{oid}",
                    type="primary", use_container_width=True,
                ):
                    update_google_cells(group, C, {"DONE": TRUE_VAL})
                    st.session_state.confirmed_cancels.add(str(oid))
                    st.session_state.local_in_work.discard(oid)
                    _log_action(oid, group, current_store, "cancel_confirmed")
                    save_state_to_sheets()
                    st.rerun()

            elif has_edit:
                btn_label = "Учесть правку" if in_work_section else "Учесть Изменение"
                if st.button(btn_label, key=f"rev_{'w' if in_work_section else 'n'}_{oid}"):
                    st.session_state.reviewed_changes.add(_review_key(oid, edit_text))
                    save_state_to_sheets()
                    st.rerun()

            elif in_work_section:
                if is_move_needed:
                    if st.button(
                        "🚛 ОТПРАВИТЬ ПЕРЕМЕЩЕНИЕ", key=f"mv_{oid}",
                        type="primary", use_container_width=True,
                    ):
                        update_google_cells(group, C, {"MOVE": TRUE_VAL})
                        st.session_state.local_in_work.discard(oid)
                        save_state_to_sheets()
                        st.rerun()
                else:
                    action_label = "✅ ПРИНЯТО И СОБРАНО" if incoming else "✅ ЗАВЕРШИТЬ СБОРКУ"
                    if st.button(
                        action_label, key=f"dn_{oid}",
                        type="primary", use_container_width=True,
                    ):
                        update_google_cells(group, C, {"DONE": TRUE_VAL, "MOVE": FALSE_VAL})
                        st.session_state.local_in_work.discard(oid)
                        st.session_state.reviewed_changes.discard(_review_key(oid, edit_text))
                        _log_action(oid, group, current_store, "done")
                        save_state_to_sheets()
                        st.rerun()

                    st.markdown("---")
                    if st.button(
                        "🚫 Отменить заказ", key=f"cancel_{oid}",
                        use_container_width=True,
                    ):
                        update_google_cells(group, C, {"STATUS": CANCELLED_VAL})
                        st.session_state.local_in_work.discard(oid)
                        save_state_to_sheets()
                        st.rerun()

            else:
                if st.button("В работу", key=f"w_{oid}", use_container_width=True):
                    st.session_state.local_in_work.add(oid)
                    save_state_to_sheets()
                    st.rerun()

    with col1:
        st.subheader("🆕 Новые / Изменения")
        new_items = display_df[~display_df[C_ORDER].isin(st.session_state.local_in_work)]
        for oid, group in new_items.groupby(C_ORDER, sort=False):
            _render_order_group(oid, group, in_work_section=False)

    with col2:
        st.subheader("🛠 В сборке")
        in_work_df = display_df[display_df[C_ORDER].isin(st.session_state.local_in_work)]
        for oid, group in in_work_df.groupby(C_ORDER, sort=False):
            _render_order_group(oid, group, in_work_section=True)


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
        & (~work_base["_is_cancelled"])
    ]
    st.dataframe(
        pz[TABLE_COLS].rename(columns=COL_RENAME),
        use_container_width=True, hide_index=True,
    )

elif menu == "✅ Выполненные сборки":
    st.title("✅ Последние собранные")
    done = work_base[
        (work_base[C_DONE] == TRUE_VAL) & (~work_base["_is_cancelled"])
    ].iloc[::-1].head(PREVIEW_ORDERS)
    st.dataframe(
        done[TABLE_COLS].rename(columns=COL_RENAME),
        use_container_width=True, hide_index=True,
    )

elif menu == "🚫 Отмененные заказы":
    st.title("🚫 Отменённые (подтверждённые)")
    _confirmed = {str(x) for x in st.session_state.confirmed_cancels}
    cancelled  = work_base[
        work_base["_is_cancelled"]
        & work_base[C_ORDER].astype(str).isin(_confirmed)
    ]
    if cancelled.empty:
        st.info("Подтверждённых отмен пока нет.")
    else:
        st.dataframe(
            cancelled[TABLE_COLS + [C_STATUS]].rename(columns=COL_RENAME),
            use_container_width=True, hide_index=True,
        )
