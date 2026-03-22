import streamlit as st
import gspread
import pandas as pd
import json
import os
import time
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from google.oauth2.credentials import Credentials
import extra_streamlit_components as stx

# ── КОНСТАНТЫ ────────────────────────────────────────────────────────────────
DB_FILE   = "orders_persistent_state.json"
SHEET_ID  = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME  = "Заказы ИМ Авеню"
PZ_LIST   = ["ПЗ Пекин", "ПЗ Горбушка"]
START_ROW = 26596
TRUE_VAL  = "TRUE"
FALSE_VAL = "FALSE"

COOKIE_NAME    = "avenue_auth_status"
COOKIE_VALUE   = "authorized"
COOKIE_DAYS    = 30
REFRESH_MS     = 600_000  # 10 минут
PREVIEW_ORDERS = 50        # кол-во заказов в «Выполненных»

# ── НАСТРОЙКА СТРАНИЦЫ ───────────────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")

# ── АВТОРИЗАЦИЯ ──────────────────────────────────────────────────────────────
cookie_manager = stx.CookieManager(key="avenue_auth_manager_v3")

def check_password() -> bool:
    if st.session_state.get("password_correct"):
        return True
    all_cookies = cookie_manager.get_all()
    if all_cookies is None:
        time.sleep(0.8)
        all_cookies = cookie_manager.get_all()
    if all_cookies and str(all_cookies.get(COOKIE_NAME)) == COOKIE_VALUE:
        st.session_state.password_correct = True
        return True
    st.title("🔐 Вход в систему")
    if "password" not in st.secrets:
        st.error("Критическая ошибка: Пароль не настроен в Secrets.")
        st.stop()
    pwd = st.text_input("Введите код доступа:", type="password", key="login_input")
    remember = st.checkbox("Запомнить меня на этом устройстве", value=True)
    if st.button("Войти", key="login_btn", type="primary"):
        if pwd == st.secrets["password"]:
            st.session_state.password_correct = True
            if remember:
                with st.spinner("Запоминаем устройство..."):
                    cookie_manager.set(
                        COOKIE_NAME,
                        COOKIE_VALUE,
                        expires_at=datetime.now() + timedelta(days=COOKIE_DAYS),
                        key="save_cookie_op",
                    )
                    time.sleep(1.2)
            st.rerun()
        else:
            st.error("❌ Неверный код")
    return False

if not check_password():
    st.stop()

# ── ПОСТОЯННОЕ СОСТОЯНИЕ (JSON) ───────────────────────────────────────────────

def load_persistent_state() -> tuple[set, set]:
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE) as f:
                data = json.load(f)
            return (
                set(data.get("local_in_work", [])),
                set(data.get("reviewed_changes", [])),
            )
        except (json.JSONDecodeError, OSError):
            pass
    return set(), set()

def save_persistent_state() -> None:
    try:
        with open(DB_FILE, "w") as f:
            json.dump(
                {
                    "local_in_work": list(st.session_state.local_in_work),
                    "reviewed_changes": list(st.session_state.reviewed_changes),
                },
                f,
            )
    except OSError as e:
        st.warning(f"Не удалось сохранить состояние: {e}")

# ── ИНИЦИАЛИЗАЦИЯ SESSION STATE ───────────────────────────────────────────────
st_autorefresh(interval=REFRESH_MS, key="data_refresh")

if "local_in_work" not in st.session_state:
    saved_in_work, saved_reviewed = load_persistent_state()
    st.session_state.local_in_work    = saved_in_work
    st.session_state.reviewed_changes = saved_reviewed
    st.session_state.prev_order_ids   = set()
    st.session_state.new_orders_alert = set()
    st.session_state.last_sync        = "Не обновлялось"

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────

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
    except Exception as e:
        st.error(f"Ошибка авторизации Google: {e}")
        st.stop()

def get_worksheet() -> gspread.Worksheet:
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        return spreadsheet.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return spreadsheet.get_worksheet(0)

@st.cache_data(ttl=600)
def load_data_integrated() -> tuple[pd.DataFrame, dict]:
    sheet    = get_worksheet()
    raw_data = sheet.get_all_values()
    if not raw_data:
        return pd.DataFrame(), {}
    header_idx = next(
        (i for i, row in enumerate(raw_data[:100]) if "Наименование" in row and "Склад" in row),
        -1,
    )
    if header_idx == -1: return pd.DataFrame(), {}
    headers = [str(h).strip().replace("\n", " ") for h in raw_data[header_idx]]
    def col_idx(name: str) -> int:
        return headers.index(name)
    col_map = {
        "ORDER": col_idx("Наименование") - 1, "PRODUCT": col_idx("Наименование"),
        "QTY": col_idx("Кол-во"), "WH": col_idx("Склад"), "COMMENT": col_idx("Комментарий"),
        "EDIT": col_idx("Изменения заказа"), "INWORK": col_idx("Под ЗАКАЗ"),
        "MOVE": col_idx("Перемещение"), "DONE": col_idx("Собрано"),
        "STATUS": col_idx("Статус") if "Статус" in headers else len(headers) - 1,
    }
    df = pd.DataFrame(raw_data[START_ROW - 1:], columns=headers)
    df["_sheet_row"] = range(START_ROW, START_ROW + len(df))
    df = df[df[headers[col_map["PRODUCT"]]].str.strip().astype(bool)].copy()
    st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
    return df, col_map

def update_google_cells(group: pd.DataFrame, col_map: dict, updates: dict) -> None:
    sheet = get_worksheet()
    cell_list = [
        gspread.Cell(row=int(row_num), col=col_map[key] + 1, value=val)
        for key, val in updates.items() for row_num in group["_sheet_row"]
    ]
    sheet.update_cells(cell_list, value_input_option="USER_ENTERED")
    load_data_integrated.clear()

# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────────────────

def identify_target_store(comment: str) -> str:
    c = str(comment).lower()
    if "d" in c: return "Горбушка"
    if any(k in c for k in ["пек", "пкн", "pekin"]): return "Пекин"
    if any(k in c for k in ["горб", "грб", "gorb"]): return "Горбушка"
    return "Общий"

def render_order_table(group: pd.DataFrame, table_cols: list, col_rename: dict) -> None:
    st.table(group[table_cols].rename(columns=col_rename))

def get_warehouse_squares(group: pd.DataFrame) -> str:
    """Генерирует только цветные квадраты складов."""
    all_whs = " ".join(group[C_WH].fillna("").astype(str)).lower()
    squares = []
    if "сток" in all_whs: squares.append("🟦")
    if "горб" in all_whs: squares.append("🟪")
    if "пекин" in all_whs: squares.append("🟩")
    return "".join(squares)

def build_header_with_squares(oid, tags_str, squares):
    """Сборка заголовка с выравниванием квадратов в ровную линию по правому краю."""
    text = f"Заказ №{oid}{tags_str}"
    
    # Чтобы квадраты стояли в один ровный столбец, мы используем метод компенсации длины.
    # Общая длина текстовой части до начала квадратов (примерно 55 символов для Wide layout).
    # Используем неразрывный пробел \u00A0 для точности.
    target_length = 50
    current_length = len(text)
    
    padding = max(1, target_length - current_length)
    # \u2001 — это фиксированный широкий пробел (Em Quad) для надежного выравнивания
    spacer = "\u2001" * padding
    
    return f"{text}{spacer}{squares}"

# ── ЗАГРУЗКА ДАННЫХ ───────────────────────────────────────────────────────────
df_mem, C = load_data_integrated()
if df_mem.empty or not C:
    st.error("Не удалось загрузить данные.")
    st.stop()

cols = df_mem.columns
C_ORDER, C_PRODUCT, C_QTY, C_WH, C_COMMENT = cols[C["ORDER"]], cols[C["PRODUCT"]], cols[C["QTY"]], cols[C["WH"]], cols[C["COMMENT"]]
C_DONE, C_MOVE, C_STATUS, C_EDIT, C_INWORK = cols[C["DONE"]], cols[C["MOVE"]], cols[C["STATUS"]], cols[C["EDIT"]], cols[C["INWORK"]]

TABLE_COLS, COL_RENAME = [C_ORDER, C_PRODUCT, C_QTY, C_WH, C_COMMENT], {C_ORDER: "Заказ", C_PRODUCT: "Товар", C_QTY: "Кол", C_WH: "Склад", C_COMMENT: "Коммент"}

current_order_ids = set(df_mem[C_ORDER].unique())
if st.session_state.prev_order_ids:
    new_ids = current_order_ids - st.session_state.prev_order_ids
    if new_ids: st.session_state.new_orders_alert = new_ids
st.session_state.prev_order_ids = current_order_ids

work_base = df_mem[~df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)].copy()
work_base["_target_store"] = work_base[C_COMMENT].apply(identify_target_store)

# ── САЙДБАР ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏢 Меню Авеню")
menu = st.sidebar.selectbox("Выберите раздел:", ["🏪 Магазин: ГОРБУШКА", "🏪 Магазин: ПЕКИН", "🚚 Перемещения (Активные)", "⏳ Товар Под заказ", "✅ Выполненные сборки", "🚫 Отмененные заказы"])

if st.sidebar.button("🚪 Выйти"):
    cookie_manager.delete(COOKIE_NAME)
    st.session_state.password_correct = False
    st.rerun()

st.sidebar.caption(f"🔄 Синхронизация: {st.session_state.last_sync}")
if st.sidebar.button("🔃 Обновить вручную"):
    load_data_integrated.clear()
    st.rerun()

# ── РАЗДЕЛ: МАГАЗИН ───────────────────────────────────────────────────────────

def render_store(current_store: str) -> None:
    st.title(f"🏪 Заказы: {current_store}")

    store_new_alert = {oid for oid in st.session_state.new_orders_alert if oid in work_base[C_ORDER].values}
    if store_new_alert:
        st.success(f"🆕 Новые заказы: {', '.join(str(o) for o in sorted(store_new_alert))}")
    
    if current_store == "Горбушка":
        wh_match = work_base[C_WH].str.contains("Горб|Сток", case=False, na=False)
    else:
        wh_match = work_base[C_WH].str.contains("Пекин", case=False, na=False)

    is_pz_row = work_base[C_WH].isin(PZ_LIST)
    is_f_match = wh_match & ~is_pz_row
    is_pz_match = (work_base[C_WH] == f"ПЗ {current_store}") & (work_base[C_INWORK] == TRUE_VAL)
    is_move = work_base[C_MOVE] == TRUE_VAL
    is_incoming = is_move & (work_base["_target_store"] == current_store)

    display_df = work_base[((is_f_match | is_pz_match) & ~is_move) | is_incoming].copy()
    display_df = display_df[(display_df[C_DONE] != TRUE_VAL) | ((display_df[C_EDIT] != "") & (~display_df[C_ORDER].isin(st.session_state.reviewed_changes)))]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🆕 Новые / Изменения")
        new_items = display_df[~display_df[C_ORDER].isin(st.session_state.local_in_work)]
        for oid, group in new_items.groupby(C_ORDER, sort=False):
            comment_str = str(group[C_COMMENT].iloc[0]).lower()
            target = group["_target_store"].iloc[0]
            incoming = group[C_MOVE].iloc[0] == TRUE_VAL
            is_pz_item = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
            has_edit = (group[C_EDIT] != "").any() and oid not in st.session_state.reviewed_changes
            is_move_needed = (target != current_store and target != "Общий" and not incoming)
            
            tags = []
            if "d" in comment_str: tags.append("📦 ДОСТАВКА")
            if is_move_needed: tags.append("🚚 ПЕРЕМЕЩЕНИЕ")
            if has_edit: tags.append("⚠️ ИЗМЕНЕНИЕ")
            if is_pz_item: tags.append("⏳ ПЗ")
            if incoming: tags.append("🚚 ЕДЕТ")
            
            tag_str = f" [{ ' | '.join(tags) }]" if tags else ""
            squares = get_warehouse_squares(group)
            header_label = build_header_with_squares(oid, tag_str, squares)
            
            with st.expander(header_label):
                if has_edit: st.error(f"Изменение: {group[C_EDIT].iloc[0]}")
                render_order_table(group, TABLE_COLS, COL_RENAME)
                
                if has_edit:
                    if st.button("Учесть Изменение", key=f"rev_n_{oid}"):
                        st.session_state.reviewed_changes.add(oid)
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
            comment_str = str(group[C_COMMENT].iloc[0]).lower()
            target = group["_target_store"].iloc[0]
            incoming = group[C_MOVE].iloc[0] == TRUE_VAL
            is_pz_item = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
            has_edit = (group[C_EDIT] != "").any() and oid not in st.session_state.reviewed_changes
            is_move_needed = (target != current_store and target != "Общий" and not incoming)
            
            tags = []
            if "d" in comment_str: tags.append("📦 ДОСТАВКА")
            if is_move_needed: tags.append("🚚 ПЕРЕМЕЩЕНИЕ")
            if has_edit: tags.append("⚠️ ПРАВКА")
            if is_pz_item: tags.append("⏳ ПЗ")
            
            tag_str = f" [{ ' | '.join(tags) }]" if tags else ""
            squares = get_warehouse_squares(group)
            header_label = build_header_with_squares(oid, tag_str, squares)
            
            with st.expander(header_label):
                if has_edit: st.error(f"Правка: {group[C_EDIT].iloc[0]}")
                render_order_table(group, TABLE_COLS, COL_RENAME)
                
                if has_edit:
                    if st.button("Учесть правку", key=f"rev_w_{oid}"):
                        st.session_state.reviewed_changes.add(oid)
                        save_persistent_state()
                        st.rerun()
                elif is_move_needed:
                    if st.button("🚛 ОТПРАВИТЬ ПЕРЕМЕЩЕНИЕ", key=f"mv_{oid}", type="primary", use_container_width=True):
                        update_google_cells(group, C, {"MOVE": TRUE_VAL})
                        st.session_state.local_in_work.discard(oid)
                        save_persistent_state()
                        st.rerun()
                else:
                    label = "✅ ПРИНЯТО И СОБРАНО" if incoming else "✅ ЗАВЕРШИТЬ СБОРКУ"
                    if st.button(label, key=f"dn_{oid}", type="primary", use_container_width=True):
                        update_google_cells(group, C, {"DONE": TRUE_VAL, "MOVE": FALSE_VAL})
                        st.session_state.local_in_work.discard(oid)
                        st.session_state.reviewed_changes.discard(oid)
                        save_persistent_state()
                        st.rerun()

# ── МАРШРУТИЗАЦИЯ ─────────────────────────────────────────────────────────────
if "Магазин" in menu:
    render_store("Горбушка" if "ГОРБУШКА" in menu else "Пекин")
elif menu == "🚚 Перемещения (Активные)":
    st.title("🚚 В пути")
    moves = work_base[work_base[C_MOVE] == TRUE_VAL]
    for oid, group in moves.groupby(C_ORDER, sort=False):
        squares = get_warehouse_squares(group)
        header = build_header_with_squares(oid, " [🚚 АКТИВНО]", squares)
        with st.expander(header):
            render_order_table(group, TABLE_COLS, COL_RENAME)
            if st.button("Удалить из списка", key=f"cl_mv_{oid}"):
                update_google_cells(group, C, {"MOVE": FALSE_VAL})
                st.rerun()
elif menu == "⏳ Товар Под заказ":
    st.title("⏳ Ожидание ПЗ")
    pz = work_base[work_base[C_WH].isin(PZ_LIST) & (work_base[C_INWORK] != TRUE_VAL) & (work_base[C_DONE] != TRUE_VAL)]
    st.dataframe(pz[TABLE_COLS].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)
elif menu == "✅ Выполненные сборки":
    st.title("✅ Последние собранные")
    done = work_base[work_base[C_DONE] == TRUE_VAL].iloc[::-1].head(PREVIEW_ORDERS)
    st.dataframe(done[TABLE_COLS].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)
elif menu == "🚫 Отмененные заказы":
    st.title("🚫 Отмененные")
    st.dataframe(df_mem[df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)][TABLE_COLS + [C_STATUS]].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)
