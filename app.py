import streamlit as st
import gspread
import pandas as pd
import json
import os
import time
from datetime import datetime
import extra_streamlit_components as stx
from streamlit_autorefresh import st_autorefresh
from google.oauth2.credentials import Credentials

# ── КОНСТАНТЫ ────────────────────────────────────────────────────────────────
DB_FILE      = "orders_persistent_state.json"
SHEET_ID     = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME     = "Заказы ИМ Авеню"
PZ_LIST      = ["ПЗ Пекин", "ПЗ Горбушка"]
START_ROW    = 26596
TRUE_VAL     = "TRUE"
FALSE_VAL    = "FALSE"

# ── НАСТРОЙКА СТРАНИЦЫ ───────────────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")

# ── ПЕРСИСТЕНТНОСТЬ (JSON) ────────────────────────────────────────────────────
def load_persistent_state() -> tuple[set, set]:
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE) as f:
                data = json.load(f)
            return (set(data.get("local_in_work", [])), set(data.get("reviewed_changes", [])))
        except (json.JSONDecodeError, OSError): pass
    return set(), set()

def save_persistent_state() -> None:
    try:
        with open(DB_FILE, "w") as f:
            json.dump({
                "local_in_work": list(st.session_state.local_in_work),
                "reviewed_changes": list(st.session_state.reviewed_changes),
            }, f)
    except OSError as e: st.warning(f"Не удалось сохранить: {e}")

# ── ИНИЦИАЛИЗАЦИЯ SESSION STATE ──────────────────────────────────────────────
if "local_in_work" not in st.session_state:
    saved_in_work, saved_reviewed = load_persistent_state()
    st.session_state.update({
        "local_in_work": saved_in_work,
        "reviewed_changes": saved_reviewed,
        "prev_order_ids": set(),
        "new_orders_alert": set(),
        "last_sync": "Не обновлялось",
        "password_correct": False
    })

# ── АВТОРИЗАЦИЯ (COOKIES) ─────────────────────────────────────────────────────
def check_password() -> bool:
    # 1. Если уже авторизован в текущей сессии (память сервера)
    if st.session_state.get("password_correct"):
        return True

    # 2. Инициализируем менеджер куки только если его нет (защита от дубликатов)
    if "cookie_manager" not in st.session_state:
        try:
            st.session_state.cookie_manager = stx.CookieManager(key="cookie_manager_v3")
        except Exception:
            return False # Пропускаем такт, если Streamlit еще не готов

    cm = st.session_state.cookie_manager

    # Пытаемся получить токен из браузера
    try:
        auth_token = cm.get("auth_token")
    except Exception:
        return False

    # Сверяем с секретами
    if auth_token == st.secrets.get("password"):
        st.session_state.password_correct = True
        return True

    # 3. Форма входа
    st.title("🔐 Вход в систему")
    pwd = st.text_input("Введите код доступа:", type="password")
    if st.button("Войти"):
        if pwd == st.secrets.get("password"):
            st.session_state.password_correct = True
            cm.set("auth_token", pwd, expires_at=datetime.now() + pd.Timedelta(days=30))
            st.rerun()
        else:
            st.error("❌ Неверный код")
    return False

# Сначала проверка доступа
if not check_password():
    st.stop()

# ── ДАННЫЕ И ОБНОВЛЕНИЕ ───────────────────────────────────────────────────────
st_autorefresh(interval=600_000, key="data_refresh")

@st.cache_resource
def get_client():
    try:
        gs = st.secrets["connections"]["gsheets"]
        creds = Credentials.from_authorized_user_info({
            "client_id": gs["client_id"], "client_secret": gs["client_secret"],
            "refresh_token": gs["refresh_token"], "type": "authorized_user",
        }, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Ошибка Google: {e}"); st.stop()

def get_worksheet():
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    try: return spreadsheet.worksheet(TAB_NAME)
    except: return spreadsheet.get_worksheet(0)

@st.cache_data(ttl=600)
def load_data_integrated():
    sheet = get_worksheet()
    raw_data = sheet.get_all_values()
    if not raw_data: return pd.DataFrame(), {}
    
    header_idx = next((i for i, r in enumerate(raw_data[:100]) if "Наименование" in r and "Склад" in r), -1)
    if header_idx == -1: return pd.DataFrame(), {}
    
    headers = [str(h).strip().replace("\n", " ") for h in raw_data[header_idx]]
    def col_idx(name): return headers.index(name)

    try:
        col_map = {
            "ORDER": col_idx("Наименование") - 1, "PRODUCT": col_idx("Наименование"),
            "QTY": col_idx("Кол-во"), "WH": col_idx("Склад"), "COMMENT": col_idx("Комментарий"),
            "EDIT": col_idx("Изменения заказа"), "INWORK": col_idx("Под ЗАКАЗ"),
            "MOVE": col_idx("Перемещение"), "DONE": col_idx("Собрано"),
            "STATUS": col_idx("Статус") if "Статус" in headers else len(headers) - 1,
        }
    except Exception as e: st.error(f"Колонки: {e}"); return pd.DataFrame(), {}

    df = pd.DataFrame(raw_data[START_ROW - 1:], columns=headers)
    df["_sheet_row"] = range(START_ROW, START_ROW + len(df))
    df = df[df[headers[col_map["PRODUCT"]]].str.strip() != ""].copy()
    st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
    return df, col_map

def update_google_cells(group, col_map, updates):
    sheet = get_worksheet()
    cell_list = [gspread.Cell(row=int(r), col=col_map[k]+1, value=v) for k,v in updates.items() for r in group["_sheet_row"]]
    sheet.update_cells(cell_list, value_input_option="USER_ENTERED")
    load_data_integrated.clear()

# ── ОБРАБОТКА ДАННЫХ ──────────────────────────────────────────────────────────
df_mem, C = load_data_integrated()
if df_mem.empty: st.stop()

cols = df_mem.columns
C_ORDER, C_PRODUCT, C_QTY, C_WH, C_COMMENT = cols[C["ORDER"]], cols[C["PRODUCT"]], cols[C["QTY"]], cols[C["WH"]], cols[C["COMMENT"]]
C_DONE, C_MOVE, C_STATUS, C_EDIT, C_INWORK = cols[C["DONE"]], cols[C["MOVE"]], cols[C["STATUS"]], cols[C["EDIT"]], cols[C["INWORK"]]

TABLE_COLS = [C_ORDER, C_PRODUCT, C_QTY, C_WH, C_COMMENT]
COL_RENAME = {C_ORDER: "Заказ", C_PRODUCT: "Товар", C_QTY: "Кол", C_WH: "Склад", C_COMMENT: "Коммент"}

curr_ids = set(df_mem[C_ORDER].unique())
if st.session_state.prev_order_ids and curr_ids - st.session_state.prev_order_ids:
    st.session_state.new_orders_alert = curr_ids - st.session_state.prev_order_ids
st.session_state.prev_order_ids = curr_ids

work_base = df_mem[~df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)].copy()
def get_target(c):
    c = str(c).lower()
    if any(x in c for x in ("пек", "пкн")): return "Пекин"
    if any(x in c for x in ("горб", "грб")): return "Горбушка"
    return "Общий"
work_base["_target_store"] = work_base[C_COMMENT].apply(get_target)

# ── САЙДБАР ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏢 Меню Авеню")
menu = st.sidebar.selectbox("Раздел:", ["🏪 ГОРБУШКА", "🏪 ПЕКИН", "🚚 В пути", "⏳ Под заказ", "✅ Собранные", "🚫 Отмененные"])

if st.sidebar.button("🔃 Обновить"):
    load_data_integrated.clear(); st.rerun()

if st.sidebar.button("🚪 Выйти"):
    if "cookie_manager" in st.session_state:
        st.session_state.cookie_manager.delete("auth_token")
    st.session_state.password_correct = False
    st.rerun()

# ── РЕНДЕР МАГАЗИНА ───────────────────────────────────────────────────────────
def render_store(store_name):
    st.title(f"🏪 {store_name}")
    wh_tag = "Горб" if "ГОРБ" in store_name else "Пекин"
    
    mask = (
        ((work_base[C_WH].str.contains(wh_tag, case=False, na=False)) & (work_base[C_MOVE] != TRUE_VAL)) |
        ((work_base[C_MOVE] == TRUE_VAL) & (work_base["_target_store"] == store_name.split()[-1].capitalize()))
    )
    disp = work_base[mask].copy()
    disp = disp[(disp[C_DONE] != TRUE_VAL) | ((disp[C_DONE] == TRUE_VAL) & (disp[C_EDIT] != "") & (~disp[C_ORDER].isin(st.session_state.reviewed_changes)))]

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🆕 Новые")
        items = disp[~disp[C_ORDER].isin(st.session_state.local_in_work)]
        for oid, group in items.groupby(C_ORDER, sort=False):
            with st.expander(f"Заказ №{oid}"):
                st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
                if st.button("В работу", key=f"btn_{oid}"):
                    st.session_state.local_in_work.add(oid); save_persistent_state(); st.rerun()
    with c2:
        st.subheader("🛠 В сборке")
        items = disp[disp[C_ORDER].isin(st.session_state.local_in_work)]
        for oid, group in items.groupby(C_ORDER, sort=False):
            with st.expander(f"Заказ №{oid}"):
                st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
                if st.button("✅ Собрано", key=f"dn_{oid}", type="primary"):
                    update_google_cells(group, C, {"DONE": TRUE_VAL, "MOVE": FALSE_VAL})
                    st.session_state.local_in_work.discard(oid); save_persistent_state(); st.rerun()

# ── МАРШРУТИЗАЦИЯ ─────────────────────────────────────────────────────────────
if "ГОРБУШКА" in menu: render_store("🏪 Магазин Горбушка")
elif "ПЕКИН" in menu: render_store("🏪 Магазин Пекин")
elif "В пути" in menu:
    st.dataframe(work_base[work_base[C_MOVE] == TRUE_VAL][TABLE_COLS].rename(columns=COL_RENAME), hide_index=True)
elif "Под заказ" in menu:
    st.dataframe(work_base[work_base[C_WH].isin(PZ_LIST)][TABLE_COLS].rename(columns=COL_RENAME), hide_index=True)
elif "Собранные" in menu:
    st.dataframe(work_base[work_base[C_DONE] == TRUE_VAL].tail(50)[TABLE_COLS].rename(columns=COL_RENAME), hide_index=True)
elif "Отмененные" in menu:
    st.dataframe(df_mem[df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)][TABLE_COLS].rename(columns=COL_RENAME), hide_index=True)
