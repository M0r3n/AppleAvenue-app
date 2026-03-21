import streamlit as st
import gspread
import pandas as pd
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from google.oauth2.credentials import Credentials

# 1. НАСТРОЙКИ СТРАНИЦЫ И БЕЗОПАСНОСТЬ
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state.password_correct = False
    if st.session_state.password_correct:
        return True

    st.title("🔐 Вход в систему")
    if "password" not in st.secrets:
        st.error("Критическая ошибка: Пароль не настроен в Secrets.")
        st.stop()

    pwd = st.text_input("Введите код доступа:", type="password")
    if st.button("Войти"):
        if pwd == st.secrets["password"]:
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("❌ Неверный код")
    return False

if not check_password():
    st.stop()

# Авто-обновление (10 минут)
st_autorefresh(interval=600_000, key="data_refresh")

# Инициализация состояний
for key, default in [
    ("local_in_work", set()), 
    ("reviewed_changes", set()), 
    ("prev_order_ids", set()), 
    ("last_sync", "Не обновлялось")
]:
    if key not in st.session_state:
        st.session_state[key] = default

# КОНСТАНТЫ (Твои настройки таблицы)
SHEET_ID = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME = "Заказы ИМ Авеню"
PZ_LIST  = ["ПЗ Пекин", "ПЗ Горбушка"]
START_WORKING_ROW = 26596

# 2. ПОДКЛЮЧЕНИЕ К GOOGLE (ОБЛАЧНОЕ)
@st.cache_resource
def get_client():
    try:
        gs_creds = st.secrets["connections"]["gsheets"]
        creds_info = {
            "client_id": gs_creds["client_id"],
            "client_secret": gs_creds["client_secret"],
            "refresh_token": gs_creds["refresh_token"],
            "type": "authorized_user",
        }
        creds = Credentials.from_authorized_user_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Ошибка авторизации Google: {e}")
        st.stop()

# 3. ЗАГРУЗКА И ОБРАБОТКА ДАННЫХ
@st.cache_data(ttl=600)
def load_data_integrated():
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        sheet = spreadsheet.worksheet(TAB_NAME)
    except:
        sheet = spreadsheet.get_worksheet(0)

    raw_data = sheet.get_all_values()
    if not raw_data: return pd.DataFrame(), {}

    # Умный поиск заголовков (ищем строку, где есть 'Наименование')
    header_idx = -1
    for i, row in enumerate(raw_data[:100]):
        if "Наименование" in row and "Склад" in row:
            if not any("ячейку" in str(c) for c in row):
                header_idx = i
                break
    if header_idx == -1: return pd.DataFrame(), {}

    headers = [str(h).strip().replace('\n', ' ') for h in raw_data[header_idx]]
    
    # Обработка данных с учетом стартовой строки
    df = pd.DataFrame(raw_data[START_WORKING_ROW-1:], columns=headers)
    # КРИТИЧНО: Привязываем реальный номер строки Google Таблицы
    df["_sheet_row"] = range(START_WORKING_ROW, START_WORKING_ROW + len(df))
    
    # Очистка от пустых строк
    df = df[df["Наименование"].str.strip() != ""].copy()

    # Маппинг индексов колонок (для gspread индексация с 0)
    col_map = {
        "ORDER": headers.index("Наименование") - 1, # Обычно ID заказа слева от названия
        "PRODUCT": headers.index("Наименование"),
        "QTY": headers.index("Кол-во"),
        "WH": headers.index("Склад"),
        "COMMENT": headers.index("Комментарий"),
        "EDIT": headers.index("Изменения заказа"),
        "INWORK": headers.index("Под ЗАКАЗ"),
        "MOVE": headers.index("Перемещение"),
        "DONE": headers.index("Собрано"),
        "STATUS": headers.index("Статус") if "Статус" in headers else len(headers)-1
    }
    st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
    return df, col_map

# ФУНКЦИЯ ЗАПИСИ ГАЛОЧЕК В ТАБЛИЦУ
def update_google_cells(group: pd.DataFrame, updates: dict):
    client = get_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    cell_list = []
    
    for col_idx, value in updates.items():
        for row_num in group["_sheet_row"]:
            # gspread использует 1-базовую индексацию (row=1, col=1)
            cell_list.append(gspread.Cell(row=int(row_num), col=int(col_idx) + 1, value=value))
    
    sheet.update_cells(cell_list, value_input_option="USER_ENTERED")
    load_data_integrated.clear() # Сбрасываем кэш, чтобы увидеть изменения

def identify_target_store(comment):
    c = str(comment).lower()
    if any(x in c for x in ["пек", "пкн", "pekin"]): return "Пекин"
    if any(x in c for x in ["горб", "грб", "gorb"]): return "Горбушка"
    return "Общий"

# 4. ПОДГОТОВКА ДАННЫХ
df_mem, C = load_data_integrated()
if df_mem.empty:
    st.warning("Данные не загружены. Проверьте структуру таблицы.")
    st.stop()

# Колонки для отображения
C_ORDER_NAME = df_mem.columns[C['ORDER']]
C_PRODUCT_NAME = df_mem.columns[C['PRODUCT']]
C_QTY_NAME = df_mem.columns[C['QTY']]
C_WH_NAME = df_mem.columns[C['WH']]
C_COMMENT_NAME = df_mem.columns[C['COMMENT']]
C_DONE_NAME = df_mem.columns[C['DONE']]
C_MOVE_NAME = df_mem.columns[C['MOVE']]
C_STATUS_NAME = df_mem.columns[C['STATUS']]
C_EDIT_NAME = df_mem.columns[C['EDIT']]
C_INWORK_NAME = df_mem.columns[C['INWORK']]

TABLE_COLS = [C_ORDER_NAME, C_PRODUCT_NAME, C_QTY_NAME, C_WH_NAME, C_COMMENT_NAME]
COL_RENAME = {C_ORDER_NAME: "Заказ", C_PRODUCT_NAME: "Товар", C_QTY_NAME: "Кол", C_WH_NAME: "Склад", C_COMMENT_NAME: "Коммент"}

# Фильтр отмененных
is_canceled = df_mem[C_STATUS_NAME].str.lower().str.contains("отмен", na=False)
work_base = df_mem[~is_canceled].copy()

# 5. ИНТЕРФЕЙС
st.sidebar.title("🏢 Меню Авеню")
menu = st.sidebar.selectbox("Выберите раздел:", [
    "🏪 Магазин: ГОРБУШКА", "🏪 Магазин: ПЕКИН", "🚚 Перемещения (Активные)", "⏳ Товар Под заказ", "✅ Выполненные сборки"
])

st.sidebar.caption(f"🔄 Синхронизация: {st.session_state.last_sync}")
if st.sidebar.button("🔃 Обновить вручную"):
    load_data_integrated.clear(); st.rerun()

# 6. ЛОГИКА РАЗДЕЛОВ
if "Магазин" in menu:
    current_store = "Горбушка" if "ГОРБУШКА" in menu else "Пекин"
    st.title(f"🏪 Заказы: {current_store}")

    # Фильтрация для конкретного магазина
    wh_keywords = ["Горб", "Сток"] if current_store == "Горбушка" else ["Пекин"]
    is_pz_row = work_base[C_WH_NAME].isin(PZ_LIST)
    is_f_match = work_base[C_WH_NAME].str.contains('|'.join(wh_keywords), case=False, na=False) & ~is_pz_row
    is_pz_match = (work_base[C_WH_NAME] == f"ПЗ {current_store}") & (work_base[C_INWORK_NAME] == "TRUE")

    # Основной датафрейм для отображения (включая перемещения к нам)
    display_df = work_base[
        ((is_f_match | is_pz_match) & (work_base[C_MOVE_NAME] != "TRUE")) | 
        ((work_base[C_MOVE_NAME] == "TRUE") & (work_base[C_COMMENT_NAME].apply(identify_target_store) == current_store))
    ].copy()

    # Убираем уже собранное (если нет новых правок)
    display_df = display_df[
        (display_df[C_DONE_NAME] != "TRUE") | 
        ((display_df[C_DONE_NAME] == "TRUE") & (display_df[C_EDIT_NAME] != "") & (~display_df[C_ORDER_NAME].isin(st.session_state.reviewed_changes)))
    ]

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🆕 Новые / Изменения")
        new_items = display_df[~display_df[C_ORDER_NAME].isin(st.session_state.local_in_work)]
        for oid, group in new_items.groupby(C_ORDER_NAME, sort=False):
            with st.expander(f"Заказ №{oid}"):
                st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
                if st.button("В работу", key=f"w_{oid}"):
                    st.session_state.local_in_work.add(oid); st.rerun()

    with col2:
        st.subheader("🛠 В сборке")
        in_work = display_df[display_df[C_ORDER_NAME].isin(st.session_state.local_in_work)]
        for oid, group in in_work.groupby(C_ORDER_NAME, sort=False):
            with st.expander(f"Заказ №{oid} 🛠"):
                st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
                
                target = identify_target_store(group[C_COMMENT_NAME].iloc[0])
                is_incoming = group[C_MOVE_NAME].iloc[0] == "TRUE"

                # Если товар нужно отправить в другой магазин
                if target != current_store and target != "Общий" and not is_incoming:
                    if st.button("🚛 Отправить перемещение", key=f"mv_{oid}"):
                        update_google_cells(group, {C['MOVE']: "TRUE"})
                        st.session_state.local_in_work.discard(oid); st.rerun()
                else:
                    # КНОПКА КОТОРАЯ СТАВИТ ГАЛОЧКУ
                    if st.button("✅ Завершить сборку", key=f"dn_{oid}", type="primary"):
                        # Записываем TRUE в колонку Собрано и FALSE в Перемещение
                        update_google_cells(group, {C['DONE']: "TRUE", C['MOVE']: "FALSE"})
                        st.session_state.local_in_work.discard(oid); st.rerun()

elif menu == "🚚 Перемещения (Активные)":
    st.title("🚚 В пути")
    moves = work_base[work_base[C_MOVE_NAME] == "TRUE"]
    if moves.empty: st.info("Активных перемещений нет")
    for oid, group in moves.groupby(C_ORDER_NAME, sort=False):
        with st.expander(f"Перемещение №{oid}"):
            st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
            if st.button("Удалить из списка", key=f"cl_mv_{oid}"):
                update_google_cells(group, {C['MOVE']: "FALSE"})
                st.rerun()

elif menu == "⏳ Товар Под заказ":
    st.title("⏳ Ожидание ПЗ")
    pz = work_base[work_base[C_WH_NAME].isin(PZ_LIST) & (work_base[C_INWORK_NAME] != "TRUE")]
    st.dataframe(pz[TABLE_COLS].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)

elif menu == "✅ Выполненные сборки":
    st.title("✅ Последние собранные")
    done = work_base[work_base[C_DONE_NAME] == "TRUE"].iloc[::-1].head(50)
    st.dataframe(done[TABLE_COLS].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)
