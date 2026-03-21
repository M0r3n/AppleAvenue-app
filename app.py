import streamlit as st
import gspread
import pandas as pd
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from google.oauth2.credentials import Credentials

# ─────────────────────────────────────────────
# 1. ЗАЩИТА И КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Заказы", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state.password_correct = False
    if st.session_state.password_correct:
        return True

    st.title("🔐 Вход в систему")
    if "password" not in st.secrets:
        st.error("Ошибка: Ключ 'password' не найден в Secrets.")
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

# Авто-обновление каждые 10 минут
st_autorefresh(interval=600_000, key="data_refresh")

# Локальная память сессии
if "local_in_work" not in st.session_state:
    st.session_state.local_in_work = set()
if "reviewed_changes" not in st.session_state:
    st.session_state.reviewed_changes = set()
if "prev_order_ids" not in st.session_state:
    st.session_state.prev_order_ids = set()
if "new_orders_alert" not in st.session_state:
    st.session_state.new_orders_alert = set()
if "last_sync" not in st.session_state:
    st.session_state.last_sync = "Не обновлялось"

# КОНСТАНТЫ
SHEET_ID = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME = "Заказы ИМ Авеню"
PZ_LIST  = ["ПЗ Пекин", "ПЗ Горбушка"]
START_WORKING_ROW = 26596

# ─────────────────────────────────────────────
# 2. ПОДКЛЮЧЕНИЕ GOOGLE (ОБЛАЧНОЕ)
# ─────────────────────────────────────────────
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
        creds = Credentials.from_authorized_user_info(
            creds_info, 
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Ошибка авторизации Google: {e}")
        st.stop()

# ─────────────────────────────────────────────
# 3. ЗАГРУЗКА И ОБРАБОТКА ДАННЫХ
# ─────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_data_integrated():
    try:
        client = get_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        try:
            sheet = spreadsheet.worksheet(TAB_NAME)
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.get_worksheet(0)

        raw_data = sheet.get_all_values()
        if not raw_data: return pd.DataFrame(), {}

        # Поиск заголовков
        header_idx = -1
        for i, row in enumerate(raw_data[:100]):
            row_clean = [str(c).strip() for c in row]
            if "Наименование" in row_clean and "Склад" in row_clean:
                if not any("В одну ячейку вписывается" in str(c) for c in row):
                    header_idx = i
                    break
        if header_idx == -1: return pd.DataFrame(), {}

        headers = [str(h).strip().replace('\n', ' ') for h in raw_data[header_idx]]
        final_headers = []
        counts = {}
        for h in headers:
            name = h if h else "Пусто"
            counts[name] = counts.get(name, 0) + 1
            final_headers.append(name if counts[name] == 1 else f"{name}_{counts[name]-1}")

        start_idx = START_WORKING_ROW - 1
        content_rows = raw_data[start_idx:] if len(raw_data) > start_idx else []

        df = pd.DataFrame(content_rows, columns=final_headers)
        # Индексация для Google Sheets (строки начинаются с 1)
        df["_sheet_row"] = range(START_WORKING_ROW + 1, START_WORKING_ROW + 1 + len(df))
        df = df[df["Наименование"].str.strip() != ""].copy()

        col_map = {
            "ORDER":   final_headers.index("Наименование") - 1,
            "PRODUCT": final_headers.index("Наименование"),
            "QTY":     final_headers.index("Кол-во"),
            "WH":      final_headers.index("Склад"),
            "COMMENT": final_headers.index("Комментарий"),
            "EDIT":    final_headers.index("Изменения заказа"),
            "INWORK":  final_headers.index("Под ЗАКАЗ"),
            "MOVE":    final_headers.index("Перемещение"),
            "DONE":    final_headers.index("Собрано"),
            "STATUS":  final_headers.index("Статус") if "Статус" in final_headers else len(final_headers) - 1,
        }
        st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
        return df, col_map
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {e}")
        return pd.DataFrame(), {}

def update_google_cells(group: pd.DataFrame, updates: dict):
    client = get_client()
    sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    cells = [
        gspread.Cell(row=int(r), col=col_idx + 1, value=value)
        for col_idx, value in updates.items()
        for r in group["_sheet_row"]
    ]
    sheet.update_cells(cells, value_input_option="USER_ENTERED")
    load_data_integrated.clear()

def identify_target_store(comment):
    c = str(comment).lower()
    if any(x in c for x in ["пек", "пкн", "pekin"]): return "Пекин"
    if any(x in c for x in ["горб", "грб", "gorb"]): return "Горбушка"
    return "Общий"

# ─────────────────────────────────────────────
# 4. ПОДГОТОВКА ДАННЫХ ДЛЯ ИНТЕРФЕЙСА
# ─────────────────────────────────────────────
df_mem, C = load_data_integrated()

if df_mem.empty:
    st.warning("Данные не найдены.")
    st.stop()

# Маппинг имен колонок
C_ORDER_NAME   = df_mem.columns[C['ORDER']]
C_PRODUCT_NAME = df_mem.columns[C['PRODUCT']]
C_QTY_NAME     = df_mem.columns[C['QTY']]
C_WH_NAME      = df_mem.columns[C['WH']]
C_COMMENT_NAME = df_mem.columns[C['COMMENT']]
C_EDIT_NAME    = df_mem.columns[C['EDIT']]
C_INWORK_NAME  = df_mem.columns[C['INWORK']]
C_MOVE_NAME    = df_mem.columns[C['MOVE']]
C_DONE_NAME    = df_mem.columns[C['DONE']]
C_STATUS_NAME  = df_mem.columns[C['STATUS']]

COL_RENAME = {
    C_ORDER_NAME: "Заказ", C_PRODUCT_NAME: "Товар", C_QTY_NAME: "Кол",
    C_WH_NAME: "Склад", C_COMMENT_NAME: "Коммент", C_DONE_NAME: "Собрано",
    C_STATUS_NAME: "Статус"
}

# Отслеживание новых заказов
current_order_ids = set(df_mem[C_ORDER_NAME].unique())
if st.session_state.prev_order_ids:
    st.session_state.new_orders_alert = current_order_ids - st.session_state.prev_order_ids
st.session_state.prev_order_ids = current_order_ids

is_canceled = df_mem[C_STATUS_NAME].str.lower().str.contains("отмен", na=False)
canceled_df = df_mem[is_canceled]
work_base   = df_mem[~is_canceled]

# ─────────────────────────────────────────────
# 5. ИНТЕРФЕЙС / САЙДБАР
# ─────────────────────────────────────────────
st.sidebar.title("🏢 Меню Авеню")
menu = st.sidebar.selectbox("Раздел:", [
    "🏪 Магазин: ГОРБУШКА", "🏪 Магазин: ПЕКИН", "⏳ Товар Под заказ",
    "🚚 Перемещения (Активные)", "✅ Выполненные сборки", "🚫 Отмененные заказы"
])

st.sidebar.markdown("---")
st.sidebar.caption(f"🔄 Обновлено: {st.session_state.last_sync}")
if st.sidebar.button("🔃 Обновить сейчас"):
    load_data_integrated.clear()
    st.rerun()

# ─────────────────────────────────────────────
# 6. ЛОГИКА СТРАНИЦ МАГАЗИНОВ
# ─────────────────────────────────────────────
if "Магазин" in menu:
    current_page_store = "Горбушка" if "ГОРБУШКА" in menu else "Пекин"
    st.title(f"🏪 Заказы: {current_page_store}")

    # Алерт о новых заказах
    store_new_alert = {oid for oid in st.session_state.new_orders_alert if oid in work_base[C_ORDER_NAME].values}
    if store_new_alert:
        st.success(f"🆕 Новые заказы: {', '.join(str(o) for o in sorted(store_new_alert))}")

    # Фильтрация склада
    wh_keywords = ["Горб", "Сток"] if current_page_store == "Горбушка" else ["Пекин"]
    is_pz_row  = work_base[C_WH_NAME].isin(PZ_LIST)
    is_f_match = work_base[C_WH_NAME].str.contains('|'.join(wh_keywords), case=False, na=False) & ~is_pz_row
    is_pz_match = (work_base[C_WH_NAME] == f"ПЗ {current_page_store}") & (work_base[C_INWORK_NAME] == "TRUE")

    sending_df   = work_base[(is_f_match | is_pz_match) & (work_base[C_MOVE_NAME] != "TRUE")]
    receiving_df = work_base[(work_base[C_MOVE_NAME] == "TRUE") & (work_base[C_COMMENT_NAME].apply(identify_target_store) == current_page_store)]

    display_df = pd.concat([sending_df, receiving_df]).drop_duplicates(subset=["_sheet_row"])
    # Скрыть собранное, если нет активных правок
    display_df = display_df[
        (display_df[C_DONE_NAME] != "TRUE") |
        ((display_df[C_DONE_NAME] == "TRUE") & (display_df[C_EDIT_NAME] != "") & (~display_df[C_ORDER_NAME].isin(st.session_state.reviewed_changes)))
    ]
    display_df["_target_store"] = display_df[C_COMMENT_NAME].apply(identify_target_store)

    col1, col2 = st.columns(2)
    TABLE_COLS = [C_ORDER_NAME, C_PRODUCT_NAME, C_QTY_NAME, C_WH_NAME, C_COMMENT_NAME]

    with col1:
        st.subheader("🆕 Новые / Изменения")
        new_items = display_df[~display_df[C_ORDER_NAME].isin(st.session_state.local_in_work)]
        if new_items.empty: st.info("Нет новых заказов")
        for oid, group in new_items.groupby(C_ORDER_NAME, sort=False):
            target = group["_target_store"].iloc[0]
            is_incoming = group[C_MOVE_NAME].iloc[0] == "TRUE"
            needs_move = target != current_page_store and target != "Общий" and not is_incoming
            is_pz_item = (group[C_WH_NAME].isin(PZ_LIST)).any() and (group[C_INWORK_NAME] == "TRUE").any()
            has_active_edit = (group[C_EDIT_NAME] != "").any() and oid not in st.session_state.reviewed_changes

            tag = ((" ⚠️ ПРАВКА" if has_active_edit else "") + (" ⏳ ПЗ" if is_pz_item else "") + 
                   (" 🚚 В ПУТИ" if is_incoming else "") + (f" 📦 В {target.upper()}" if needs_move else ""))

            with st.expander(f"Заказ №{oid}{tag}"):
                if has_active_edit: st.error(f"Правка: {group[C_EDIT_NAME].iloc[0]}")
                st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
                if has_active_edit:
                    if st.button("Изменения учтены", key=f"rev_new_{oid}"):
                        st.session_state.reviewed_changes.add(oid); st.rerun()
                elif st.button("В работу", key=f"btn_work_{oid}"):
                    st.session_state.local_in_work.add(oid); st.rerun()

    with col2:
        st.subheader("🛠 В работе")
        in_work_items = display_df[display_df[C_ORDER_NAME].isin(st.session_state.local_in_work)]
        if in_work_items.empty: st.info("Пусто")
        for oid, group in in_work_items.groupby(C_ORDER_NAME, sort=False):
            target = group["_target_store"].iloc[0]
            is_incoming = group[C_MOVE_NAME].iloc[0] == "TRUE"
            needs_move = target != current_page_store and target != "Общий" and not is_incoming
            is_pz_item = (group[C_WH_NAME].isin(PZ_LIST)).any() and (group[C_INWORK_NAME] == "TRUE").any()
            has_active_edit = (group[C_EDIT_NAME] != "").any() and oid not in st.session_state.reviewed_changes

            tag = (" ⚠️ ПРАВКА" if has_active_edit else "") + (" ⏳ ПЗ" if is_pz_item else "")

            with st.expander(f"Заказ №{oid}{tag}"):
                if has_active_edit:
                    st.error(f"Правка: {group[C_EDIT_NAME].iloc[0]}")
                    if st.button("Изменения учтены", key=f"rev_work_{oid}"):
                        st.session_state.reviewed_changes.add(oid); st.rerun()
                else:
                    st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
                    if needs_move:
                        if st.button("🚛 В перемещения", key=f"mv_btn_{oid}"):
                            update_google_cells(group, {C['MOVE']: "TRUE"})
                            st.session_state.local_in_work.discard(oid); st.rerun()
                    else:
                        label = "✅ Принято/Собрано" if is_incoming else "✅ Завершить сборку"
                        if st.button(label, key=f"btn_done_{oid}", type="primary"):
                            update_google_cells(group, {C['DONE']: "TRUE", C['MOVE']: "FALSE"})
                            st.session_state.local_in_work.discard(oid)
                            st.session_state.reviewed_changes.discard(oid); st.rerun()

# ─────────────────────────────────────────────
# 7. ОСТАЛЬНЫЕ РАЗДЕЛЫ
# ─────────────────────────────────────────────
elif menu == "⏳ Товар Под заказ":
    st.title("⏳ Ожидание ПЗ")
    pz_waiting = work_base[work_base[C_WH_NAME].isin(PZ_LIST) & (work_base[C_INWORK_NAME] != "TRUE") & (work_base[C_DONE_NAME] != "TRUE")]
    st.dataframe(pz_waiting[TABLE_COLS].iloc[::-1].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)

elif menu == "🚚 Перемещения (Активные)":
    st.title("🚚 Активные перемещения")
    active_moves = work_base[work_base[C_MOVE_NAME] == "TRUE"]
    if active_moves.empty: st.info("Нет активных перемещений")
    for oid, group in active_moves.groupby(C_ORDER_NAME, sort=False):
        with st.expander(f"Перемещение №{oid}"):
            st.table(group[TABLE_COLS].rename(columns=COL_RENAME))
            if st.button("Выполнено (Удалить)", key=f"mv_cl_{oid}"):
                update_google_cells(group, {C['MOVE']: "FALSE"})
                st.rerun()

elif menu == "✅ Выполненные сборки":
    st.title("✅ Архив (Последние 100)")
    done_df = work_base[work_base[C_DONE_NAME] == "TRUE"].iloc[::-1].head(100)
    st.dataframe(done_df[[C_ORDER_NAME, C_PRODUCT_NAME, C_QTY_NAME, C_WH_NAME, C_DONE_NAME]].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)

elif menu == "🚫 Отмененные заказы":
    st.title("🚫 Отмененные заказы")
    st.dataframe(canceled_df[[C_ORDER_NAME, C_PRODUCT_NAME, C_QTY_NAME, C_WH_NAME, C_COMMENT_NAME, C_STATUS_NAME]].iloc[::-1].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)
