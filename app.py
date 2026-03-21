import streamlit as st
import gspread
import pandas as pd
import json
import os
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from google.oauth2.credentials import Credentials

# ── КОНСТАНТЫ ────────────────────────────────────────────────────────────────
DB_FILE      = "orders_persistent_state.json"
SHEET_ID     = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME     = "Заказы ИМ Авеню"
PZ_LIST      = ["ПЗ Пекин", "ПЗ Горбушка"]
START_ROW    = 26596          # первая строка данных на листе
TRUE_VAL     = "TRUE"
FALSE_VAL    = "FALSE"

# ── ПЕРСИСТЕНТНОСТЬ ───────────────────────────────────────────────────────────
def load_persistent_state() -> tuple[set, set]:
    """Читает local_in_work и reviewed_changes из JSON-файла."""
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
    """Сохраняет текущее состояние session_state на диск."""
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


# ── СТРАНИЦА И АВТОРИЗАЦИЯ ───────────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")


def check_password() -> bool:
    if st.session_state.get("password_correct"):
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

# Авто-обновление каждые 10 минут
st_autorefresh(interval=600_000, key="data_refresh")

# Инициализация session_state (выполняется один раз)
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
    """Создаёт и кеширует авторизованный gspread-клиент."""
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
    """Возвращает нужный лист; при ошибке берёт первый."""
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        return spreadsheet.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return spreadsheet.get_worksheet(0)


@st.cache_data(ttl=600)
def load_data_integrated() -> tuple[pd.DataFrame, dict]:
    """
    Загружает данные из Google Sheets.
    Возвращает DataFrame и словарь col_map с именами колонок.
    """
    sheet    = get_worksheet()
    raw_data = sheet.get_all_values()
    if not raw_data:
        return pd.DataFrame(), {}

    # Ищем строку-заголовок
    header_idx = next(
        (
            i for i, row in enumerate(raw_data[:100])
            if "Наименование" in row
            and "Склад" in row
            and not any("ячейку" in str(c) for c in row)
        ),
        -1,
    )
    if header_idx == -1:
        return pd.DataFrame(), {}

    headers = [str(h).strip().replace("\n", " ") for h in raw_data[header_idx]]

    # Безопасный поиск индекса колонки
    def col_idx(name: str) -> int:
        if name not in headers:
            raise ValueError(f"Колонка '{name}' не найдена в таблице")
        return headers.index(name)

    try:
        col_map = {
            "ORDER":   col_idx("Наименование") - 1,   # колонка левее «Наименования»
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
    except ValueError as e:
        st.error(str(e))
        return pd.DataFrame(), {}

    df = pd.DataFrame(raw_data[START_ROW - 1:], columns=headers)
    df["_sheet_row"] = range(START_ROW, START_ROW + len(df))
    df = df[df[headers[col_map["PRODUCT"]]].str.strip() != ""].copy()

    st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
    return df, col_map


def update_google_cells(group: pd.DataFrame, col_map: dict, updates: dict) -> None:
    """
    Записывает updates = {col_key: value, …} в ячейки Google Sheets
    для всех строк переданной группы.
    """
    sheet     = get_worksheet()
    cell_list = [
        gspread.Cell(row=int(row_num), col=col_map[key] + 1, value=val)
        for key, val in updates.items()
        for row_num in group["_sheet_row"]
    ]
    sheet.update_cells(cell_list, value_input_option="USER_ENTERED")
    load_data_integrated.clear()


# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────────────────
def identify_target_store(comment: str) -> str:
    c = str(comment).lower()
    if any(x in c for x in ("пек", "пкн", "pekin")):
        return "Пекин"
    if any(x in c for x in ("горб", "грб", "gorb")):
        return "Горбушка"
    return "Общий"


def render_order_table(group: pd.DataFrame, table_cols: list, col_rename: dict) -> None:
    st.table(group[table_cols].rename(columns=col_rename))


# ── ЗАГРУЗКА ДАННЫХ ───────────────────────────────────────────────────────────
df_mem, C = load_data_integrated()

if df_mem.empty or not C:
    st.error("Не удалось загрузить данные. Проверьте подключение к таблице.")
    st.stop()

# Имена колонок по индексам из col_map
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

# Обнаружение новых заказов (только при реальном изменении набора)
current_order_ids = set(df_mem[C_ORDER].unique())
if st.session_state.prev_order_ids:
    new_ids = current_order_ids - st.session_state.prev_order_ids
    if new_ids:                                   # обновляем только при появлении новых
        st.session_state.new_orders_alert = new_ids
if current_order_ids != st.session_state.prev_order_ids:
    st.session_state.prev_order_ids = current_order_ids

# Фильтрация отменённых
is_canceled = df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)
canceled_df = df_mem[is_canceled].copy()
work_base   = df_mem[~is_canceled].copy()

# Предвычисляем целевой магазин для всего DataFrame (вместо вызовов в каждом цикле)
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
st.sidebar.caption(f"🔄 Синхронизация: {st.session_state.last_sync}")
if st.sidebar.button("🔃 Обновить вручную"):
    load_data_integrated.clear()
    st.rerun()


# ── РАЗДЕЛ: МАГАЗИН ───────────────────────────────────────────────────────────
def render_store(current_store: str) -> None:
    st.title(f"🏪 Заказы: {current_store}")

    store_new_alert = {
        oid for oid in st.session_state.new_orders_alert
        if oid in work_base[C_ORDER].values
    }
    if store_new_alert:
        st.success(f"🆕 Появились новые заказы: {', '.join(str(o) for o in sorted(store_new_alert))}")

    wh_keywords = ["Горб", "Сток"] if current_store == "Горбушка" else ["Пекин"]
    is_pz_row   = work_base[C_WH].isin(PZ_LIST)
    is_f_match  = (
        work_base[C_WH].str.contains("|".join(wh_keywords), case=False, na=False)
        & ~is_pz_row
    )
    is_pz_match = (
        (work_base[C_WH] == f"ПЗ {current_store}")
        & (work_base[C_INWORK] == TRUE_VAL)
    )

    display_df = work_base[
        (
            (is_f_match | is_pz_match)
            & (work_base[C_MOVE] != TRUE_VAL)
        )
        | (
            (work_base[C_MOVE] == TRUE_VAL)
            & (work_base["_target_store"] == current_store)
        )
    ].copy()

    display_df = display_df[
        (display_df[C_DONE] != TRUE_VAL)
        | (
            (display_df[C_DONE] == TRUE_VAL)
            & (display_df[C_EDIT] != "")
            & (~display_df[C_ORDER].isin(st.session_state.reviewed_changes))
        )
    ]

    col1, col2 = st.columns(2)

    # ── Новые / Изменения
    with col1:
        st.subheader("🆕 Новые / Изменения")
        new_items = display_df[~display_df[C_ORDER].isin(st.session_state.local_in_work)]
        if new_items.empty:
            st.info("Нет новых заказов")
        else:
            for oid, group in new_items.groupby(C_ORDER, sort=False):
                is_incoming = group[C_MOVE].iloc[0] == TRUE_VAL
                is_pz_item  = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
                has_edit    = (group[C_EDIT] != "").any() and oid not in st.session_state.reviewed_changes

                tag = (
                    (" ⚠️ ПРАВКА" if has_edit    else "")
                    + (" ⏳ ПЗ"    if is_pz_item  else "")
                    + (" 🚚 ЕДЕТ"  if is_incoming else "")
                )
                with st.expander(f"Заказ №{oid}{tag}"):
                    if has_edit:
                        st.error(f"Правка: {group[C_EDIT].iloc[0]}")
                        if st.button("Учесть правку", key=f"rev_n_{oid}"):
                            st.session_state.reviewed_changes.add(oid)
                            save_persistent_state()
                            st.rerun()
                    else:
                        render_order_table(group, TABLE_COLS, COL_RENAME)
                        if st.button("В работу", key=f"w_{oid}"):
                            st.session_state.local_in_work.add(oid)
                            save_persistent_state()
                            st.rerun()

    # ── В сборке
    with col2:
        st.subheader("🛠 В сборке")
        in_work = display_df[display_df[C_ORDER].isin(st.session_state.local_in_work)]
        if in_work.empty:
            st.info("Пока ничего не взято")
        else:
            for oid, group in in_work.groupby(C_ORDER, sort=False):
                target      = group["_target_store"].iloc[0]
                is_incoming = group[C_MOVE].iloc[0] == TRUE_VAL
                has_edit    = (group[C_EDIT] != "").any() and oid not in st.session_state.reviewed_changes

                with st.expander(f"Заказ №{oid}{' ⚠️ ПРАВКА' if has_edit else ''}"):
                    if has_edit:
                        st.error(f"Правка: {group[C_EDIT].iloc[0]}")
                        if st.button("Учесть правку", key=f"rev_w_{oid}"):
                            st.session_state.reviewed_changes.add(oid)
                            save_persistent_state()
                            st.rerun()
                    else:
                        render_order_table(group, TABLE_COLS, COL_RENAME)
                        if target != current_store and target != "Общий" and not is_incoming:
                            if st.button("🚛 Отправить перемещение", key=f"mv_{oid}"):
                                update_google_cells(group, C, {"MOVE": TRUE_VAL})
                                st.session_state.local_in_work.discard(oid)
                                save_persistent_state()
                                st.rerun()
                        else:
                            btn_label = "✅ Принято и собрано" if is_incoming else "✅ Завершить сборку"
                            if st.button(btn_label, key=f"dn_{oid}", type="primary"):
                                update_google_cells(group, C, {"DONE": TRUE_VAL, "MOVE": FALSE_VAL})
                                st.session_state.local_in_work.discard(oid)
                                st.session_state.reviewed_changes.discard(oid)
                                save_persistent_state()
                                st.rerun()


# ── МАРШРУТИЗАЦИЯ ─────────────────────────────────────────────────────────────
if "Магазин" in menu:
    current_store = "Горбушка" if "ГОРБУШКА" in menu else "Пекин"
    render_store(current_store)

elif menu == "🚚 Перемещения (Активные)":
    st.title("🚚 В пути")
    moves = work_base[work_base[C_MOVE] == TRUE_VAL]
    if moves.empty:
        st.info("Активных перемещений нет")
    else:
        for oid, group in moves.groupby(C_ORDER, sort=False):
            with st.expander(f"Перемещение №{oid}"):
                render_order_table(group, TABLE_COLS, COL_RENAME)
                if st.button("Удалить из списка", key=f"cl_mv_{oid}"):
                    update_google_cells(group, C, {"MOVE": FALSE_VAL})
                    st.rerun()

elif menu == "⏳ Товар Под заказ":
    st.title("⏳ Ожидание ПЗ")
    pz = work_base[
        work_base[C_WH].isin(PZ_LIST)
        & (work_base[C_INWORK] != TRUE_VAL)
        & (work_base[C_DONE] != TRUE_VAL)
    ]
    st.dataframe(pz[TABLE_COLS].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)

elif menu == "✅ Выполненные сборки":
    st.title("✅ Последние собранные")
    done = work_base[work_base[C_DONE] == TRUE_VAL].iloc[::-1].head(50)
    st.dataframe(done[TABLE_COLS].rename(columns=COL_RENAME), use_container_width=True, hide_index=True)

elif menu == "🚫 Отмененные заказы":
    st.title("🚫 Отмененные")
    st.dataframe(
        canceled_df[TABLE_COLS + [C_STATUS]].rename(columns=COL_RENAME),
        use_container_width=True,
        hide_index=True,
    )
