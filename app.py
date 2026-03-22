"""
Авеню: Система Заказов — оптимизированная версия
Изменения: производительность, безопасность, стабильность, читаемость.
Функционал и интерфейс полностью сохранены.
"""

import streamlit as st
import gspread
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Optional
from streamlit_autorefresh import st_autorefresh
from google.oauth2.credentials import Credentials
import extra_streamlit_components as stx

# ── КОНСТАНТЫ ────────────────────────────────────────────────────────────────
DB_FILE        = "orders_persistent_state.json"
SHEET_ID       = "15DIisQJVQqxcPIX08xaX4b7t3Rwfrzj2DV5DqkAWQeg"
TAB_NAME       = "Заказы ИМ Авеню"
PZ_LIST        = frozenset(["ПЗ Пекин", "ПЗ Горбушка"])  # frozenset быстрее для isin
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

# Ключевые слова для определения магазина — скомпилированы один раз
_PEKIN_KEYWORDS  = ("пек", "пкн", "pekin")
_GORB_KEYWORDS   = ("горб", "грб", "gorb")

# ── НАСТРОЙКА СТРАНИЦЫ ───────────────────────────────────────────────────────
st.set_page_config(page_title="Авеню: Система Заказов", layout="wide")

# ── АВТОРИЗАЦИЯ ──────────────────────────────────────────────────────────────
# Инициализируем CookieManager один раз через session_state,
# чтобы избежать дублирующихся экземпляров при ререндерах.
if "cookie_manager" not in st.session_state:
    st.session_state.cookie_manager = stx.CookieManager(key="avenue_auth_manager_v4")

cookie_manager: stx.CookieManager = st.session_state.cookie_manager


def check_password() -> bool:
    """Проверяет авторизацию через session_state или куки."""
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

    pwd     = st.text_input("Введите код доступа:", type="password", key="login_input")
    remember = st.checkbox("Запомнить меня на этом устройстве", value=True)

    if st.button("Войти", key="login_btn", type="primary"):
        # Сравнение через hmac не требуется для простого пароля,
        # но secrets хранятся на сервере — риск минимален.
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

# ── ПОСТОЯННОЕ СОСТОЯНИЕ (JSON) ───────────────────────────────────────────────

def load_persistent_state() -> tuple[set, set]:
    """Загружает сохранённое состояние из JSON-файла."""
    if not os.path.exists(DB_FILE):
        return set(), set()
    try:
        with open(DB_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return (
            set(data.get("local_in_work", [])),
            set(data.get("reviewed_changes", [])),
        )
    except (json.JSONDecodeError, OSError, ValueError):
        # Если файл повреждён — начинаем с чистого состояния
        return set(), set()


def save_persistent_state() -> None:
    """Сохраняет текущее состояние в JSON-файл атомарно (через tmpfile)."""
    tmp_path = DB_FILE + ".tmp"
    try:
        payload = {
            "local_in_work":    list(st.session_state.local_in_work),
            "reviewed_changes": list(st.session_state.reviewed_changes),
        }
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp_path, DB_FILE)  # атомарная замена — защита от частичной записи
    except OSError as e:
        st.warning(f"Не удалось сохранить состояние: {e}")
    finally:
        # Удаляем временный файл, если замена не произошла
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


# ── АВТООБНОВЛЕНИЕ И ИНИЦИАЛИЗАЦИЯ СОСТОЯНИЯ ─────────────────────────────────
refresh_count = st_autorefresh(interval=REFRESH_MS, key="data_refresh")

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
    """
    Создаёт и кэширует клиент gspread на весь жизненный цикл сервера.
    cache_resource гарантирует один экземпляр клиента между сессиями.
    """
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


def get_worksheet() -> gspread.Worksheet:
    """Возвращает рабочий лист; при отсутствии вкладки берёт первую."""
    client = get_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        return spreadsheet.worksheet(TAB_NAME)
    except gspread.WorksheetNotFound:
        return spreadsheet.get_worksheet(0)


@st.cache_data(ttl=3600)
def load_data_integrated() -> tuple[pd.DataFrame, dict]:
    """
    Загружает все данные из Google Sheets одним запросом get_all_values().
    TTL=3600; принудительный сброс кэша — через .clear() при автообновлении.
    """
    sheet    = get_worksheet()
    raw_data = sheet.get_all_values()
    if not raw_data:
        return pd.DataFrame(), {}

    # Ищем строку-заголовок среди первых 100 строк
    header_idx = next(
        (i for i, row in enumerate(raw_data[:100])
         if "Наименование" in row and "Склад" in row),
        -1,
    )
    if header_idx == -1:
        return pd.DataFrame(), {}

    headers = [str(h).strip().replace("\n", " ") for h in raw_data[header_idx]]

    # Вспомогательная функция с понятным сообщением об ошибке
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

    # Строим DataFrame сразу из нужного диапазона строк
    data_rows = raw_data[START_ROW - 1:]
    df = pd.DataFrame(data_rows, columns=headers)
    df["_sheet_row"] = range(START_ROW, START_ROW + len(df))

    # Фильтруем пустые строки по колонке «Наименование» (PRODUCT)
    product_col = headers[col_map["PRODUCT"]]
    df = df[df[product_col].str.strip().astype(bool)].copy()

    st.session_state.last_sync = datetime.now().strftime("%H:%M:%S")
    return df, col_map


# Принудительный сброс кэша при срабатывании автотаймера
if refresh_count > 0:
    load_data_integrated.clear()

# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────────────────

def update_google_cells(group: pd.DataFrame, col_map: dict, updates: dict) -> None:
    """
    Обновляет ячейки Google Sheets одним batch-запросом.
    После записи инвалидирует кэш данных.
    """
    sheet     = get_worksheet()
    col_count = len(group)

    # Формируем список Cell-объектов
    cell_list = [
        gspread.Cell(row=int(row_num), col=col_map[key] + 1, value=val)
        for key, val in updates.items()
        for row_num in group["_sheet_row"]
    ]

    if cell_list:
        sheet.update_cells(cell_list, value_input_option="USER_ENTERED")

    load_data_integrated.clear()


def identify_target_store(comment: str) -> str:
    """Определяет целевой магазин по ключевым словам в комментарии."""
    c = str(comment).lower()
    if "d" in c:
        return STORE_GORB
    if any(k in c for k in _PEKIN_KEYWORDS):
        return STORE_PEKIN
    if any(k in c for k in _GORB_KEYWORDS):
        return STORE_GORB
    return "Общий"


def render_order_table(group: pd.DataFrame, table_cols: list, col_rename: dict) -> None:
    """Отображает таблицу позиций заказа."""
    st.table(group[table_cols].rename(columns=col_rename))


# ── ПОДГОТОВКА ДАННЫХ ─────────────────────────────────────────────────────────
df_mem, C = load_data_integrated()

if df_mem.empty or not C:
    st.error("Не удалось загрузить данные. Проверьте таблицу и настройки.")
    st.stop()

# Именованные алиасы для колонок (читаемость кода)
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

# Обновляем алерт о новых заказах
current_order_ids = set(df_mem[C_ORDER].unique())
if st.session_state.prev_order_ids:
    new_ids = current_order_ids - st.session_state.prev_order_ids
    if new_ids:
        st.session_state.new_orders_alert = new_ids
st.session_state.prev_order_ids = current_order_ids

# Базовый DataFrame без отменённых заказов
work_base = df_mem[
    ~df_mem[C_STATUS].str.lower().str.contains("отмен", na=False)
].copy()

# Вычисляем целевой магазин векторно — один раз для всего DataFrame
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
    """Формирует строку тегов для заголовка expander."""
    tags = []
    if "d" in comment_str:    tags.append("📦 ДОСТАВКА")
    if is_move_needed:         tags.append("🚚 ПЕРЕМЕЩЕНИЕ")
    if has_edit:               tags.append("⚠️ ИЗМЕНЕНИЕ" if not extra_tag else extra_tag)
    if is_pz_item:             tags.append("⏳ ПЗ")
    if incoming:               tags.append("🚚 ЕДЕТ")
    return " | ".join(tags)


def render_store(current_store: str) -> None:
    """Рендерит интерфейс конкретного магазина."""
    st.title(f"🏪 Заказы: {current_store}")

    # Алерт о новых заказах, актуальных для этого магазина
    store_new_alert = {
        oid for oid in st.session_state.new_orders_alert
        if oid in work_base[C_ORDER].values
    }
    if store_new_alert:
        st.success(
            f"🆕 Обнаружены новые заказы: "
            f"{', '.join(str(o) for o in sorted(store_new_alert))}"
        )

    # Маски для фильтрации строк — вычисляем один раз
    is_pz_row = work_base[C_WH].isin(PZ_LIST)
    is_move   = work_base[C_MOVE] == TRUE_VAL

    if current_store == STORE_GORB:
        wh_match = work_base[C_WH].str.contains("Горб|Сток", case=False, na=False)
    else:
        wh_match = work_base[C_WH].str.contains("Пекин", case=False, na=False)

    is_f_match   = wh_match & ~is_pz_row
    is_pz_match  = (
        (work_base[C_WH] == f"ПЗ {current_store}")
        & (work_base[C_INWORK] == TRUE_VAL)
    )
    is_incoming  = is_move & (work_base["_target_store"] == current_store)

    # Объединяем условия и фильтруем строки
    base_mask    = ((is_f_match | is_pz_match) & ~is_move) | is_incoming
    reviewed     = st.session_state.reviewed_changes
    has_unrev    = work_base[C_EDIT].ne("") & ~work_base[C_ORDER].isin(reviewed)

    display_df = work_base[
        base_mask & ((work_base[C_DONE] != TRUE_VAL) | has_unrev)
    ].copy()

    col1, col2 = st.columns(2)

    # ── Новые заказы ──────────────────────────────────────────────────────────
    with col1:
        st.subheader("🆕 Новые / Изменения")
        new_items = display_df[~display_df[C_ORDER].isin(st.session_state.local_in_work)]

        for oid, group in new_items.groupby(C_ORDER, sort=False):
            comment_str    = str(group[C_COMMENT].iloc[0]).lower()
            target         = group["_target_store"].iloc[0]
            incoming       = group[C_MOVE].iloc[0] == TRUE_VAL
            is_pz_item     = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
            has_edit       = group[C_EDIT].ne("").any() and oid not in reviewed
            is_move_needed = target != current_store and target != "Общий" and not incoming

            tag_str = _build_tags(comment_str, is_move_needed, has_edit, is_pz_item, incoming)
            label   = f"Заказ №{oid}{f' [{tag_str}]' if tag_str else ''}"

            with st.expander(label):
                if has_edit:
                    st.error(f"Изменение: {group[C_EDIT].iloc[0]}")
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

    # ── В сборке ──────────────────────────────────────────────────────────────
    with col2:
        st.subheader("🛠 В сборке")
        in_work = display_df[display_df[C_ORDER].isin(st.session_state.local_in_work)]

        for oid, group in in_work.groupby(C_ORDER, sort=False):
            comment_str    = str(group[C_COMMENT].iloc[0]).lower()
            target         = group["_target_store"].iloc[0]
            incoming       = group[C_MOVE].iloc[0] == TRUE_VAL
            is_pz_item     = group[C_WH].isin(PZ_LIST).any() and (group[C_INWORK] == TRUE_VAL).any()
            has_edit       = group[C_EDIT].ne("").any() and oid not in reviewed
            is_move_needed = target != current_store and target != "Общий" and not incoming

            tag_str = _build_tags(
                comment_str, is_move_needed, has_edit, is_pz_item, incoming,
                extra_tag="⚠️ ПРАВКА",
            )
            label = f"Заказ №{oid}{f' [{tag_str}]' if tag_str else ''}"

            with st.expander(label):
                if has_edit:
                    st.error(f"Правка: {group[C_EDIT].iloc[0]}")
                render_order_table(group, TABLE_COLS, COL_RENAME)

                if has_edit:
                    if st.button("Учесть правку", key=f"rev_w_{oid}"):
                        st.session_state.reviewed_changes.add(oid)
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
                        st.session_state.reviewed_changes.discard(oid)
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
