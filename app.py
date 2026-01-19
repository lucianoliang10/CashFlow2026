import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).with_name("data.db")
MONTHS = [
    "Jan/26",
    "Feb/26",
    "Mar/26",
    "Apr/26",
    "May/26",
    "Jun/26",
    "Jul/26",
    "Aug/26",
    "Sep/26",
    "Oct/26",
    "Nov/26",
    "Dec/26",
]
MONTH_COLUMNS = [f"m{index + 1}" for index in range(12)]
INITIAL_BALANCE = 25_542_000.00
INFLOW_CATEGORIES = {
    "Football Revenues": [
        "Awards",
        "Broadcast",
        "Matchday",
        "Marketing & Commercial",
        "Sponsor",
        "Space Lease",
        "Fan Program",
        "Licensing",
        "Merchandising",
        "Social Medias",
    ],
}
OUTFLOW_CATEGORIES = {
    "Payroll Men’s Football": [
        "Salary (M)",
        "Image Right",
        "Signing Fee (Image)",
        "Payroll Taxes (M)",
        "Professional Services (M)",
        "Merit Payments",
    ],
    "Payroll Youth & Women’s Football": [
        "Salary (YW)",
        "Payroll Taxes (YW)",
        "Professional Services (YW)",
    ],
    "Payroll Corporate": [
        "Salary (Corporate)",
        "Payroll Taxes (Corporate)",
        "Professional Services (Corporate)",
    ],
    "Other Payroll Expenses": ["Benefits"],
    "Suppliers": [
        "General Suppliers",
        "Matchday",
        "Matchday Suppliers",
        "Logistics Expenses",
        "Utility Bills",
        "Merchandising",
    ],
    "Taxes": [
        "Football Specific Tribute (TEF)",
        "Other Taxes",
    ],
}
CATEGORY_OPTIONS = list(INFLOW_CATEGORIES.keys()) + list(OUTFLOW_CATEGORIES.keys())
SUBCATEGORY_OPTIONS = [
    subcategory
    for subcategories in list(INFLOW_CATEGORIES.values()) + list(OUTFLOW_CATEGORIES.values())
    for subcategory in subcategories
]


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def initialize_database() -> None:
    with get_connection() as connection:
        existing_tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='outflow_items'"
        ).fetchone()
        if existing_tables:
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(outflow_items)")
            }
            required = {
                "id",
                "entry_type",
                "category",
                "subcategory",
                "item",
                *MONTH_COLUMNS,
            }
            if not required.issubset(columns):
                connection.execute("DROP TABLE outflow_items")
                existing_tables = None

        if not existing_tables:
            connection.execute(
                """
                CREATE TABLE outflow_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_type TEXT NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT NOT NULL,
                    item TEXT NOT NULL,
                    m1 REAL NOT NULL,
                    m2 REAL NOT NULL,
                    m3 REAL NOT NULL,
                    m4 REAL NOT NULL,
                    m5 REAL NOT NULL,
                    m6 REAL NOT NULL,
                    m7 REAL NOT NULL,
                    m8 REAL NOT NULL,
                    m9 REAL NOT NULL,
                    m10 REAL NOT NULL,
                    m11 REAL NOT NULL,
                    m12 REAL NOT NULL
                )
                """
            )

        existing = connection.execute("SELECT COUNT(*) FROM outflow_items").fetchone()[0]
        if existing == 0:
            seed_items = [
                ("Outflow", "Suppliers", "Matchday", "Synergia")
                + (150_000.0,) * 12,
                ("Outflow", "Suppliers", "Matchday", "JP Rio") + (80_000.0,) * 12,
            ]
            connection.executemany(
                """
                INSERT INTO outflow_items (
                    entry_type, category, subcategory, item, m1, m2, m3, m4, m5, m6,
                    m7, m8, m9, m10, m11, m12
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                seed_items,
            )
            connection.commit()


def load_outflow_items() -> pd.DataFrame:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT entry_type, category, subcategory, item,
                   m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12
            FROM outflow_items
            ORDER BY entry_type, category, subcategory, item, id
            """
        ).fetchall()
    data = {
        "Tipo": [row[0] for row in rows],
        "Categoria": [row[1] for row in rows],
        "Subcategoria": [row[2] for row in rows],
        "Item": [row[3] for row in rows],
    }
    for index, month in enumerate(MONTHS, start=4):
        data[month] = [row[index] for row in rows]
    df = pd.DataFrame(data)
    if df.empty:
        df = pd.DataFrame(columns=["Tipo", "Categoria", "Subcategoria", "Item", *MONTHS])
    return df


def persist_outflow_items(df: pd.DataFrame) -> None:
    cleaned = df.dropna(subset=["Item"]).copy()
    if cleaned.empty:
        items = []
    else:
        cleaned["Item"] = cleaned["Item"].astype(str).str.strip()
        cleaned["Tipo"] = cleaned["Tipo"].astype(str).str.strip()
        cleaned["Categoria"] = cleaned["Categoria"].astype(str).str.strip()
        cleaned["Subcategoria"] = cleaned["Subcategoria"].astype(str).str.strip()
        for month in MONTHS:
            cleaned[month] = pd.to_numeric(cleaned[month], errors="coerce").fillna(0.0)
        items = [
            (
                row["Tipo"],
                row["Categoria"],
                row["Subcategoria"],
                row["Item"],
            )
            + tuple(row[month] for month in MONTHS)
            for _, row in cleaned.iterrows()
        ]
    with get_connection() as connection:
        connection.execute("DELETE FROM outflow_items")
        if items:
            connection.executemany(
                """
                INSERT INTO outflow_items (
                    entry_type, category, subcategory, item, m1, m2, m3, m4, m5, m6,
                    m7, m8, m9, m10, m11, m12
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                items,
            )
        connection.commit()


def format_currency(value: float) -> str:
    if pd.isna(value):
        return ""
    scaled = value / 1000
    if scaled == 0:
        return "-"
    formatted = f"{abs(scaled):,.0f}".replace(",", ".")
    if scaled < 0:
        return f"({formatted})"
    return formatted


def compute_summary(outflow_df: pd.DataFrame) -> pd.DataFrame:
    base_series = pd.Series([0.0] * 12, index=MONTHS)
    required_columns = {"Tipo", "Categoria", "Subcategoria", *MONTHS}
    if not required_columns.issubset(outflow_df.columns):
        for column in required_columns:
            if column not in outflow_df.columns:
                outflow_df[column] = ""
    inflow_subcategories = {
        name: base_series.copy()
        for subcategories in INFLOW_CATEGORIES.values()
        for name in subcategories
    }
    outflow_subcategories = {
        name: base_series.copy()
        for subcategories in OUTFLOW_CATEGORIES.values()
        for name in subcategories
    }

    values = outflow_df[MONTHS].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    inflow_mask = outflow_df["Tipo"].str.strip().str.casefold() == "inflow"
    outflow_mask = outflow_df["Tipo"].str.strip().str.casefold() == "outflow"

    for subcategory in inflow_subcategories:
        mask = inflow_mask & (outflow_df["Subcategoria"] == subcategory)
        inflow_subcategories[subcategory] = values[mask].sum(axis=0)

    for subcategory in outflow_subcategories:
        mask = outflow_mask & (outflow_df["Subcategoria"] == subcategory)
        outflow_subcategories[subcategory] = values[mask].sum(axis=0)

    inflow_category_totals = {
        category: sum(
            (inflow_subcategories[name] for name in subcategories),
            base_series.copy(),
        )
        for category, subcategories in INFLOW_CATEGORIES.items()
    }
    outflow_category_totals = {
        category: sum(
            (outflow_subcategories[name] for name in subcategories),
            base_series.copy(),
        )
        for category, subcategories in OUTFLOW_CATEGORIES.items()
    }
    inflow_total = sum(inflow_category_totals.values(), base_series.copy())
    outflow_total = sum(outflow_category_totals.values(), base_series.copy())
    net = inflow_total - outflow_total

    saldo = []
    running = INITIAL_BALANCE
    for value in net:
        running += value
        saldo.append(running)

    rows: list[tuple[str, pd.Series | pd.Index]] = []
    rows.append(("SALDO ACUMULADO", pd.Series(saldo, index=MONTHS)))
    rows.append(("INFLOWS", inflow_total))
    for category, subcategories in INFLOW_CATEGORIES.items():
        rows.append((category, inflow_category_totals[category]))
        for name in subcategories:
            rows.append((name, inflow_subcategories[name]))
    rows.append(("OUTFLOWS", outflow_total))
    for category, subcategories in OUTFLOW_CATEGORIES.items():
        rows.append((category, outflow_category_totals[category]))
        for name in subcategories:
            rows.append((name, outflow_subcategories[name]))

    data = {name: values.values for name, values in rows}
    summary = pd.DataFrame(data, index=MONTHS).T
    return summary


st.set_page_config(page_title="CashFlow 2026", layout="wide")

st.title("Projeção de Caixa 2026")

initialize_database()

if "outflow_items" not in st.session_state:
    st.session_state["outflow_items"] = load_outflow_items()

outflow_items = st.session_state["outflow_items"]

with st.expander("Detalhar Itens", expanded=True):
    st.caption(
        "Edite valores diretamente, adicione ou remova linhas, aplique valores "
        "mensais em lote ou importe uma planilha. Classifique cada item por "
        "categoria e subcategoria."
    )
    upload = st.file_uploader(
        "Importar planilha (Excel)",
        type=["xlsx", "xls"],
    )
    if upload is not None and st.button("Aplicar planilha"):
        try:
            imported = pd.read_excel(upload)
        except ImportError:
            st.error(
                "Biblioteca necessária para ler Excel ausente. "
                "Instale o suporte no ambiente para importar."
            )
        else:
            expected_columns = {"Tipo", "Categoria", "Subcategoria", "Item", *MONTHS}
            if not expected_columns.issubset(set(imported.columns)):
                st.error(
                    "A planilha precisa ter colunas: Tipo, Categoria, Subcategoria, "
                    "Item e Jan/26 a Dec/26."
                )
            else:
                imported = imported[["Tipo", "Categoria", "Subcategoria", "Item", *MONTHS]]
                st.session_state["outflow_items"] = imported
                outflow_items = imported
                persist_outflow_items(imported)
                st.success("Planilha importada com sucesso.")

    st.markdown("**Aplicar valor mensal em lote**")
    selected_items = st.multiselect(
        "Itens para aplicar",
        options=outflow_items["Item"].dropna().tolist(),
    )
    monthly_value = st.number_input(
        "Valor mensal",
        min_value=0.0,
        step=1000.0,
        value=0.0,
        format="%.2f",
    )
    if st.button("Aplicar valor mensal"):
        if selected_items:
            updated = outflow_items.copy()
            updated.loc[updated["Item"].isin(selected_items), MONTHS] = monthly_value
            st.session_state["outflow_items"] = updated
            outflow_items = updated
            persist_outflow_items(updated)
            st.success("Valores mensais aplicados.")
        else:
            st.warning("Selecione ao menos um item para aplicar o valor mensal.")

    if st.button("Adicionar item"):
        default_row = {
            "Tipo": "Outflow",
            "Categoria": CATEGORY_OPTIONS[0],
            "Subcategoria": SUBCATEGORY_OPTIONS[0],
            "Item": "",
        }
        for month in MONTHS:
            default_row[month] = 0.0
        outflow_items = pd.concat(
            [outflow_items, pd.DataFrame([default_row])], ignore_index=True
        )
        st.session_state["outflow_items"] = outflow_items

    edited_items = st.data_editor(
        outflow_items,
        hide_index=True,
        num_rows="dynamic",
        use_container_width=True,
        key="outflow_editor",
        column_config={
            "Tipo": st.column_config.SelectboxColumn(
                "Tipo",
                options=["Inflow", "Outflow"],
                required=True,
            ),
            "Categoria": st.column_config.SelectboxColumn(
                "Categoria",
                options=CATEGORY_OPTIONS,
                required=True,
            ),
            "Subcategoria": st.column_config.SelectboxColumn(
                "Subcategoria",
                options=SUBCATEGORY_OPTIONS,
                required=True,
            ),
            "Item": st.column_config.TextColumn("Item", required=True),
        },
    )

if st.button("Salvar alterações"):
    persist_outflow_items(edited_items)
    st.session_state["outflow_items"] = edited_items
    st.success("Alterações salvas.")

summary = compute_summary(edited_items)
summary_display = summary.copy()
summary_display.insert(0, "Resumo", summary_display.index)

st.subheader("Resumo Mensal")
highlight_rows = ["SALDO ACUMULADO", "INFLOWS", "OUTFLOWS"]
bold_rows = set(highlight_rows)
bold_rows.update(INFLOW_CATEGORIES.keys())
bold_rows.update(OUTFLOW_CATEGORIES.keys())


def highlight_categories(row: pd.Series) -> list[str]:
    styles = []
    label = row["Resumo"]
    for _ in row:
        cell_style = ""
        if label in highlight_rows:
            cell_style += "background-color: #f0f2f6;"
        if label in bold_rows:
            cell_style += " font-weight: 700;"
        styles.append(cell_style.strip())
    return styles


styled_summary = summary_display.style.format(
    format_currency, subset=MONTHS
).apply(highlight_categories, axis=1)
summary_table = st.dataframe(
    styled_summary,
    use_container_width=True,
    height=600,
    on_select="rerun",
    selection_mode="single-row",
    key="summary_table",
)

selected_rows = []
if summary_table is not None:
    selected_rows = list(getattr(summary_table.selection, "rows", []))
if not selected_rows:
    selected_rows = (
        st.session_state.get("summary_table", {}).get("selected_rows", [])
    )
selected_label = (
    summary_display["Resumo"].iloc[selected_rows[0]] if selected_rows else None
)

filtered_items = edited_items
if selected_label in INFLOW_CATEGORIES:
    filtered_items = edited_items[
        edited_items["Categoria"] == selected_label
    ]
elif selected_label in OUTFLOW_CATEGORIES:
    filtered_items = edited_items[
        edited_items["Categoria"] == selected_label
    ]
elif selected_label in SUBCATEGORY_OPTIONS:
    filtered_items = edited_items[
        edited_items["Subcategoria"] == selected_label
    ]
elif selected_label == "INFLOWS":
    filtered_items = edited_items[
        edited_items["Tipo"].str.strip().str.casefold() == "inflow"
    ]
elif selected_label == "OUTFLOWS":
    filtered_items = edited_items[
        edited_items["Tipo"].str.strip().str.casefold() == "outflow"
    ]

if selected_label:
    st.markdown(f"**Itens filtrados: {selected_label}**")
    st.dataframe(filtered_items, use_container_width=True)
