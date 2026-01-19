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


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def initialize_database() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS outflow_items (
                item TEXT PRIMARY KEY,
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
                ("Synergia",) + (150_000.0,) * 12,
                ("JP Rio",) + (80_000.0,) * 12,
            ]
            connection.executemany(
                """
                INSERT INTO outflow_items (
                    item, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                seed_items,
            )
            connection.commit()


def load_outflow_items() -> pd.DataFrame:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT item, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12
            FROM outflow_items
            ORDER BY item
            """
        ).fetchall()
    data = {"Item": [row[0] for row in rows]}
    for index, month in enumerate(MONTHS, start=1):
        data[month] = [row[index] for row in rows]
    return pd.DataFrame(data)


def persist_outflow_items(df: pd.DataFrame) -> None:
    with get_connection() as connection:
        for _, row in df.iterrows():
            values = [row[month] for month in MONTHS]
            connection.execute(
                """
                UPDATE outflow_items
                SET m1 = ?, m2 = ?, m3 = ?, m4 = ?, m5 = ?, m6 = ?, m7 = ?,
                    m8 = ?, m9 = ?, m10 = ?, m11 = ?, m12 = ?
                WHERE item = ?
                """,
                (*values, row["Item"]),
            )
        connection.commit()


def compute_summary(outflow_df: pd.DataFrame) -> pd.DataFrame:
    inflow = pd.Series([0.0] * 12, index=MONTHS)
    outflow = outflow_df[MONTHS].sum(axis=0)
    net = inflow - outflow

    saldo = []
    running = INITIAL_BALANCE
    for value in net:
        running += value
        saldo.append(running)

    summary = pd.DataFrame(
        {
            "Inflow Matchday": inflow.values,
            "Outflow Matchday": outflow.values,
            "Net": net.values,
            "Saldo Acumulado": saldo,
        },
        index=MONTHS,
    ).T
    return summary


st.set_page_config(page_title="CashFlow 2026", layout="wide")

st.title("Projeção de Caixa 2026")

initialize_database()

outflow_items = load_outflow_items()

with st.expander("Detalhar Outflow Matchday", expanded=True):
    edited_items = st.data_editor(
        outflow_items,
        hide_index=True,
        num_rows="fixed",
        use_container_width=True,
    )

persist_outflow_items(edited_items)

summary = compute_summary(edited_items)

st.subheader("Resumo Mensal")
st.dataframe(summary, use_container_width=True)
