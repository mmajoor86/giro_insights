import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.data_load.degiro import load_degiro_data
from src.data_transform.portfolio import compute_portfolio

st.set_page_config(page_title="DEGIRO Portfolio Insights", layout="wide")
st.title("DEGIRO Portfolio Insights")

# --- Sidebar: File Upload ---
st.sidebar.header("Upload DEGIRO Exports")
account_file = st.sidebar.file_uploader("Account.xlsx", type=["xlsx"])
transactions_file = st.sidebar.file_uploader("Transactions.xlsx", type=["xlsx"])
run_button = st.sidebar.button(
    "Run Analysis", disabled=not (account_file and transactions_file)
)

if run_button:
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    with open("data/raw/Account.xlsx", "wb") as f:
        f.write(account_file.getbuffer())
    with open("data/raw/Transactions.xlsx", "wb") as f:
        f.write(transactions_file.getbuffer())

    with st.status("Running portfolio analysis...", expanded=True) as status:
        st.write("Loading DEGIRO exports...")
        load_degiro_data()

        st.write("Fetching stock rates and FX data from yfinance...")
        df = compute_portfolio()

        status.update(label="Analysis complete!", state="complete", expanded=False)

    st.session_state["portfolio_df"] = df

# --- Visualization ---
if "portfolio_df" in st.session_state:
    df = st.session_state["portfolio_df"].copy()
    df["Date"] = pd.to_datetime(df["Date"])

    # --- Summary Metrics ---
    latest_date = df["Date"].max()
    latest = df[df["Date"] == latest_date]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Portfolio Value", f"\u20ac {latest['eur'].sum():,.0f}")
    col2.metric("Positions", latest["Product"].nunique())
    col3.metric("Earliest Date", df["Date"].min().strftime("%b %Y"))
    col4.metric("Latest Date", latest_date.strftime("%d %b %Y"))

    # --- Monthly Stacked Bar + Total Line ---
    st.subheader("Portfolio Over Time")

    df["MonthYear"] = df["Date"].dt.to_period("M")
    last_days = df.groupby("MonthYear")["Date"].max()
    df_filtered = df[df["Date"].isin(last_days.values)]

    pivot_df = df_filtered.pivot_table(
        index="Date", columns="Product", values="eur", aggfunc="sum", fill_value=0
    )

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.15,
        subplot_titles=[
            "EUR per Product (End of Month)",
            "Total Portfolio Value (End of Month)",
        ],
    )

    for product in pivot_df.columns:
        fig.add_trace(
            go.Bar(x=pivot_df.index, y=pivot_df[product], name=product), row=1, col=1
        )

    monthly_total = pivot_df.sum(axis=1)
    fig.add_trace(
        go.Scatter(
            x=monthly_total.index,
            y=monthly_total.values,
            name="Total EUR",
            mode="lines",
            line=dict(color="gray", width=2),
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        barmode="stack",
        height=700,
        legend_title="Product",
        template="plotly_white",
    )
    fig.update_yaxes(title_text="EUR", row=1, col=1)
    fig.update_yaxes(title_text="EUR", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)

    # --- Current Holdings Table ---
    st.subheader("Current Holdings")

    holdings = (
        latest.groupby(["Product", "Ticker", "currency"])
        .agg(Shares=("Aantal", "sum"), Value_EUR=("eur", "sum"))
        .reset_index()
        .sort_values("Value_EUR", ascending=False)
    )
    holdings["Value_EUR"] = holdings["Value_EUR"].round(2)

    st.dataframe(
        holdings,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Value_EUR": st.column_config.NumberColumn("Value (EUR)", format="%.2f"),
            "Shares": st.column_config.NumberColumn("Shares", format="%.2f"),
        },
    )

    # --- Per-Stock Drilldown ---
    st.subheader("Product Drilldown")

    products = sorted(df["Product"].unique())
    selected = st.selectbox("Select a product", products)

    df_product = df_filtered[df_filtered["Product"] == selected]
    product_monthly = df_product.groupby("Date")["eur"].sum().reset_index()

    fig_product = go.Figure()
    fig_product.add_trace(
        go.Scatter(
            x=product_monthly["Date"],
            y=product_monthly["eur"],
            mode="lines+markers",
            name=selected,
        )
    )
    fig_product.update_layout(
        yaxis_title="EUR",
        template="plotly_white",
        height=400,
    )
    st.plotly_chart(fig_product, use_container_width=True)

else:
    st.info(
        "Upload your DEGIRO **Account.xlsx** and **Transactions.xlsx** files "
        "in the sidebar to get started."
    )
