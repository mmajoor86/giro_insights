import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from src.data_load.degiro_account import load_account_data
from src.data_load.degiro_transactions import load_transactions
from src.data_load.fx_rates import load_fx
from src.data_load.stock_rates import load_stockrates
from src.data_transform.build_portfolio import build_daily_portfolio

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
    os.makedirs("data/transformed", exist_ok=True)

    with open("data/raw/Account.xlsx", "wb") as f:
        f.write(account_file.getbuffer())
    with open("data/raw/Transactions.xlsx", "wb") as f:
        f.write(transactions_file.getbuffer())

    with st.status("Running portfolio analysis...", expanded=True) as status:
        st.write("Loading DEGIRO exports...")
        load_transactions()
        load_account_data()

        st.write("Fetching stock rates and FX data from yfinance...")
        load_stockrates()
        load_fx()

        st.write("Building portfolio...")
        df = build_daily_portfolio()

        status.update(label="Analysis complete!", state="complete", expanded=False)

    st.session_state["portfolio_df"] = df

# --- Visualization ---
if "portfolio_df" in st.session_state:
    df = st.session_state["portfolio_df"].copy()
    df["Datum"] = pd.to_datetime(df["Datum"])

    # --- Summary Metrics ---
    latest_date = df.loc[df['Product']!='Cash Balance']["Datum"].max()
    latest = df[df["Datum"] == latest_date]

    total_value = latest["Total_Value"].sum()
    invested = latest["Cumulatieve Inleg"].max()
    profit_eur = total_value - invested
    profit_pct = (profit_eur / invested * 100) if invested else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Portfolio Value", f"\u20ac {total_value:,.0f}")
    col2.metric("Profit", f"\u20ac {profit_eur:,.0f}", delta=f"{profit_pct:+.1f}%")
    col3.metric("Positions", latest["Product"].nunique())
    col4.metric("Latest Date", latest_date.strftime("%d %b %Y"))

    # --- Portfolio Over Time ---
    st.subheader("Portfolio Over Time")

    df["MonthYear"] = df["Datum"].dt.to_period("M")
    last_days = df.loc[df['Product']!='Cash Balance'].groupby("MonthYear")["Datum"].max()
    df_filtered = df[df["Datum"].isin(last_days.values)]

    # Aggregate: total shares vs cash balance per month
    is_cash = df_filtered["Product"] == "Cash Balance"
    monthly = df_filtered.groupby("Datum").agg(
        Shares=("Total_Value", lambda x: x[~is_cash.loc[x.index]].sum()),
        Cash=("Total_Value", lambda x: x[is_cash.loc[x.index]].sum()),
        Invested=("Cumulatieve Inleg", "max"),
    ).reset_index()

    monthly["Total"] = monthly["Shares"] + monthly["Cash"]
    monthly["Profit_EUR"] = monthly["Total"] - monthly["Invested"]
    monthly["Profit_pct"] = (monthly["Profit_EUR"] / monthly["Invested"] * 100).round(1)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=monthly["Datum"], y=monthly["Shares"], name="Shares"))
    fig.add_trace(go.Bar(x=monthly["Datum"], y=monthly["Cash"], name="Cash Balance"))
    fig.add_trace(go.Scatter(
        x=monthly["Datum"],
        y=monthly["Invested"],
        name="Amount Invested",
        mode="lines+markers",
        line=dict(color="black", width=2, dash="dash"),
        marker=dict(size=4),
    ))
    fig.add_trace(go.Scatter(
        x=monthly["Datum"],
        y=monthly["Profit_EUR"],
        name="Profit",
        mode="lines+markers",
        text=monthly["Profit_pct"].apply(lambda x: f"{x:+.1f}%"),
        textposition="top center",
        line=dict(color="green", width=2),
        marker=dict(size=4),
    ))

    fig.update_layout(
        barmode="stack",
        height=500,
        yaxis_title="EUR",
        template="plotly_white",
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- Current Holdings Table ---
    st.subheader("Current Holdings")

    holdings = (
        latest.groupby(["Product", "Ticker"])
        .agg(Shares=("Aantal", "sum"), Value_EUR=("Total_Value", "sum"))
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
    product_monthly = df_product.groupby("Datum")["Total_Value"].sum().reset_index()

    fig_product = go.Figure()
    fig_product.add_trace(
        go.Scatter(
            x=product_monthly["Datum"],
            y=product_monthly["Total_Value"],
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
