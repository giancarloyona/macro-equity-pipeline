import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px
from macro_pipeline.main import run_pipeline

st.set_page_config(page_title="Macro Equity Insights", layout="wide")

st.title("Macro-Equity Real Return Analyzer")
st.markdown("""
This tool calculates the **Real Return** of assets (adjusted for inflation) 
using data from **FRED** and **Yahoo Finance**.
""")

st.sidebar.header("Pipeline Configuration")

equity_ticker = st.sidebar.text_input("Equity Ticker (Yahoo)", value="SPY")
macro_series = st.sidebar.text_input("Macro Series (FRED)", value="CPIAUCSL")

default_start = datetime.now() - timedelta(days=365 * 10)
start_date = st.sidebar.date_input("Start Date", value=default_start)

if st.sidebar.button("Run Analysis"):
    try:
        with st.spinner(f"Fetching and processing data for {equity_ticker}..."):

            data = run_pipeline(
                equity_ticker=equity_ticker,
                macro_series=macro_series,
                start_date=datetime.combine(start_date, datetime.min.time()),
            )

        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "Final Cumulative Real Return",
                f"{data['cumulative_real_return'].iloc[-1]:.2%}",
            )

        with col2:
            st.metric("Total Data Points", len(data))

        st.subheader(f"Cumulative Real Return: {equity_ticker} adj. by {macro_series}")
        fig = px.line(
            data, y="cumulative_real_return", title="Wealth Index (Real Terms)"
        )
        fig.update_layout(yaxis_tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error executing pipeline: {e}")
        st.info("Check if your FRED_API_KEY is correctly set in the .env file.")

else:
    st.info("Configure the parameters and click 'Run Analysis' to start.")
