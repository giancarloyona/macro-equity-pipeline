import streamlit as st
from datetime import datetime, timedelta
from macro_pipeline.main import run_pipeline
from macro_pipeline.processors.analytics import AnalyticsProcessor
import pandas as pd
import numpy as np
import plotly.graph_objects as go

st.set_page_config(page_title="Macro Equity Insights", layout="wide")

st.title("Macro-Equity Real Return Analyzer")
st.markdown("""
This tool calculates the **Real Return** of assets (adjusted for inflation) 
using data from **FRED** and **Yahoo Finance**.
""")

st.sidebar.header("Pipeline Configuration")

equity_tickers = st.sidebar.text_input(
    "Tickers (comma-separated)",
    value="SPY,QQQ",
    help="Example: SPY,QQQ",
)

macro_series = st.sidebar.text_input("Macro Series (FRED)", value="CPIAUCSL")

default_start = datetime.now() - timedelta(days=365 * 10)
start_date = st.sidebar.date_input("Start Date", value=default_start)

if st.sidebar.button("Run Analysis"):
    try:
        equity_tickers = [ticker.strip() for ticker in equity_tickers.split(",")]
        processor = AnalyticsProcessor()

        with st.spinner(f"Fetching and processing data for {equity_tickers}..."):
            data = run_pipeline(
                equity_tickers=equity_tickers,
                macro_series=macro_series,
                start_date=datetime.combine(start_date, datetime.min.time()),
            )

        metrics = processor.get_performance_metrics(data, equity_tickers)

        st.subheader("Cumulative Real Return")

        cumulative_cols = [
            col for col in data.columns if "cumulative_real_return" in col
        ]
        st.markdown(
            """
            <style>
            .stPlotlyChart {overflow:visible !important;}
            </style>
            """,
            unsafe_allow_html=True,
        )

        fig = go.Figure()
        color_palette = [
            "#1f77b4",
            "#ff7f0e",
            "#2ca02c",
            "#d62728",
            "#9467bd",
            "#8c564b",
            "#e377c2",
            "#7f7f7f",
            "#bcbd22",
            "#17becf",
        ]

        for i, col in enumerate(cumulative_cols):
            fig.add_trace(
                go.Scatter(
                    x=data.index,
                    y=data[col],
                    mode="lines",
                    name=col.replace("cumulative_real_return_", "").upper(),
                    line=dict(color=color_palette[i % len(color_palette)], width=2),
                    hovertemplate="<b>Date</b>: %{x|%Y-%m-%d}<br><b>Cumulative Return</b>: %{y:.2%}<extra></extra>",
                )
            )

        fig.update_layout(
            title="Cumulative Real Returns Over Time",
            xaxis_title="Date",
            yaxis_title="Cumulative Real Return",
            yaxis_tickformat=".1%",
            legend_title="Asset",
            template="plotly_white",
            hovermode="x unified",
            margin=dict(l=40, r=40, t=60, b=40),
            height=460,
        )

        st.plotly_chart(fig, width="stretch")

        if len(equity_tickers) >= 2:
            st.divider()
            st.subheader("Asset Correlation Matrix (Real Returns)")

            with st.expander("Why correlation matters?", expanded=False):
                st.write("""
                    Correlation ranges from **-1 to 1**:
                    * **1.0**: Assets move in perfect sync (no diversification).
                    * **0.0**: Assets move independently (good for risk reduction).
                    * **-1.0**: Assets move in opposite directions (perfect hedge).
                """)

            corr = processor.get_correlation_matrix(data, equity_tickers)

            fig_corr = go.Figure(
                data=go.Heatmap(
                    z=corr.values,
                    x=corr.columns,
                    y=corr.index,
                    colorscale="RdBu_r",
                    zmin=-1,
                    zmax=1,
                    colorbar=dict(title="Correlation"),
                    text=np.round(corr.values, 2),
                    hovertemplate="<b>%{y}</b> vs <b>%{x}</b>: %{z:.2f}<extra></extra>",
                )
            )
            # Add correlation values as annotations on heatmap
            for i in range(len(corr.index)):
                for j in range(len(corr.columns)):
                    fig_corr.add_annotation(
                        x=corr.columns[j],
                        y=corr.index[i],
                        text=f"{corr.values[i, j]:.2f}",
                        showarrow=False,
                        font=dict(color="black", size=12),
                    )
            fig_corr.update_layout(
                xaxis_title="Asset",
                yaxis_title="Asset",
                title="Correlation Matrix (Real Returns)",
                yaxis_autorange="reversed",
            )

            st.plotly_chart(fig_corr, width="content")
        else:
            st.info("Add two or more tickers to see the correlation matrix.")

        st.subheader("Performance metrics")
        for col in cumulative_cols:
            latest_return = data[col].iloc[-1]
            st.metric(label=col, value=f"{latest_return:.2%}")

        st.subheader("Key Performance Indicators (Real Terms)")

        if isinstance(metrics, dict):
            df_metrics = pd.DataFrame([metrics], index=[0])
        else:
            df_metrics = pd.DataFrame(metrics, index=range(len(metrics)))

        st.dataframe(
            df_metrics.style.highlight_max(
                axis=0, subset=["sharpe_ratio", "total_real_return"], color="lightgreen"
            )
        )

    except Exception as e:
        st.error(f"Error executing pipeline: {e}")
        st.info("Check if your FRED_API_KEY is correctly set in the .env file.")

else:
    st.info("Configure the parameters and click 'Run Analysis' to start.")
