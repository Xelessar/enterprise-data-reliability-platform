"""Monitoring dashboard: pipeline status, DQ score trend, anomalies, and
Root Cause Analysis for failed runs. Reads directly from `pipeline_runs` /
`dq_results` — no separate API layer for a project this size.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from config.settings import get_db_url
from monitoring.tracker import PipelineRunTracker

st.set_page_config(page_title="Data Reliability Platform", layout="wide")


@st.cache_resource
def get_tracker() -> PipelineRunTracker:
    return PipelineRunTracker(create_engine(get_db_url()))


def load_runs(limit: int = 200) -> pd.DataFrame:
    runs = get_tracker().recent_runs(limit=limit)
    return pd.DataFrame(runs)


st.title("Enterprise Data Reliability & Intelligence Platform")

df = load_runs()

if df.empty:
    st.info(
        "No pipeline runs recorded yet. Trigger the `data_reliability_pipeline` "
        "DAG in Airflow to populate this dashboard."
    )
    st.stop()

col1, col2, col3, col4 = st.columns(4)
latest = df.iloc[0]
success_rate = get_tracker().success_rate()

col1.metric("Success rate (last 100 runs)", f"{success_rate}%")
col2.metric("Latest DQ score", f"{latest['dq_score']}" if pd.notna(latest["dq_score"]) else "n/a")
col3.metric("Latest status", str(latest["status"]).upper())
col4.metric("Latest anomaly count", int(latest["anomaly_count"]) if pd.notna(latest["anomaly_count"]) else 0)

st.subheader("Pipeline run history")
st.dataframe(
    df[["run_id", "status", "started_at", "duration_seconds", "row_count", "dq_score", "anomaly_count"]],
    use_container_width=True,
)

st.subheader("Data Quality Score trend")
dq_trend = df.dropna(subset=["dq_score"]).sort_values("started_at")
if not dq_trend.empty:
    fig = px.line(dq_trend, x="started_at", y="dq_score", markers=True)
    fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="fail threshold")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.caption("No DQ score history yet.")

st.subheader("Failed runs — Root Cause Analysis")
failed = df[df["status"] == "failed"]
if failed.empty:
    st.success("No failed runs.")
else:
    for _, row in failed.iterrows():
        with st.expander(f"{row['run_id']} — {row['started_at']}"):
            st.code(row["rca_summary"] or "No RCA summary captured.", language="text")
