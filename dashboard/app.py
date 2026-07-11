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


def csv_download_button(df: pd.DataFrame, label: str, filename: str, key: str) -> None:
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        key=key,
    )


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
run_history_cols = ["run_id", "status", "started_at", "duration_seconds", "row_count", "dq_score", "anomaly_count"]
st.dataframe(df[run_history_cols], use_container_width=True)
csv_download_button(df[run_history_cols], "Download run history (CSV)", "pipeline_run_history.csv", key="dl_runs")

st.subheader("Data Quality Score trend")
dq_trend = df.dropna(subset=["dq_score"]).sort_values("started_at")
if not dq_trend.empty:
    fig = px.line(dq_trend, x="started_at", y="dq_score", markers=True)
    fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="fail threshold")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.caption("No DQ score history yet.")

st.subheader("Anomalous records — which data needs review")
runs_with_anomalies = df[df["anomaly_count"].fillna(0) > 0]
if runs_with_anomalies.empty:
    st.caption("No anomalies flagged in any run yet.")
else:
    options = {
        f"{r['run_id']} ({int(r['anomaly_count'])} flagged)": r["id"]
        for _, r in runs_with_anomalies.iterrows()
    }
    choice = st.selectbox("Run", list(options.keys()))
    anomalies = get_tracker().anomalies_for_run(options[choice])
    if anomalies:
        RECOMMENDED_ACTION = {
            "critical": "Immediate review — likely data entry error, sensor fault, or a genuine critical event",
            "high": "Review before this record is used in downstream reports or models",
            "medium": "Monitor — may be natural variation, but worth a second look",
        }
        SEVERITY_ICON = {"critical": "🔴", "high": "🟠", "medium": "🟡"}

        anomalies_df = pd.DataFrame(anomalies)
        anomalies_df["severity"] = anomalies_df["severity"].fillna("medium")
        anomalies_df["Severity"] = anomalies_df["severity"].map(
            lambda s: f"{SEVERITY_ICON.get(s, '')} {s.upper()}"
        )
        anomalies_df["Recommended action"] = anomalies_df["severity"].map(
            lambda s: RECOMMENDED_ACTION.get(s, "Review")
        )
        anomalies_df = anomalies_df.rename(
            columns={
                "record_id": "id",
                "record_timestamp": "timestamp",
                "record_value": "value",
                "anomaly_score": "score (lower = more anomalous)",
                "reason": "Reason flagged",
            }
        )
        anomalies_display_cols = [
            "id",
            "timestamp",
            "value",
            "Severity",
            "Reason flagged",
            "Recommended action",
            "score (lower = more anomalous)",
        ]
        st.dataframe(anomalies_df[anomalies_display_cols], use_container_width=True)
        csv_download_button(
            anomalies_df[anomalies_display_cols],
            "Download flagged records (CSV)",
            f"anomalies_{choice.split(' ')[0]}.csv",
            key="dl_anomalies",
        )
    else:
        st.caption("Anomaly count was recorded but no per-record detail was saved for this run.")

st.subheader("Failed runs — Root Cause Analysis")
failed = df[df["status"] == "failed"]
if failed.empty:
    st.success("No failed runs.")
else:
    for _, row in failed.iterrows():
        with st.expander(f"{row['run_id']} — {row['started_at']}"):
            st.code(row["rca_summary"] or "No RCA summary captured.", language="text")
