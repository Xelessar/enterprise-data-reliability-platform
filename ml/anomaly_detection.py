"""Row-level anomaly detection with full MLflow experiment tracking.

Isolation Forest is the default (fast, no distributional assumptions, handles
the mixed-scale numeric features typical of enterprise data). Swapping in
Local Outlier Factor or an autoencoder later only means adding a branch to
`_build_model` — the tracking/scoring contract stays the same.
"""
from __future__ import annotations

from dataclasses import dataclass

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor

from feature_engineering.build_features import build_feature_matrix


@dataclass
class AnomalyDetectionResult:
    scored_df: pd.DataFrame
    anomaly_count: int
    anomaly_ratio: float
    mlflow_run_id: str


_SEVERITY_ACTIONS = {
    "critical": "Immediate review — likely data entry error, sensor fault, or a genuine critical event",
    "high": "Review before this record is used in downstream reports or models",
    "medium": "Monitor — may be natural variation, but worth a second look",
}


def _explain_row(row: pd.Series, feature_means: pd.Series, feature_stds: pd.Series) -> tuple[str, str]:
    """Isolation Forest gives no per-feature attribution, so we approximate
    'why' with the feature contributing the largest z-score deviation —
    good enough to turn a bare score into something a reviewer can act on.
    """
    z_scores = (row - feature_means) / feature_stds
    worst_feature = z_scores.abs().idxmax()
    z = z_scores[worst_feature]
    severity = "critical" if abs(z) >= 3 else "high" if abs(z) >= 2 else "medium"
    reason = (
        f"{worst_feature}={row[worst_feature]:.2f} is {abs(z):.1f} std devs from the "
        f"population mean ({feature_means[worst_feature]:.2f})"
    )
    return reason, severity


def _build_model(algorithm: str, contamination: float):
    if algorithm == "isolation_forest":
        return IsolationForest(contamination=contamination, random_state=42, n_estimators=200)
    if algorithm == "local_outlier_factor":
        return LocalOutlierFactor(contamination=contamination, novelty=False)
    raise ValueError(f"Unsupported anomaly detection algorithm: {algorithm}")


def run_anomaly_detection(
    df: pd.DataFrame,
    ml_config: dict,
    tracking_uri: str,
) -> AnomalyDetectionResult:
    ad_cfg = ml_config["anomaly_detection"]
    algorithm = ad_cfg.get("algorithm", "isolation_forest")
    contamination = ad_cfg.get("contamination", 0.05)
    feature_columns = ad_cfg.get("features")

    features = build_feature_matrix(df, feature_columns)

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(ml_config["mlflow"]["experiment_name"])

    with mlflow.start_run() as run:
        mlflow.log_param("algorithm", algorithm)
        mlflow.log_param("contamination", contamination)
        mlflow.log_param("n_features", features.shape[1])
        mlflow.log_param("n_samples", features.shape[0])

        model = _build_model(algorithm, contamination)

        if algorithm == "local_outlier_factor":
            predictions = model.fit_predict(features)
            scores = model.negative_outlier_factor_
        else:
            model.fit(features)
            predictions = model.predict(features)
            scores = model.decision_function(features)
            mlflow.sklearn.log_model(model, artifact_path="model")

        scored_df = df.copy()
        scored_df["anomaly_score"] = scores
        scored_df["is_anomaly"] = predictions == -1

        feature_means = features.mean()
        feature_stds = features.std().replace(0, 1)
        explanations = features.apply(lambda row: _explain_row(row, feature_means, feature_stds), axis=1)
        scored_df["anomaly_reason"] = [e[0] for e in explanations]
        scored_df["anomaly_severity"] = [e[1] for e in explanations]

        anomaly_count = int(scored_df["is_anomaly"].sum())
        anomaly_ratio = round(anomaly_count / len(scored_df), 4) if len(scored_df) else 0.0

        mlflow.log_metric("anomaly_count", anomaly_count)
        mlflow.log_metric("anomaly_ratio", anomaly_ratio)

        return AnomalyDetectionResult(
            scored_df=scored_df,
            anomaly_count=anomaly_count,
            anomaly_ratio=anomaly_ratio,
            mlflow_run_id=run.info.run_id,
        )
