"""
Anomaly Detector Mod for DataPy Framework.

Generic anomaly detection on numeric columns using statistical methods.
Detects outliers using IQR method and Z-score analysis.
"""

from pathlib import Path
import os
import json
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# ---------------------------------------------------------------------
# Required metadata
# ---------------------------------------------------------------------
METADATA = ModMetadata(
    type="anomaly_detector",
    version="1.1.0",
    description="Generic anomaly detection on numeric data using statistical methods",
    category="transformer",
    input_ports=["data"],
    output_ports=["anomalies", "clean_data"],
    globals=["total_anomalies", "anomaly_rate", "columns_analyzed"],
    packages=["pandas>=1.5.0", "numpy>=1.20.0"]
)

# ---------------------------------------------------------------------
# Parameter schema (kept simple on purpose)
# ---------------------------------------------------------------------
CONFIG_SCHEMA = ConfigSchema(
    required={},
    optional={
        "data": {
            "type": "object",
            "default": None,
            "description": "Input DataFrame OR CSV file path (str)"
        },
        "file_path": {
            "type": "str",
            "default": None,
            "description": "Path to CSV file to read if 'data' is not provided"
        },
        "numeric_columns": {
            "type": "list",
            "default": None,
            "description": "List of numeric columns to analyze (None = auto-detect)"
        },
        "method": {
            "type": "str",
            "default": "iqr",
            "description": "Detection method: 'iqr' or 'zscore'"
        },
        "threshold": {
            "type": "float",
            "default": 1.5,
            "description": "Threshold for outlier detection (IQR multiplier or Z-score)"
        },
        "output_anomalies_only": {
            "type": "bool",
            "default": False,
            "description": "If True, only anomalies are written (no clean_data)"
        },
        # Minimal output controls (CSV only)
        "output_dir": {
            "type": "str",
            "default": "demo_output",
            "description": "Directory to write CSV outputs (created if missing)"
        },
        "file_prefix": {
            "type": "str",
            "default": "anomaly",
            "description": "Prefix for output filenames"
        }
    }
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _resolve_path(p: Union[str, Path]) -> Path:
    """Expand env/user and resolve to absolute Path."""
    return Path(os.path.expandvars(os.path.expanduser(str(p)))).resolve()

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _write_csv(df: pd.DataFrame, path: Path, write_index: bool = False) -> None:
    df.to_csv(path, index=write_index)

def _load_input_data(logger, data_param: Any, file_path: Optional[str]) -> pd.DataFrame:
    """
    Load input:
      - If data_param is a DataFrame -> return it
      - If data_param is a string -> treat as CSV path
      - Else, if file_path provided -> read that CSV
      - Otherwise -> error
    """
    if isinstance(data_param, pd.DataFrame):
        logger.info("Using provided DataFrame input", extra={"rows": len(data_param)})
        return data_param

    if isinstance(data_param, (str, Path)):
        csv_path = _resolve_path(data_param)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at path: {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info("Loaded data from 'data' path", extra={"path": str(csv_path), "rows": len(df)})
        return df

    if file_path is not None:
        csv_path = _resolve_path(file_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at file_path: {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info("Loaded data from 'file_path'", extra={"path": str(csv_path), "rows": len(df)})
        return df

    raise TypeError("Provide a DataFrame in 'data', a CSV path in 'data', or a 'file_path' to CSV.")

# ---------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------
def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute anomaly detection with given parameters.

    Returns:
        ModResult dictionary with file paths, metrics, and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "anomaly_detector")
    mod_type = params.get("_mod_type", "anomaly_detector")

    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)

    # Initialize result
    result = ModResult(mod_type, mod_name)

    try:
        # Extract parameters
        data_param = params.get("data")
        file_path = params.get("file_path")
        numeric_columns = params.get("numeric_columns", None)
        method = params.get("method", "iqr")
        threshold = params.get("threshold", 1.5)
        output_anomalies_only = params.get("output_anomalies_only", False)

        # Output controls (simple)
        output_dir = _resolve_path(params.get("output_dir", "demo_output"))
        file_prefix = params.get("file_prefix", "anomaly")
        _ensure_dir(output_dir)

        # Load input as DataFrame (supports DataFrame or path)
        try:
            data = _load_input_data(logger, data_param, file_path)
        except Exception as e:
            error_msg = f"Failed to prepare input data: {e}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()

        logger.info(
            "Starting anomaly detection",
            extra={"method": method, "threshold": threshold, "input_rows": len(data)}
        )

        # Validate input
        if not isinstance(data, pd.DataFrame):
            error_msg = f"Input data must be a pandas DataFrame, got {type(data)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()

        if data.empty:
            warning_msg = "Input data is empty"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
            # write empty outputs for consistency
            anomalies_csv = output_dir / f"{file_prefix}_anomalies.csv"
            clean_csv = output_dir / f"{file_prefix}_clean.csv"
            details_json = output_dir / f"{file_prefix}_details.json"
            _write_csv(pd.DataFrame(), anomalies_csv)
            if not output_anomalies_only:
                _write_csv(pd.DataFrame(), clean_csv)
            with open(details_json, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
            result.add_artifact("anomalies_path", str(anomalies_csv))
            if not output_anomalies_only:
                result.add_artifact("clean_data_path", str(clean_csv))
            result.add_artifact("analysis_details_path", str(details_json))
            return result.warning()

        # Auto-detect numeric columns if not specified
        if numeric_columns is None:
            numeric_columns = data.select_dtypes(include=[np.number]).columns.tolist()
            logger.info(f"Auto-detected numeric columns: {numeric_columns}")

        if not numeric_columns:
            warning_msg = "No numeric columns found for anomaly detection"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
            # Write passthrough clean data + empty anomalies
            anomalies_csv = output_dir / f"{file_prefix}_anomalies.csv"
            clean_csv = output_dir / f"{file_prefix}_clean.csv"
            details_json = output_dir / f"{file_prefix}_details.json"
            _write_csv(pd.DataFrame(), anomalies_csv)
            if not output_anomalies_only:
                _write_csv(data.copy(), clean_csv)
            with open(details_json, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
            result.add_artifact("anomalies_path", str(anomalies_csv))
            if not output_anomalies_only:
                result.add_artifact("clean_data_path", str(clean_csv))
            result.add_artifact("analysis_details_path", str(details_json))
            return result.warning()

        # Validate specified columns exist
        missing_columns = [c for c in numeric_columns if c not in data.columns]
        if missing_columns:
            error_msg = f"Specified columns not found in data: {missing_columns}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()

        # Prepare analysis
        analysis_data = data[numeric_columns].copy()

        anomaly_mask = pd.Series(False, index=data.index)
        anomaly_details: Dict[str, Any] = {}

        for column in numeric_columns:
            col_data = analysis_data[column].dropna()

            if len(col_data) == 0:
                logger.warning(f"Column '{column}' has no valid data, skipping")
                continue

            if method == "iqr":
                Q1 = col_data.quantile(0.25)
                Q3 = col_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR

                col_anomalies = (data[column] < lower_bound) | (data[column] > upper_bound)
                anomaly_details[column] = {
                    "method": "IQR",
                    "lower_bound": float(lower_bound),
                    "upper_bound": float(upper_bound),
                    "threshold": threshold
                }

            elif method == "zscore":
                mean_val = col_data.mean()
                std_val = col_data.std()
                if std_val == 0:
                    logger.warning(f"Column '{column}' has zero std deviation, skipping")
                    continue
                z_scores = np.abs((data[column] - mean_val) / std_val)
                col_anomalies = z_scores > threshold
                anomaly_details[column] = {
                    "method": "Z-Score",
                    "mean": float(mean_val),
                    "std": float(std_val),
                    "threshold": threshold
                }

            else:
                error_msg = f"Unknown method '{method}'. Use 'iqr' or 'zscore'"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()

            anomaly_mask |= col_anomalies.fillna(False)
            logger.info(f"Column '{column}': {int(col_anomalies.fillna(False).sum())} anomalies detected")

        # Split records
        anomaly_records = data[anomaly_mask].copy()
        clean_records = data[~anomaly_mask].copy()

        # Add per-row scores for insight (simple, no extra params)
        if not anomaly_records.empty:
            scores_rows = []
            for idx in anomaly_records.index:
                row_scores = {}
                for column in numeric_columns:
                    if column in anomaly_details:
                        val = anomaly_records.at[idx, column] if column in anomaly_records.columns else np.nan
                        if pd.isna(val):
                            row_scores[f"{column}_score"] = 0.0
                        else:
                            detail = anomaly_details[column]
                            if detail["method"] == "IQR":
                                lower_dist = max(0.0, detail["lower_bound"] - float(val))
                                upper_dist = max(0.0, float(val) - detail["upper_bound"])
                                row_scores[f"{column}_score"] = max(lower_dist, upper_dist)
                            else:  # Z-score
                                row_scores[f"{column}_score"] = abs(
                                    (float(val) - detail["mean"]) / detail["std"]
                                )
                scores_rows.append(row_scores)
            if scores_rows:
                score_df = pd.DataFrame(scores_rows, index=anomaly_records.index)
                anomaly_records = pd.concat([anomaly_records, score_df], axis=1)

        # Metrics
        total_records = len(data)
        total_anomalies = len(anomaly_records)
        anomaly_rate = (total_anomalies / total_records) if total_records > 0 else 0.0

        logger.info(
            "Anomaly detection completed",
            extra={
                "total_records": total_records,
                "anomalies_found": total_anomalies,
                "anomaly_rate": f"{anomaly_rate:.2%}",
                "columns_analyzed": len(numeric_columns)
            }
        )

        # Persist results (CSV only, minimal)
        anomalies_csv = output_dir / f"{file_prefix}_anomalies.csv"
        clean_csv = output_dir / f"{file_prefix}_clean.csv"
        details_json = output_dir / f"{file_prefix}_details.json"

        _write_csv(anomaly_records, anomalies_csv)
        if not output_anomalies_only:
            _write_csv(clean_records, clean_csv)
        with open(details_json, "w", encoding="utf-8") as f:
            json.dump(anomaly_details, f, ensure_ascii=False, indent=2)

        logger.info("Outputs written", extra={
            "output_dir": str(output_dir),
            "anomalies_file": str(anomalies_csv),
            "clean_file": None if output_anomalies_only else str(clean_csv),
            "details_file": str(details_json)
        })

        # Report metrics
        result.add_metric("total_records", total_records)
        result.add_metric("total_anomalies", total_anomalies)
        result.add_metric("clean_records", total_records - total_anomalies)
        result.add_metric("anomaly_rate", round(anomaly_rate, 4))
        result.add_metric("columns_analyzed", len(numeric_columns))
        result.add_metric("method_used", method)
        result.add_metric("threshold_used", threshold)

        # Artifacts = file paths
        result.add_artifact("anomalies_path", str(anomalies_csv))
        if not output_anomalies_only:
            result.add_artifact("clean_data_path", str(clean_csv))
        result.add_artifact("analysis_details_path", str(details_json))

        # Globals for downstream mods
        result.add_global("total_anomalies", total_anomalies)
        result.add_global("anomaly_rate", round(anomaly_rate, 4))
        result.add_global("columns_analyzed", len(numeric_columns))

        # Status
        if total_anomalies > 0:
            result.add_warning(f"Found {total_anomalies} anomalies ({anomaly_rate:.2%} of data)")
            return result.warning()
        else:
            return result.success()

    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()

    except Exception as e:
        error_msg = f"Unexpected error in anomaly detector: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()
