#!/usr/bin/env python3
"""
Cleaning → Parquet Agent
Loads all JSON files produced by the data_cleaning_agent from cleaned_data/, normalizes
to a consistent tabular schema, writes Parquet files into processed_data/, and generates
a comprehensive data profile (counts, nulls, uniques, basic outliers) saved to processed_data/analysis/.
"""

import os
import json
import glob
import argparse
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd


DEFAULT_INPUT_DIR = "cleaned_data"
DEFAULT_OUTPUT_DIR = "processed_data"
EXCHANGE_RATE = 3.8
MIN_PRICE = 10
MAX_PRICE = 90000


def read_all_cleaned_json(input_dir: str) -> List[Dict[str, Any]]:
    pattern = os.path.join(input_dir, "*.json")
    records: List[Dict[str, Any]] = []
    for path in sorted(glob.glob(pattern)):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
        except Exception:
            continue
    return records


def normalize_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.json_normalize(records, max_level=1)
    # Standardize expected columns
    preferred_order = [
        "index", "global_index", "scraped_at", "title", "url",
        "location", "has_location", "district", "property_type",
        "price_raw", "price_numeric", "currency", "has_price", "price_per_sqm",
        "area_raw", "area_numeric", "bedrooms", "bathrooms",
        "has_parking", "parking_count", "has_pool", "has_garden", "has_balcony",
        "has_elevator", "has_security", "has_gym", "is_furnished", "allows_pets",
        "is_new", "has_terrace", "has_laundry", "has_air_conditioning",
        "image_count", "image_urls", "page", "site_page", "element_class", "element_tag",
        "data_completeness", "feature_count", "full_text"
    ]
    # Ensure columns exist
    for col in preferred_order:
        if col not in df.columns:
            df[col] = pd.NA
    # Reorder
    df = df[preferred_order]
    # Coerce types
    numeric_cols = [
        "index", "global_index", "price_numeric", "price_per_sqm", "area_numeric",
        "bedrooms", "bathrooms", "parking_count", "image_count", "page", "site_page",
        "data_completeness", "feature_count"
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    bool_cols = [
        "has_location", "has_price", "has_parking", "has_pool", "has_garden",
        "has_balcony", "has_elevator", "has_security", "has_gym", "is_furnished",
        "allows_pets", "is_new", "has_terrace", "has_laundry", "has_air_conditioning"
    ]
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].astype("boolean")
    # Lists
    if "image_urls" in df.columns:
        # keep as list; Parquet will store nested if using pyarrow
        pass
    return df


def write_parquet(df: pd.DataFrame, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(output_dir, f"urbania_cleaned_{ts}.parquet")
    df.to_parquet(out_path, engine="pyarrow", index=False)
    return out_path


def parse_amount(value: str) -> float | None:
    """
    Notebook-aligned rules:
    - 0 comas: convertir directo a float.
    - Si hay al menos 1 coma y la cantidad de dígitos antes de la primera coma > 2:
      cortar TODO después de la primera coma y convertir la parte previa a float.
      ej: "190,000" -> "190" -> 190.0; "1234,567,890" -> "1234" -> 1234.0
    - 1 coma (y no aplica la regla anterior): eliminarla y convertir a float.
      ej: "9,990" -> 9990.0
    - 2+ comas (y no aplica la regla anterior): recortar TODO después de la segunda coma,
      luego eliminar la primera coma y convertir a float.
      ej: "1,400,000" -> "1,400" -> 1400.0
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    comma_idxs = [i for i, ch in enumerate(s) if ch == ',']
    try:
        if len(comma_idxs) == 0:
            return float(s)

        # regla adicional: dígitos antes de la primera coma > 2
        first = comma_idxs[0]
        digits_before = sum(1 for ch in s[:first] if ch.isdigit())
        if digits_before > 2:
            head = ''.join(ch for ch in s[:first] if ch.isdigit() or ch in '+-.')
            return float(head) if head not in ('', '+', '-') else None

        if len(comma_idxs) == 1:
            return float(s.replace(',', ''))

        # 2+ comas: cortar en la segunda, eliminar la primera
        second = comma_idxs[1]
        trimmed = s[:second]
        # eliminar la primera coma del tramo
        first_in_trim = trimmed.find(',')
        if first_in_trim != -1:
            trimmed = trimmed[:first_in_trim] + trimmed[first_in_trim+1:]
        return float(trimmed)
    except ValueError:
        return None


def apply_notebook_cleaning_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reproduce key cleaning steps from verification.ipynb:
    - Infer currency from price_raw (PEN if contains 'S/', USD if contains 'USD' and not PEN)
    - Compute price_numeric_comparable from first segment of price_raw using parse_amount
    - Convert USD price_numeric to PEN using FX=3.8
    - Filter rows with price_numeric > 10 and <= 90000
    - Recompute price_per_sqm when area_numeric > 0
    """
    d = df.copy()

    # Ensure string series for parsing
    pr = d.get("price_raw")
    if pr is None:
        pr = pd.Series([None] * len(d), index=d.index)
    pr = pr.astype(str).fillna("")

    # Infer currency from price_raw
    pen_mask = pr.str.contains("S/", na=False)
    usd_mask = pr.str.contains("USD", na=False) & (~pen_mask)
    # Initialize currency column if missing
    if "currency" not in d.columns:
        d["currency"] = pd.NA
    # Only set when missing or clearly derivable
    d.loc[pen_mask, "currency"] = "PEN"
    d.loc[usd_mask, "currency"] = "USD"

    # Compute price_numeric from first segment of price_raw
    first_seg = pr.str.split(" · ").str[0]
    cleaned = first_seg.str.replace("S/", "", regex=False).str.replace("USD", "", regex=False).str.strip()
    d["price_numeric"] = cleaned.apply(parse_amount)

    # Convert USD prices to PEN at FX 3.8
    usd_final = (d["currency"].astype(str) == "USD") & d["price_numeric"].notna()
    d.loc[usd_final, "price_numeric"] = d.loc[usd_final, "price_numeric"] * EXCHANGE_RATE
    # Set currency to PEN for all rows after conversion
    d.loc[:,"currency"] = "PEN"
    # Filter price range
    d = d[(pd.to_numeric(d["price_numeric"], errors="coerce") > MIN_PRICE) & (pd.to_numeric(d["price_numeric"], errors="coerce") <= MAX_PRICE)]

    # Recompute price_per_sqm where possible
    if "area_numeric" in d.columns:
        area = pd.to_numeric(d["area_numeric"], errors="coerce")
        price = pd.to_numeric(d["price_numeric"], errors="coerce")
        valid = area > 0
        d.loc[valid, "price_per_sqm"] = price[valid] / area[valid]

    return d


def deduplicate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows using robust, stable keys.
    Priority 1: use unique URL when available.
    Priority 2: use a composite business key ignoring non-stable fields like image_urls.
    Also canonicalize list columns (e.g., image_urls) to make them comparable if needed.
    """
    dfc = df.copy()

    # Canonicalize list-like columns to tuples (avoids unhashable when needed)
    if "image_urls" in dfc.columns:
        dfc["image_urls"] = dfc["image_urls"].apply(
            lambda x: tuple(sorted(set(x))) if isinstance(x, list) else x
        )

    before = len(dfc)

    dfc = dfc.sort_values(by=["scraped_at"], na_position="last")

    # Split by URL presence to avoid collapsing all NaN URLs into one
    if "url" in dfc.columns:
        has_url = dfc["url"].notna() & (dfc["url"].astype(str).str.len() > 3) & (dfc["url"].astype(str).str.startswith("http"))
        df_with_url = dfc.loc[has_url]
        df_no_url = dfc.loc[~has_url]

        # Dedup rows that have a real URL
        df_with_url = df_with_url.drop_duplicates(subset=["url"], keep="first")

        # For rows without URL, prefer dedup by normalized full_text when available
        if "full_text" in df_no_url.columns:
            tmp = df_no_url.copy()
            tmp["_ft_norm"] = (
                tmp["full_text"].astype(str).str.replace("\s+", " ", regex=True).str.strip()
            )
            df_no_url = tmp.drop_duplicates(subset=["_ft_norm"], keep="first").drop(columns=["_ft_norm"])
        # Fallback composite key (ignores image_urls)
        key_cols = [
            c for c in [
                "title", "location", "property_type", "price_numeric",
                "area_numeric", "bedrooms", "bathrooms"
            ] if c in df_no_url.columns
        ]
        if key_cols:
            df_no_url = df_no_url.drop_duplicates(subset=key_cols, keep="first")

        dfc = pd.concat([df_with_url, df_no_url], ignore_index=True)
    else:
        # No URL column: use full_text if present
        if "full_text" in dfc.columns:
            dfc["_ft_norm"] = dfc["full_text"].astype(str).str.replace("\s+", " ", regex=True).str.strip()
            dfc = dfc.drop_duplicates(subset=["_ft_norm"], keep="first").drop(columns=["_ft_norm"]) 
        else:
            key_cols = [
                c for c in [
                    "title", "location", "property_type", "price_numeric",
                    "area_numeric", "bedrooms", "bathrooms"
                ] if c in dfc.columns
            ]
            if key_cols:
                dfc = dfc.drop_duplicates(subset=key_cols, keep="first")

    after = len(dfc)
    print(f"Dedup: {before} -> {after} rows")
    return dfc


def basic_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    report["row_count"] = int(len(df))
    report["columns"] = list(df.columns)
    # Null counts
    nulls = df.isna().sum().to_dict()
    report["null_counts"] = {k: int(v) for k, v in nulls.items()}
    # Unique counts (limit to reasonable columns)
    unique_cols = [
        "location", "district", "property_type", "currency"
    ]
    uniques: Dict[str, Any] = {}
    for c in unique_cols:
        if c in df.columns:
            uniques[c] = int(df[c].nunique(dropna=True))
    report["unique_counts"] = uniques
    # Descriptive stats for key numeric columns
    numeric_cols = [
        "price_numeric", "area_numeric", "bedrooms", "bathrooms", "price_per_sqm"
    ]
    stats: Dict[str, Any] = {}
    for c in numeric_cols:
        if c in df.columns:
            desc = df[c].describe().to_dict()
            # convert numpy types to native
            stats[c] = {k: (float(v) if hasattr(v, "item") else v) for k, v in desc.items()}
    report["descriptive_stats"] = stats
    # Simple outlier flags via IQR for price_numeric and price_per_sqm
    outliers: Dict[str, Any] = {}
    for c in ["price_numeric", "price_per_sqm"]:
        if c in df.columns:
            s = df[c].dropna()
            if len(s) >= 10:
                q1 = s.quantile(0.25)
                q3 = s.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                outliers[c] = {
                    "lower_bound": float(lower),
                    "upper_bound": float(upper),
                    "num_below": int((s < lower).sum()),
                    "num_above": int((s > upper).sum())
                }
    report["outliers_iqr"] = outliers
    return report


def save_analysis(report: Dict[str, Any], output_dir: str) -> str:
    analysis_dir = os.path.join(output_dir, "analysis")
    os.makedirs(analysis_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = os.path.join(analysis_dir, f"profile_{ts}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    # Also save a lightweight CSV summary of nulls
    nulls_csv = os.path.join(analysis_dir, f"null_counts_{ts}.csv")
    pd.Series(report.get("null_counts", {})).to_csv(nulls_csv, header=["null_count"]) 
    return out_json


def main():
    parser = argparse.ArgumentParser(description="Cleaned JSON → Parquet + Analysis Agent")
    parser.add_argument("--input-dir", type=str, default=DEFAULT_INPUT_DIR, help="Directory with cleaned JSON files")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Directory to write Parquet and analysis")
    args = parser.parse_args()

    print("=== Cleaning → Parquet Agent ===")
    print(f"Input dir: {args.input_dir}")
    print(f"Output dir: {args.output_dir}")

    records = read_all_cleaned_json(args.input_dir)
    if not records:
        print("No cleaned JSON files found.")
        return
    print(f"Loaded {len(records)} records from {args.input_dir}")

    df = normalize_records(records)
    print(f"Normalized to dataframe with shape: {df.shape}")

    # Apply notebook cleaning steps before deduplication
    df = apply_notebook_cleaning_rules(df)
    print(f"After notebook cleaning rules: {df.shape}")

    # Deduplicate before writing
    df = deduplicate_df(df)
    parquet_path = write_parquet(df, args.output_dir)
    print(f"Parquet written to: {parquet_path}")

    report = basic_analysis(df)
    report_path = save_analysis(report, args.output_dir)
    print(f"Analysis report saved to: {report_path}")


if __name__ == "__main__":
    main()


