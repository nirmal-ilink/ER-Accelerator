# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 MDM Data Profiling
# MAGIC 
# MAGIC **Enterprise-Grade Data Quality Analysis**
# MAGIC 
# MAGIC This notebook performs comprehensive data profiling including:
# MAGIC - Column-level statistics (nulls, distinct values, patterns)
# MAGIC - Quality metrics (Completeness, Uniqueness, Validity, Consistency)
# MAGIC - Quality rules validation (null checks, email formats, outliers, referential integrity)
# MAGIC 
# MAGIC **Results are stored to a Delta table for UI consumption.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Configuration

# COMMAND ----------

# Widget parameters - these can be set from the UI or when triggering the notebook
dbutils.widgets.text("catalog", "hive_metastore", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("table", "", "Table Name")
dbutils.widgets.text("sample_size", "100000", "Sample Size")
dbutils.widgets.text("connection_id", "", "Connection ID")

# Quality rule toggles
dbutils.widgets.dropdown("check_nulls", "true", ["true", "false"], "Check Null Values")
dbutils.widgets.dropdown("validate_email", "true", ["true", "false"], "Validate Email Formats")
dbutils.widgets.dropdown("detect_outliers", "true", ["true", "false"], "Detect Outliers")
dbutils.widgets.dropdown("check_referential_integrity", "false", ["true", "false"], "Check Referential Integrity")
dbutils.widgets.text("quality_threshold", "95", "Quality Threshold (%)")

# Reference table for referential integrity (optional)
dbutils.widgets.text("reference_table", "", "Reference Table (for integrity check)")
dbutils.widgets.text("reference_column", "", "Reference Column")
dbutils.widgets.text("foreign_key_column", "", "Foreign Key Column")

# COMMAND ----------

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
sample_size = int(dbutils.widgets.get("sample_size"))
connection_id = dbutils.widgets.get("connection_id")

# Quality rule settings
check_nulls = dbutils.widgets.get("check_nulls") == "true"
validate_email = dbutils.widgets.get("validate_email") == "true"
detect_outliers = dbutils.widgets.get("detect_outliers") == "true"
check_referential_integrity = dbutils.widgets.get("check_referential_integrity") == "true"
quality_threshold = int(dbutils.widgets.get("quality_threshold"))

# Reference table settings
reference_table = dbutils.widgets.get("reference_table")
reference_column = dbutils.widgets.get("reference_column")
foreign_key_column = dbutils.widgets.get("foreign_key_column")

print(f"📊 Profiling: {catalog}.{schema}.{table}")
print(f"📏 Sample Size: {sample_size:,}")
print(f"🔧 Quality Rules: nulls={check_nulls}, email={validate_email}, outliers={detect_outliers}, ref_integrity={check_referential_integrity}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports & Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from datetime import datetime
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Load Source Data

# COMMAND ----------

# Construct full table name
if catalog and catalog != "hive_metastore":
    full_table_name = f"{catalog}.{schema}.{table}"
else:
    full_table_name = f"{schema}.{table}"

print(f"Loading data from: {full_table_name}")

# Load the source table with sample limit for performance
source_df = spark.table(full_table_name).limit(sample_size)

# Cache for multiple operations
source_df.cache()

# Get total count (approximate for large tables)
total_rows = source_df.count()
print(f"✅ Loaded {total_rows:,} rows for profiling")

# Get schema information
columns = source_df.columns
dtypes = dict(source_df.dtypes)

print(f"📋 Columns: {len(columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Column-Level Profiling

# COMMAND ----------

def get_column_profile(df, col_name, col_type, total_count):
    """
    Generate comprehensive profile for a single column.
    
    Returns dict with:
    - name, type, null_count, null_pct, distinct_count, unique_count
    - min, max, mean, stddev (for numerics)
    - sample_values, detected_semantic_type
    - pattern_analysis, quality_issues
    """
    profile = {
        "name": col_name,
        "data_type": col_type,
        "total_rows": total_count
    }
    
    # ========== NULL ANALYSIS ==========
    null_count = df.filter(F.col(col_name).isNull() | (F.col(col_name) == "")).count()
    non_null_count = total_count - null_count
    profile["null_count"] = null_count
    profile["null_pct"] = round((null_count / total_count * 100) if total_count > 0 else 0, 2)
    profile["non_null_count"] = non_null_count
    
    # ========== DISTINCT & UNIQUE ANALYSIS ==========
    distinct_count = df.select(col_name).distinct().count()
    profile["distinct_count"] = distinct_count
    profile["distinct_pct"] = round((distinct_count / total_count * 100) if total_count > 0 else 0, 2)
    
    # Unique values (appear exactly once)
    unique_df = df.groupBy(col_name).count().filter(F.col("count") == 1)
    profile["unique_count"] = unique_df.count()
    profile["unique_pct"] = round((profile["unique_count"] / total_count * 100) if total_count > 0 else 0, 2)
    
    # ========== SAMPLE VALUES ==========
    try:
        sample_rows = df.select(col_name).filter(F.col(col_name).isNotNull()).limit(5).collect()
        profile["sample_values"] = [str(row[0])[:50] for row in sample_rows]
    except:
        profile["sample_values"] = []
    
    # ========== NUMERIC STATISTICS ==========
    numeric_types = ["int", "bigint", "float", "double", "decimal", "long", "short"]
    is_numeric = any(t in col_type.lower() for t in numeric_types)
    
    if is_numeric:
        stats = df.select(
            F.min(col_name).alias("min"),
            F.max(col_name).alias("max"),
            F.avg(col_name).alias("mean"),
            F.stddev(col_name).alias("stddev"),
            F.percentile_approx(col_name, 0.25).alias("p25"),
            F.percentile_approx(col_name, 0.50).alias("median"),
            F.percentile_approx(col_name, 0.75).alias("p75")
        ).first()
        
        profile["min"] = float(stats["min"]) if stats["min"] is not None else None
        profile["max"] = float(stats["max"]) if stats["max"] is not None else None
        profile["mean"] = round(float(stats["mean"]), 2) if stats["mean"] is not None else None
        profile["stddev"] = round(float(stats["stddev"]), 2) if stats["stddev"] is not None else None
        profile["p25"] = float(stats["p25"]) if stats["p25"] is not None else None
        profile["median"] = float(stats["median"]) if stats["median"] is not None else None
        profile["p75"] = float(stats["p75"]) if stats["p75"] is not None else None
        profile["is_numeric"] = True
    else:
        profile["is_numeric"] = False
    
    # ========== STRING LENGTH STATS ==========
    string_types = ["string", "varchar", "char", "text"]
    is_string = any(t in col_type.lower() for t in string_types)
    
    if is_string:
        len_stats = df.select(
            F.min(F.length(col_name)).alias("min_len"),
            F.max(F.length(col_name)).alias("max_len"),
            F.avg(F.length(col_name)).alias("avg_len")
        ).first()
        
        profile["min_length"] = int(len_stats["min_len"]) if len_stats["min_len"] is not None else None
        profile["max_length"] = int(len_stats["max_len"]) if len_stats["max_len"] is not None else None
        profile["avg_length"] = round(float(len_stats["avg_len"]), 1) if len_stats["avg_len"] is not None else None
        profile["is_string"] = True
    else:
        profile["is_string"] = False
    
    return profile

# COMMAND ----------

# Profile all columns
print("🔍 Profiling columns...")
column_profiles = []

for col_name in columns:
    col_type = dtypes[col_name]
    profile = get_column_profile(source_df, col_name, col_type, total_rows)
    column_profiles.append(profile)
    print(f"  ✓ {col_name} ({col_type}): {profile['null_pct']}% null, {profile['distinct_count']:,} distinct")

print(f"\n✅ Profiled {len(column_profiles)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Quality Rule: Check Null Values

# COMMAND ----------

null_analysis = {}

if check_nulls:
    print("🔍 Analyzing null values...")
    
    null_summary = []
    for profile in column_profiles:
        null_summary.append({
            "column": profile["name"],
            "null_count": profile["null_count"],
            "null_pct": profile["null_pct"],
            "severity": "critical" if profile["null_pct"] > 10 else "warning" if profile["null_pct"] > 5 else "ok"
        })
    
    null_analysis = {
        "enabled": True,
        "total_columns": len(columns),
        "columns_with_nulls": sum(1 for p in column_profiles if p["null_count"] > 0),
        "total_null_cells": sum(p["null_count"] for p in column_profiles),
        "overall_null_pct": round(sum(p["null_pct"] for p in column_profiles) / len(columns), 2) if columns else 0,
        "column_details": null_summary
    }
    
    print(f"  📊 Columns with nulls: {null_analysis['columns_with_nulls']}/{len(columns)}")
    print(f"  📊 Total null cells: {null_analysis['total_null_cells']:,}")
else:
    null_analysis = {"enabled": False}
    print("⏭️ Null check disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📧 Quality Rule: Validate Email Formats

# COMMAND ----------

email_analysis = {}

if validate_email:
    print("📧 Validating email formats...")
    
    # Email regex pattern (RFC 5322 simplified)
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Find columns that might contain emails
    potential_email_cols = [col for col in columns if any(
        kw in col.lower() for kw in ['email', 'mail', 'e_mail', 'e-mail', 'contact']
    )]
    
    if not potential_email_cols:
        # Check string columns for email patterns
        for col_name in columns:
            if dtypes[col_name] in ["string", "varchar"]:
                sample = source_df.select(col_name).filter(F.col(col_name).isNotNull()).limit(10).collect()
                for row in sample:
                    val = str(row[0]) if row[0] else ""
                    if re.match(email_pattern, val):
                        potential_email_cols.append(col_name)
                        break
    
    email_validations = []
    total_valid = 0
    total_invalid = 0
    total_checked = 0
    
    for col_name in potential_email_cols:
        # Count valid and invalid emails
        email_col_df = source_df.select(col_name).filter(F.col(col_name).isNotNull())
        col_total = email_col_df.count()
        
        valid_count = email_col_df.filter(
            F.col(col_name).rlike(email_pattern)
        ).count()
        
        invalid_count = col_total - valid_count
        
        # Get sample invalid emails
        invalid_samples = email_col_df.filter(
            ~F.col(col_name).rlike(email_pattern)
        ).limit(5).collect()
        
        email_validations.append({
            "column": col_name,
            "total_checked": col_total,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "valid_pct": round((valid_count / col_total * 100) if col_total > 0 else 0, 2),
            "invalid_samples": [str(row[0])[:50] for row in invalid_samples]
        })
        
        total_valid += valid_count
        total_invalid += invalid_count
        total_checked += col_total
        
        print(f"  ✓ {col_name}: {valid_count:,} valid, {invalid_count:,} invalid ({round(valid_count/col_total*100 if col_total else 0, 1)}%)")
    
    email_analysis = {
        "enabled": True,
        "columns_checked": potential_email_cols,
        "total_emails_checked": total_checked,
        "total_valid": total_valid,
        "total_invalid": total_invalid,
        "overall_valid_pct": round((total_valid / total_checked * 100) if total_checked > 0 else 100, 2),
        "column_details": email_validations
    }
else:
    email_analysis = {"enabled": False}
    print("⏭️ Email validation disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Quality Rule: Detect Outliers

# COMMAND ----------

outlier_analysis = {}

if detect_outliers:
    print("📈 Detecting outliers using IQR method...")
    
    # Find numeric columns
    numeric_types = ["int", "bigint", "float", "double", "decimal", "long", "short"]
    numeric_cols = [col for col in columns if any(t in dtypes[col].lower() for t in numeric_types)]
    
    outlier_results = []
    total_outliers = 0
    
    for col_name in numeric_cols:
        # Calculate IQR
        quartiles = source_df.select(
            F.percentile_approx(col_name, 0.25).alias("q1"),
            F.percentile_approx(col_name, 0.75).alias("q3")
        ).first()
        
        q1 = quartiles["q1"]
        q3 = quartiles["q3"]
        
        if q1 is not None and q3 is not None:
            iqr = q3 - q1
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            
            # Count outliers
            outlier_count = source_df.filter(
                (F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound)
            ).count()
            
            col_total = source_df.filter(F.col(col_name).isNotNull()).count()
            
            # Get sample outliers
            outlier_samples = source_df.filter(
                (F.col(col_name) < lower_bound) | (F.col(col_name) > upper_bound)
            ).select(col_name).limit(5).collect()
            
            outlier_results.append({
                "column": col_name,
                "q1": round(float(q1), 2),
                "q3": round(float(q3), 2),
                "iqr": round(float(iqr), 2),
                "lower_bound": round(float(lower_bound), 2),
                "upper_bound": round(float(upper_bound), 2),
                "outlier_count": outlier_count,
                "outlier_pct": round((outlier_count / col_total * 100) if col_total > 0 else 0, 2),
                "outlier_samples": [float(row[0]) for row in outlier_samples if row[0] is not None]
            })
            
            total_outliers += outlier_count
            
            if outlier_count > 0:
                print(f"  ⚠️ {col_name}: {outlier_count:,} outliers (bounds: [{lower_bound:.2f}, {upper_bound:.2f}])")
    
    outlier_analysis = {
        "enabled": True,
        "method": "IQR (1.5x)",
        "numeric_columns_checked": len(numeric_cols),
        "total_outliers_found": total_outliers,
        "column_details": outlier_results
    }
    
    print(f"\n  📊 Total outliers found: {total_outliers:,}")
else:
    outlier_analysis = {"enabled": False}
    print("⏭️ Outlier detection disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 Quality Rule: Check Referential Integrity

# COMMAND ----------

referential_analysis = {}

if check_referential_integrity and reference_table and foreign_key_column and reference_column:
    print(f"🔗 Checking referential integrity: {table}.{foreign_key_column} → {reference_table}.{reference_column}")
    
    try:
        # Construct reference table name
        if catalog and catalog != "hive_metastore":
            ref_full_name = f"{catalog}.{schema}.{reference_table}"
        else:
            ref_full_name = f"{schema}.{reference_table}"
        
        # Load reference table
        ref_df = spark.table(ref_full_name).select(reference_column).distinct()
        ref_values = set([row[0] for row in ref_df.collect()])
        
        # Check foreign keys
        fk_df = source_df.select(foreign_key_column).filter(F.col(foreign_key_column).isNotNull())
        fk_total = fk_df.count()
        
        # Find orphaned values (FK not in reference table)
        orphaned = fk_df.filter(~F.col(foreign_key_column).isin(list(ref_values)))
        orphan_count = orphaned.count()
        
        # Get sample orphans
        orphan_samples = orphaned.limit(10).collect()
        
        referential_analysis = {
            "enabled": True,
            "source_table": f"{schema}.{table}",
            "source_column": foreign_key_column,
            "reference_table": reference_table,
            "reference_column": reference_column,
            "total_fk_values": fk_total,
            "reference_values_count": len(ref_values),
            "orphaned_count": orphan_count,
            "integrity_pct": round(((fk_total - orphan_count) / fk_total * 100) if fk_total > 0 else 100, 2),
            "orphan_samples": [str(row[0])[:50] for row in orphan_samples]
        }
        
        if orphan_count > 0:
            print(f"  ⚠️ Found {orphan_count:,} orphaned records ({referential_analysis['integrity_pct']}% integrity)")
        else:
            print(f"  ✅ All {fk_total:,} foreign keys are valid (100% integrity)")
            
    except Exception as e:
        referential_analysis = {
            "enabled": True,
            "error": str(e)
        }
        print(f"  ❌ Error checking referential integrity: {e}")
else:
    referential_analysis = {"enabled": False}
    if check_referential_integrity:
        print("⏭️ Referential integrity check skipped (missing reference table configuration)")
    else:
        print("⏭️ Referential integrity check disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Calculate Overall Quality Metrics

# COMMAND ----------

def calculate_quality_metrics(column_profiles, null_analysis, email_analysis, outlier_analysis, referential_analysis):
    """
    Calculate the four key quality metrics:
    - Completeness: % of non-null values
    - Uniqueness: % of distinct values
    - Validity: % of values passing format validation
    - Consistency: Cross-field quality score
    """
    
    # ========== COMPLETENESS (Non-null values) ==========
    total_cells = sum(p["total_rows"] for p in column_profiles)
    total_null_cells = sum(p["null_count"] for p in column_profiles)
    completeness = round(((total_cells - total_null_cells) / total_cells * 100) if total_cells > 0 else 100, 1)
    
    # ========== UNIQUENESS (Distinct records) ==========
    # Average distinctness across columns (weighted by non-null values)
    distinctness_scores = []
    for p in column_profiles:
        if p["non_null_count"] > 0:
            distinctness_scores.append(p["distinct_count"] / p["non_null_count"] * 100)
    uniqueness = round(sum(distinctness_scores) / len(distinctness_scores), 1) if distinctness_scores else 100
    
    # ========== VALIDITY (Format compliance) ==========
    validity_scores = []
    
    # Email validity
    if email_analysis.get("enabled") and email_analysis.get("total_emails_checked", 0) > 0:
        validity_scores.append(email_analysis["overall_valid_pct"])
    
    # If no specific validation, assume 100% (conservative)
    if not validity_scores:
        validity_scores.append(100)
    
    validity = round(sum(validity_scores) / len(validity_scores), 1)
    
    # ========== CONSISTENCY (Cross-field accuracy) ==========
    consistency_scores = []
    
    # Referential integrity contributes to consistency
    if referential_analysis.get("enabled") and "integrity_pct" in referential_analysis:
        consistency_scores.append(referential_analysis["integrity_pct"])
    
    # Outlier percentage (inverse - fewer outliers = more consistent)
    if outlier_analysis.get("enabled"):
        total_numeric = sum(1 for p in column_profiles if p.get("is_numeric"))
        if total_numeric > 0 and outlier_analysis.get("total_outliers_found", 0) > 0:
            total_numeric_vals = sum(p["non_null_count"] for p in column_profiles if p.get("is_numeric"))
            outlier_pct = (outlier_analysis["total_outliers_found"] / total_numeric_vals * 100) if total_numeric_vals else 0
            consistency_scores.append(100 - min(outlier_pct * 2, 20))  # Cap penalty at 20%
    
    # Default to 100 if no checks
    if not consistency_scores:
        consistency_scores.append(100)
    
    consistency = round(sum(consistency_scores) / len(consistency_scores), 1)
    
    # Overall quality score
    overall = round((completeness + uniqueness + validity + consistency) / 4, 1)
    
    return {
        "completeness": completeness,
        "uniqueness": uniqueness,
        "validity": validity,
        "consistency": consistency,
        "overall": overall
    }

# Calculate metrics
quality_metrics = calculate_quality_metrics(
    column_profiles, null_analysis, email_analysis, outlier_analysis, referential_analysis
)

print("=" * 60)
print("📊 DATA QUALITY SUMMARY")
print("=" * 60)
print(f"  Completeness (Non-null values):    {quality_metrics['completeness']}%")
print(f"  Uniqueness (Distinct records):     {quality_metrics['uniqueness']}%")
print(f"  Validity (Format compliance):      {quality_metrics['validity']}%")
print(f"  Consistency (Cross-field):         {quality_metrics['consistency']}%")
print("-" * 60)
print(f"  OVERALL QUALITY SCORE:             {quality_metrics['overall']}%")
print("=" * 60)

# Check against threshold
if quality_metrics['overall'] >= quality_threshold:
    print(f"✅ PASSED - Quality score {quality_metrics['overall']}% meets threshold {quality_threshold}%")
    quality_status = "PASSED"
else:
    print(f"❌ FAILED - Quality score {quality_metrics['overall']}% below threshold {quality_threshold}%")
    quality_status = "FAILED"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Results to Delta Table

# COMMAND ----------

# Prepare results payload
profiling_result = {
    "connection_id": connection_id,
    "catalog": catalog,
    "schema": schema,
    "table": table,
    "profiled_at": datetime.now().isoformat(),
    "total_rows": total_rows,
    "total_columns": len(columns),
    "sample_size": sample_size,
    "quality_metrics": quality_metrics,
    "quality_status": quality_status,
    "quality_threshold": quality_threshold,
    "column_profiles": column_profiles,
    "null_analysis": null_analysis,
    "email_analysis": email_analysis,
    "outlier_analysis": outlier_analysis,
    "referential_analysis": referential_analysis
}

# Convert to JSON string for storage
result_json = json.dumps(profiling_result, default=str)

# COMMAND ----------

# Create results DataFrame
from pyspark.sql import Row

result_row = Row(
    connection_id=connection_id,
    catalog=catalog,
    schema_name=schema,
    table_name=table,
    profiled_at=datetime.now(),
    total_rows=total_rows,
    total_columns=len(columns),
    sample_size=sample_size,
    completeness=quality_metrics["completeness"],
    uniqueness=quality_metrics["uniqueness"],
    validity=quality_metrics["validity"],
    consistency=quality_metrics["consistency"],
    overall_score=quality_metrics["overall"],
    quality_status=quality_status,
    quality_threshold=quality_threshold,
    full_result_json=result_json
)

results_df = spark.createDataFrame([result_row])

# COMMAND ----------

# Save to Delta table (create if not exists)
results_table = f"{catalog}.{schema}.mdm_profiling_results" if catalog != "hive_metastore" else f"{schema}.mdm_profiling_results"

try:
    # Append new results
    results_df.write.format("delta").mode("append").saveAsTable(results_table)
    print(f"✅ Results saved to: {results_table}")
except Exception as e:
    # Table doesn't exist, create it
    results_df.write.format("delta").mode("overwrite").saveAsTable(results_table)
    print(f"✅ Created table and saved results: {results_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Return Results (for UI consumption)

# COMMAND ----------

# Return the profiling result as notebook output
# This can be captured when running the notebook via jobs API
dbutils.notebook.exit(result_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Display Results Summary

# COMMAND ----------

# Display column profiles as a table
column_summary_data = []
for p in column_profiles:
    column_summary_data.append({
        "Column": p["name"],
        "Type": p["data_type"],
        "Null %": f"{p['null_pct']}%",
        "Distinct": f"{p['distinct_count']:,}",
        "Unique": f"{p['unique_count']:,}",
        "Samples": ", ".join(p.get("sample_values", [])[:3])
    })

column_summary_df = spark.createDataFrame(column_summary_data)
display(column_summary_df)

# COMMAND ----------

# Display quality rule results
print("\n📋 QUALITY RULE RESULTS")
print("-" * 50)

if null_analysis.get("enabled"):
    print(f"\n🔍 NULL CHECK: {null_analysis['columns_with_nulls']}/{null_analysis['total_columns']} columns have nulls")
    
if email_analysis.get("enabled"):
    print(f"\n📧 EMAIL VALIDATION: {email_analysis.get('overall_valid_pct', 'N/A')}% valid")
    
if outlier_analysis.get("enabled"):
    print(f"\n📈 OUTLIER DETECTION: {outlier_analysis.get('total_outliers_found', 0):,} outliers found")
    
if referential_analysis.get("enabled"):
    if "integrity_pct" in referential_analysis:
        print(f"\n🔗 REFERENTIAL INTEGRITY: {referential_analysis['integrity_pct']}%")
    elif "error" in referential_analysis:
        print(f"\n🔗 REFERENTIAL INTEGRITY: Error - {referential_analysis['error']}")
