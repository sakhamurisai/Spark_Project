from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import *


#basic sanity checks

def basic_validation_checks(df, table_name="cleaned_data", verbose=False):
    """Perform basic validation checks"""
    print(f"=== BASIC VALIDATION FOR {table_name.upper()} ===")
    total_rows = df.count()
    print(f"Total rows: {total_rows:,}")
    print(f"Schema: {len(df.columns)} columns")
    # Only print schema if verbose
    if verbose:
        df.printSchema()
    # Null analysis summary
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    null_summary = []
    for col_name, null_count in null_counts.asDict().items():
        null_pct = (null_count / total_rows) * 100
        if null_count > 0:
            null_summary.append(f"{col_name}: {null_count:,} ({null_pct:.2f}%)")
    if null_summary:
        print("Null columns:")
        for line in null_summary:
            print(f"  {line}")
    else:
        print("No nulls detected.")
    return total_rows

# before and after comparison

def compare_before_after(original_df, cleaned_df, verbose=False):
    """Compare original vs cleaned data"""
    print("=== BEFORE/AFTER COMPARISON ===")
    orig_count = original_df.count()
    clean_count = cleaned_df.count()
    print(f"Original rows: {orig_count:,}")
    print(f"Cleaned rows:  {clean_count:,}")
    print(f"Difference:    {orig_count - clean_count:,} ({((orig_count - clean_count) / orig_count) * 100:.2f}%)")
    if verbose:
        # Optionally print detailed distributions
        for col_name in ["card_type", "transaction_type", "employment_status"]:
            if col_name in original_df.columns:
                print(f"\nOriginal {col_name} distribution:")
                original_df.groupBy(col_name).count().orderBy(desc("count")).show(10)
            if col_name in cleaned_df.columns:
                print(f"\nCleaned {col_name} distribution:")
                cleaned_df.groupBy(col_name).count().orderBy(desc("count")).show(10)

# data quality metrics

def calculate_data_quality_metrics(df, verbose=False):
    """Calculate comprehensive data quality metrics"""
    print("=== DATA QUALITY METRICS ===")
    total_rows = df.count()
    metrics = {}
    for df_column in df.columns:
        dtype = dict(df.dtypes)[df_column]
        is_string = dtype in ["string", "varchar"]
        select_exprs = [
            count(col(df_column)).alias("total"),
            count(when(col(df_column).isNull(), df_column)).alias("nulls"),
            countDistinct(col(df_column)).alias("distinct_values")
        ]
        if is_string:
            select_exprs.append(count(when(col(df_column) == "", df_column)).alias("empty_strings"))
            select_exprs.append(count(when(col(df_column).rlike("^\\s+$"), df_column)).alias("whitespace_only"))
        else:
            select_exprs.append(lit(0).alias("empty_strings"))
            select_exprs.append(lit(0).alias("whitespace_only"))
        col_metrics = df.select(*select_exprs).collect()[0]
        valid_count = col_metrics["total"] - col_metrics["nulls"] - col_metrics["empty_strings"] - col_metrics["whitespace_only"]
        completeness = (valid_count / total_rows) * 100
        uniqueness = (col_metrics["distinct_values"] / valid_count) * 100 if valid_count > 0 else 0
        metrics[df_column] = {
            "completeness": completeness,
            "uniqueness": uniqueness,
            "distinct_values": col_metrics["distinct_values"],
            "null_count": col_metrics["nulls"]
        }
        if not verbose:
            continue
        print(f"{df_column}:")
        print(f"  Completeness: {completeness:.2f}%")
        print(f"  Uniqueness: {uniqueness:.2f}%")
        print(f"  Distinct Values: {col_metrics['distinct_values']:,}")
        print()
    print(f"Columns with < 90% completeness: {[col for col, m in metrics.items() if m['completeness'] < 90]}")
    print(f"Columns with < 80% uniqueness: {[col for col, m in metrics.items() if m['uniqueness'] < 80]}")
    return metrics

# Business Logic Validation

def validate_business_rules(df, verbose=False):
    """Validate business-specific rules"""
    print("=== BUSINESS RULES VALIDATION ===")
    total_rows = df.count()
    validation_results = {}
    # Rule 1: Transaction amounts should be reasonable (positive and within limits)
    if "transaction_amount" in df.columns:
        invalid_amounts = df.filter((col("transaction_amount") < 0) | (col("transaction_amount") > 100000)).count()
        print(f"Invalid transaction amounts: {invalid_amounts:,} ({(invalid_amounts / total_rows) * 100:.2f}%)")
        validation_results["invalid_amounts"] = invalid_amounts
    # Rule 2: Check for impossible dates
    invalid_transaction_dates = 0
    invalid_dob_dates = 0
    invalid_account_dates = 0
    if "transaction_date" in df.columns:
        invalid_transaction_dates = df.filter((col("transaction_date") < "1900-01-01") | (col("transaction_date") > current_date())).count()
        print(f"Invalid transaction dates: {invalid_transaction_dates:,} ({(invalid_transaction_dates / total_rows) * 100:.2f}%)")
    if "dob" in df.columns:
        invalid_dob_dates = df.filter((col("dob") < "1900-01-01") | (col("dob") > current_date())).count()
        print(f"Invalid date of birth: {invalid_dob_dates:,} ({(invalid_dob_dates / total_rows) * 100:.2f}%)")
    if "account_open_date" in df.columns:
        invalid_account_dates = df.filter((col("account_open_date") < "1900-01-01") | (col("account_open_date") > current_date())).count()
        print(f"Invalid account open dates: {invalid_account_dates:,} ({(invalid_account_dates / total_rows) * 100:.2f}%)")
    validation_results.update({
        "invalid_transaction_dates": invalid_transaction_dates,
        "invalid_dob_dates": invalid_dob_dates,
        "invalid_account_dates": invalid_account_dates
    })
    # Rule 3: Check age consistency
    if "age" in df.columns:
        invalid_ages = df.filter((col("age") < 0) | (col("age") > 120)).count()
        print(f"Invalid ages: {invalid_ages:,} ({(invalid_ages / total_rows) * 100:.2f}%)")
        validation_results["invalid_ages"] = invalid_ages
    # Rule 4: Check credit score range
    if "credit_score" in df.columns:
        invalid_credit_scores = df.filter((col("credit_score") < 300) | (col("credit_score") > 850)).count()
        print(f"Invalid credit scores: {invalid_credit_scores:,} ({(invalid_credit_scores / total_rows) * 100:.2f}%)")
        validation_results["invalid_credit_scores"] = invalid_credit_scores
    # Rule 5: Check account balance reasonableness
    if "account_balance" in df.columns:
        negative_balances = df.filter(col("account_balance") < -10000).count()
        extremely_high_balances = df.filter(col("account_balance") > 10000000).count()
        print(f"Negative balances (<-$10K): {negative_balances:,}")
        print(f"High balances (>$10M): {extremely_high_balances:,}")
        validation_results.update({
            "negative_balances": negative_balances,
            "extremely_high_balances": extremely_high_balances
        })
    # Rule 6: Check risk score range
    if "risk_score" in df.columns:
        invalid_risk_scores = df.filter((col("risk_score") < 0) | (col("risk_score") > 1)).count()
        print(f"Invalid risk scores: {invalid_risk_scores:,} ({(invalid_risk_scores / total_rows) * 100:.2f}%)")
        validation_results["invalid_risk_scores"] = invalid_risk_scores
    # Rule 7: Check card last four digits format
    if "card_last_four" in df.columns:
        invalid_card_last_four = df.filter(~col("card_last_four").rlike("^\\d{4}$")).count()
        print(f"Invalid card last four digits: {invalid_card_last_four:,} ({(invalid_card_last_four / total_rows) * 100:.2f}%)")
        validation_results["invalid_card_last_four"] = invalid_card_last_four
    return validation_results

#Statistical Validation

def statistical_validation(original_df, cleaned_df, verbose=False):
    """Compare statistical properties"""
    print("=== STATISTICAL VALIDATION ===")
    numeric_columns = ["transaction_amount", "age", "credit_score", "annual_income", "account_balance", "risk_score"]
    for col_name in numeric_columns:
        if col_name in original_df.columns and col_name in cleaned_df.columns:
            try:
                orig_stats = original_df.select(col_name).describe().collect()
                clean_stats = cleaned_df.select(col_name).describe().collect()
                orig_summary = {row['summary']: row[col_name] for row in orig_stats}
                clean_summary = {row['summary']: row[col_name] for row in clean_stats}
                print(f"{col_name}: before/after mean: {orig_summary.get('mean', 'N/A')} / {clean_summary.get('mean', 'N/A')}")
            except Exception as e:
                if verbose:
                    print(f"  Unable to calculate stats for {col_name}: {str(e)}")


#Sampling and Manual Review

def sample_validation(df, sample_size=1000, verbose=False):
    """Generate samples for manual review"""
    print(f"=== SAMPLING FOR MANUAL REVIEW ({sample_size} records) ===")
    total_rows = df.count()
    if total_rows == 0:
        print("No data available for sampling")
        return
    if verbose:
        # Random sample
        sample_fraction = min(sample_size / total_rows, 1.0)
        random_sample = df.sample(fraction=sample_fraction, seed=42)
        print("Random Sample:")
        random_sample.show(20, truncate=False)
        # Edge cases sample
        edge_case_conditions = []
        if "transaction_amount" in df.columns:
            edge_case_conditions.append(col("transaction_amount") > 10000)
            edge_case_conditions.append(col("transaction_amount") < 1)
        if "card_type" in df.columns:
            edge_case_conditions.append(col("card_type") == "Unknown")
        if "credit_score" in df.columns:
            edge_case_conditions.append(col("credit_score") < 400)
            edge_case_conditions.append(col("credit_score") > 800)
        if "age" in df.columns:
            edge_case_conditions.append(col("age") < 18)
            edge_case_conditions.append(col("age") > 80)
        if "account_balance" in df.columns:
            edge_case_conditions.append(col("account_balance") < 0)
            edge_case_conditions.append(col("account_balance") > 1000000)
        if "is_fraud" in df.columns:
            edge_case_conditions.append(col("is_fraud") == True)
        if "is_flagged" in df.columns:
            edge_case_conditions.append(col("is_flagged") == True)
        if "merchant_name" in df.columns:
            edge_case_conditions.append(col("merchant_name").contains("null"))
            edge_case_conditions.append(col("merchant_name").rlike("^\\s*$"))
        if edge_case_conditions:
            combined_condition = edge_case_conditions[0]
            for condition in edge_case_conditions[1:]:
                combined_condition = combined_condition | condition
            edge_cases = df.filter(combined_condition).limit(100)
            print(f"\nEdge Cases Sample ({edge_cases.count()} records found):")
            edge_cases.show(20, truncate=False)
        else:
            print("\nNo edge case conditions could be applied (missing expected columns)")
        if "annual_income" in df.columns:
            print("\nHigh-Value Customer Sample:")
            high_value_customers = df.filter(col("annual_income") > 100000).limit(20)
            high_value_customers.show(20, truncate=False)
        if "risk_score" in df.columns:
            print("\nHigh-Risk Sample:")
            high_risk_sample = df.filter(col("risk_score") > 0.8).limit(20)
            high_risk_sample.show(20, truncate=False)

