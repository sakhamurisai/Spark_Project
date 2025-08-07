from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import *


#basic sanity checks

def basic_validation_checks(df, table_name="cleaned_data"):
    """Perform basic validation checks"""
    print(f"=== BASIC VALIDATION FOR {table_name.upper()} ===")

    # Row count validation
    total_rows = df.count()
    print(f"Total rows: {total_rows:,}")

    # Schema validation
    print(f"Schema: {len(df.columns)} columns")
    df.printSchema()

    # Null analysis
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    print("\nNull Counts:")
    for col_name, null_count in null_counts.asDict().items():
        null_pct = (null_count / total_rows) * 100
        print(f"  {col_name}: {null_count:,} ({null_pct:.2f}%)")

    return total_rows

# before and after comparison

def compare_before_after(original_df, cleaned_df):
    """Compare original vs cleaned data"""
    print("=== BEFORE/AFTER COMPARISON ===")

    orig_count = original_df.count()
    clean_count = cleaned_df.count()

    print(f"Original rows: {orig_count:,}")
    print(f"Cleaned rows:  {clean_count:,}")
    print(f"Difference:    {orig_count - clean_count:,} ({((orig_count - clean_count) / orig_count) * 100:.2f}%)")

    # Compare specific columns (card_type, transaction_type, employment_status)
    print("\n--- Card Type Comparison ---")
    if "card_type" in original_df.columns:
        orig_card_dist = original_df.groupBy("card_type").count().orderBy(desc("count"))
        print("Original Card Types:")
        orig_card_dist.show(20)

    if "card_type" in cleaned_df.columns:
        clean_card_dist = cleaned_df.groupBy("card_type").count().orderBy(desc("count"))
        print("Cleaned Card Types:")
        clean_card_dist.show(20)

    print("\n--- Transaction Type Comparison ---")
    if "transaction_type" in original_df.columns and "transaction_type" in cleaned_df.columns:
        orig_trans_dist = original_df.groupBy("transaction_type").count().orderBy(desc("count"))
        print("Original Transaction Types:")
        orig_trans_dist.show(20)

        clean_trans_dist = cleaned_df.groupBy("transaction_type").count().orderBy(desc("count"))
        print("Cleaned Transaction Types:")
        clean_trans_dist.show(20)

    print("\n--- Employment Status Comparison ---")
    if "employment_status" in original_df.columns and "employment_status" in cleaned_df.columns:
        orig_emp_dist = original_df.groupBy("employment_status").count().orderBy(desc("count"))
        print("Original Employment Status:")
        orig_emp_dist.show(20)

        clean_emp_dist = cleaned_df.groupBy("employment_status").count().orderBy(desc("count"))
        print("Cleaned Employment Status:")
        clean_emp_dist.show(20)

# data quality metrics

def calculate_data_quality_metrics(df):
    """Calculate comprehensive data quality metrics"""
    print("=== DATA QUALITY METRICS ===")

    total_rows = df.count()
    metrics = {}

    for df_column in df.columns:
        col_metrics = df.select(
            count(col(df_column)).alias("total"),
            count(when(col(df_column).isNull(), df_column)).alias("nulls"),
            count(when(col(df_column) == "", df_column)).alias("empty_strings"),
            count(when(col(df_column).rlike("^\\s+$"), df_column)).alias("whitespace_only"),
            countDistinct(col(df_column)).alias("distinct_values")
        ).collect()[0]

        # Calculate quality scores
        valid_count = col_metrics["total"] - col_metrics["nulls"] - col_metrics["empty_strings"] - col_metrics[
            "whitespace_only"]
        completeness = (valid_count / total_rows) * 100
        uniqueness = (col_metrics["distinct_values"] / valid_count) * 100 if valid_count > 0 else 0

        metrics[column] = {
            "completeness": completeness,
            "uniqueness": uniqueness,
            "distinct_values": col_metrics["distinct_values"],
            "null_count": col_metrics["nulls"]
        }

        print(f"{column}:")
        print(f"  Completeness: {completeness:.2f}%")
        print(f"  Uniqueness: {uniqueness:.2f}%")
        print(f"  Distinct Values: {col_metrics['distinct_values']:,}")
        print()

    return metrics

# Business Logic Validation

def validate_business_rules(df):
    """Validate business-specific rules"""
    print("=== BUSINESS RULES VALIDATION ===")

    total_rows = df.count()
    validation_results = {}

    # Rule 1: Transaction amounts should be reasonable (positive and within limits)
    if "transaction_amount" in df.columns:
        amount_stats = df.select("transaction_amount").describe().collect()
        print("Transaction Amount Statistics:")
        for row in amount_stats:
            print(f"  {row['summary']}: {row['transaction_amount']}")

        # Check for negative or extremely high amounts
        invalid_amounts = df.filter(
            (col("transaction_amount") < 0) | 
            (col("transaction_amount") > 100000)
        ).count()
        print(f"Invalid transaction amounts (negative or >$100K): {invalid_amounts:,} ({(invalid_amounts / total_rows) * 100:.2f}%)")
        validation_results["invalid_amounts"] = invalid_amounts

    # Rule 2: Check for impossible dates
    invalid_transaction_dates = 0
    invalid_dob_dates = 0
    invalid_account_dates = 0

    if "transaction_date" in df.columns:
        invalid_transaction_dates = df.filter(
            (col("transaction_date") < "1900-01-01") |
            (col("transaction_date") > current_date())
        ).count()
        print(f"Invalid transaction dates: {invalid_transaction_dates:,} ({(invalid_transaction_dates / total_rows) * 100:.2f}%)")

    if "dob" in df.columns:
        invalid_dob_dates = df.filter(
            (col("dob") < "1900-01-01") |
            (col("dob") > current_date())
        ).count()
        print(f"Invalid date of birth: {invalid_dob_dates:,} ({(invalid_dob_dates / total_rows) * 100:.2f}%)")

    if "account_open_date" in df.columns:
        invalid_account_dates = df.filter(
            (col("account_open_date") < "1900-01-01") |
            (col("account_open_date") > current_date())
        ).count()
        print(f"Invalid account open dates: {invalid_account_dates:,} ({(invalid_account_dates / total_rows) * 100:.2f}%)")

    validation_results.update({
        "invalid_transaction_dates": invalid_transaction_dates,
        "invalid_dob_dates": invalid_dob_dates,
        "invalid_account_dates": invalid_account_dates
    })

    # Rule 3: Check age consistency
    if "age" in df.columns:
        invalid_ages = df.filter(
            (col("age") < 0) | (col("age") > 120)
        ).count()
        print(f"Invalid ages (negative or >120): {invalid_ages:,} ({(invalid_ages / total_rows) * 100:.2f}%)")
        validation_results["invalid_ages"] = invalid_ages

    # Rule 4: Check credit score range
    if "credit_score" in df.columns:
        invalid_credit_scores = df.filter(
            (col("credit_score") < 300) | (col("credit_score") > 850)
        ).count()
        print(f"Invalid credit scores (outside 300-850 range): {invalid_credit_scores:,} ({(invalid_credit_scores / total_rows) * 100:.2f}%)")
        validation_results["invalid_credit_scores"] = invalid_credit_scores

    # Rule 5: Check account balance reasonableness
    if "account_balance" in df.columns:
        negative_balances = df.filter(col("account_balance") < -10000).count()
        extremely_high_balances = df.filter(col("account_balance") > 10000000).count()
        print(f"Extremely negative balances (<-$10K): {negative_balances:,}")
        print(f"Extremely high balances (>$10M): {extremely_high_balances:,}")
        validation_results.update({
            "negative_balances": negative_balances,
            "extremely_high_balances": extremely_high_balances
        })

    # Rule 6: Check risk score range
    if "risk_score" in df.columns:
        invalid_risk_scores = df.filter(
            (col("risk_score") < 0) | (col("risk_score") > 1)
        ).count()
        print(f"Invalid risk scores (outside 0-1 range): {invalid_risk_scores:,} ({(invalid_risk_scores / total_rows) * 100:.2f}%)")
        validation_results["invalid_risk_scores"] = invalid_risk_scores

    # Rule 7: Check card last four digits format
    if "card_last_four" in df.columns:
        invalid_card_last_four = df.filter(
            ~col("card_last_four").rlike("^\\d{4}$")
        ).count()
        print(f"Invalid card last four digits format: {invalid_card_last_four:,} ({(invalid_card_last_four / total_rows) * 100:.2f}%)")
        validation_results["invalid_card_last_four"] = invalid_card_last_four

    return validation_results

#Statistical Validation

def statistical_validation(original_df, cleaned_df):
    """Compare statistical properties"""
    print("=== STATISTICAL VALIDATION ===")

    # Compare distributions for numerical columns from final_df schema
    numeric_columns = ["transaction_amount", "age", "credit_score", "annual_income", "account_balance", "risk_score"]

    for col_name in numeric_columns:
        if col_name in original_df.columns and col_name in cleaned_df.columns:
            print(f"\n--- {col_name.upper()} Distribution ---")

            # Original statistics
            try:
                orig_stats = original_df.select(col_name).describe().collect()
                print("Original:")
                for row in orig_stats:
                    print(f"  {row['summary']}: {row[col_name]}")
            except Exception as e:
                print(f"  Unable to calculate original stats for {col_name}: {str(e)}")

            # Cleaned statistics
            try:
                clean_stats = cleaned_df.select(col_name).describe().collect()
                print("Cleaned:")
                for row in clean_stats:
                    print(f"  {row['summary']}: {row[col_name]}")
            except Exception as e:
                print(f"  Unable to calculate cleaned stats for {col_name}: {str(e)}")

    # Additional statistical checks for categorical columns
    categorical_columns = ["gender", "employment_status", "account_type", "transaction_type", "card_type", "payment_method"]

    print("\n=== CATEGORICAL DISTRIBUTIONS ===")
    for col_name in categorical_columns:
        if col_name in cleaned_df.columns:
            print(f"\n--- {col_name.upper()} Distribution ---")
            try:
                cat_dist = cleaned_df.groupBy(col_name).count().orderBy(desc("count"))
                cat_dist.show(10)
            except Exception as e:
                print(f"  Unable to calculate distribution for {col_name}: {str(e)}")


#Sampling and Manual Review

def sample_validation(df, sample_size=1000):
    """Generate samples for manual review"""
    print(f"=== SAMPLING FOR MANUAL REVIEW ({sample_size} records) ===")

    total_rows = df.count()
    if total_rows == 0:
        print("No data available for sampling")
        return

    # Random sample
    sample_fraction = min(sample_size / total_rows, 1.0)
    random_sample = df.sample(fraction=sample_fraction, seed=42)

    print("Random Sample:")
    random_sample.show(20, truncate=False)

    # Edge cases sample - focusing on actual columns from final_df
    edge_case_conditions = []

    if "transaction_amount" in df.columns:
        edge_case_conditions.append(col("transaction_amount") > 10000)  # High transaction amounts
        edge_case_conditions.append(col("transaction_amount") < 1)      # Very small amounts

    if "card_type" in df.columns:
        edge_case_conditions.append(col("card_type") == "Unknown")      # Unknown cards

    if "credit_score" in df.columns:
        edge_case_conditions.append(col("credit_score") < 400)          # Very low credit scores
        edge_case_conditions.append(col("credit_score") > 800)          # Very high credit scores

    if "age" in df.columns:
        edge_case_conditions.append(col("age") < 18)                    # Minors
        edge_case_conditions.append(col("age") > 80)                    # Seniors

    if "account_balance" in df.columns:
        edge_case_conditions.append(col("account_balance") < 0)         # Negative balances
        edge_case_conditions.append(col("account_balance") > 1000000)   # High balances

    if "is_fraud" in df.columns:
        edge_case_conditions.append(col("is_fraud") == True)            # Fraudulent transactions

    if "is_flagged" in df.columns:
        edge_case_conditions.append(col("is_flagged") == True)          # Flagged transactions

    if "merchant_name" in df.columns:
        edge_case_conditions.append(col("merchant_name").contains("null"))     # Suspicious merchant names
        edge_case_conditions.append(col("merchant_name").rlike("^\\s*$"))      # Empty/whitespace merchant names

    if edge_case_conditions:
        # Combine all conditions with OR
        combined_condition = edge_case_conditions[0]
        for condition in edge_case_conditions[1:]:
            combined_condition = combined_condition | condition

        edge_cases = df.filter(combined_condition).limit(100)

        print(f"\nEdge Cases Sample ({edge_cases.count()} records found):")
        edge_cases.show(20, truncate=False)
    else:
        print("\nNo edge case conditions could be applied (missing expected columns)")

    # High-value customer sample
    if "annual_income" in df.columns:
        print("\nHigh-Value Customer Sample:")
        high_value_customers = df.filter(col("annual_income") > 100000).limit(20)
        high_value_customers.show(20, truncate=False)

    # Suspicious activity sample
    if "risk_score" in df.columns:
        print("\nHigh-Risk Sample:")
        high_risk_sample = df.filter(col("risk_score") > 0.8).limit(20)
        high_risk_sample.show(20, truncate=False)

