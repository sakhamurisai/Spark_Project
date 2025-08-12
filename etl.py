import sys
import yaml
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from lib.logger import Log4j
from lib.utils import *
from lib.data_validation import *

# cluster manager  --.master("local[3]")\

if __name__ == "__main__":
    spark = None
    logger = None


    # Load configuration from YAML file
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    spark_conf = config.get("spark", {})
    pg = config['postgresql']
    jar_path = config['postgresql']['jdbc_driver_path']

    # Build SparkSession using all configs from YAML
    builder = SparkSession.builder
    # Handle special keys for appName and master
    if "app_name" in spark_conf:
        builder = builder.appName(spark_conf["app_name"])
    if "master" in spark_conf:
        builder = builder.master(spark_conf["master"])
    # Apply all other configs
    for k, v in spark_conf.items():
        if k not in ("app_name", "master"):
            # Convert underscores to dots for Spark config keys if needed
            spark_key = k.replace("_", ".") if "." not in k else k
            builder = builder.config(f"spark.{spark_key}", v)
    builder = builder.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
    spark = builder.config("spark.jars", jar_path).getOrCreate()
    spark.conf.set("spark.sql.legacy.charVarcharAsString", "true")
    logger = Log4j(spark)
    logger.info("Starting Hello Spark!")


    # Data loading using the integrated data file
    try:
        for file in config.get("data_file", []):
            if file:
                logger.info(f"Using data file: {file}")
                logger.info("Data loading completed successfully")
                logger.info(f"Using data file: {file}")
                uncleaned_df = load_data(spark, file)
                logger.info("Data loading completed successfully")

                # If we reach here, config and data loaded successfully, continue with ETL
                # ... rest of your processing code remains the same ...

                # ADDED: Try-catch for basic cleaning (drop duplicates and nulls)
                try:
                    updated_df = uncleaned_df.dropDuplicates(["customer_id"]) \
                        .dropna(subset=["customer_id"])
                    logger.info("Basic cleaning (drop duplicates and nulls) completed successfully")
                except Exception as e:
                    logger.error(f"Error in basic cleaning: {str(e)}")
                    updated_df = uncleaned_df
                    logger.warn("Continuing with original data without basic cleaning")

                # Cleaning the gender column
                # ADDED: Try-catch for gender column cleaning
                try:
                    gender_udf = udf(gender_clean, StringType())
                    df = updated_df.withColumn("gender", gender_udf(col("gender")))
                    logger.info("Gender column cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in gender column cleaning: {str(e)}")
                    df = updated_df
                    logger.warn("Continuing with original gender column data")

                # Cleaning the customer_id column data
                # ADDED: Try-catch for customer_id filtering
                try:
                    customer_id_clean_df = df.filter(col("customer_id").rlike("^ID-\\d{8}$"))\
                        .filter(col("transaction_id").rlike("^ID-\\d{8}$"))
                    logger.info("Customer ID and Transaction ID filtering completed successfully")
                except Exception as e:
                    logger.error(f"Error in ID filtering: {str(e)}")
                    customer_id_clean_df = df
                    logger.warn("Continuing without ID filtering")

                #cleaning first_name, last_name columns and full_name column

                #Broadcasting in PySpark is used to efficiently share read-only data (like lookup tables, lists, sets)
                # with all the worker nodes in your cluster, without sending a full copy of the data to every task.
                # Broadcast sets
                # ADDED: Try-catch for broadcast variable creation and full_name cleaning
                try:
                    first_name = df.select("first_name").dropna().toPandas()["first_name"].str.lower().tolist()
                    last_name = df.select("last_name").dropna().toPandas()["last_name"].str.lower().tolist()

                    broadcast_first = spark.sparkContext.broadcast(set(first_name))
                    broadcast_last = spark.sparkContext.broadcast(set(last_name))

                    # Create UDF
                    full_name_udf = get_full_name_udf(broadcast_first, broadcast_last)

                    # Apply to column
                    full_name_df = customer_id_clean_df.withColumn("full_name", full_name_udf(col("full_name")))
                    logger.info("Full name cleaning with broadcast variables completed successfully")
                except Exception as e:
                    logger.error(f"Error in full name cleaning with broadcast: {str(e)}")
                    full_name_df = customer_id_clean_df
                    logger.warn("Continuing without full name broadcast cleaning")

                #droping the unused column
                # ADDED: Try-catch for dropping an unused column
                try:
                    drop_column_df = full_name_df.drop("unused_field")
                    logger.info("Unused field column dropped successfully")
                except Exception as e:
                    logger.error(f"Error dropping unused field: {str(e)}")
                    drop_column_df = full_name_df
                    logger.warn("Continuing without dropping unused field")

                # dropping the temp_data column
                # ADDED: Try-catch for dropping temp_data column
                try:
                    drop_temp_data_df = drop_column_df.drop("temp_data")
                    logger.info("Temp_data column dropped successfully")
                except Exception as e:
                    logger.error(f"Error dropping temp_data column: {str(e)}")
                    drop_temp_data_df = drop_column_df
                    logger.warn("Continuing without dropping temp_data column")

                # Applying the udf function for the first name and the last name
                # ADDED: Try-catch for first_name and last_name cleaning
                try:
                    last_name_and_first_name_udf = udf(name_checker,StringType())
                    first_last_name_df = drop_temp_data_df.withColumn("first_name",last_name_and_first_name_udf(col("first_name")))\
                        .withColumn("last_name",last_name_and_first_name_udf(col("last_name")))
                    logger.info("First name and last name cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in first/last name cleaning: {str(e)}")
                    first_last_name_df = drop_temp_data_df
                    logger.warn("Continuing with original first/last name data")

                # cleaning the date of birth column
                # ADDED: Try-catch for date of birth cleaning
                try:
                    format_date_string_udf = udf(format_date,StringType())
                    date_df = first_last_name_df.withColumn("dob",format_date_string_udf(col("dob")))
                    logger.info("Date of birth cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in date of birth cleaning: {str(e)}")
                    date_df = first_last_name_df
                    logger.warn("Continuing with original date of birth data")

                # cleaning the age column
                # ADDED: Try-catch for age cleaning
                try:
                    clean_age_udf = udf(clean_age,StringType())
                    age_df = date_df.withColumn("age",clean_age_udf(col("age"),col("dob")))
                    logger.info("Age column cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in age cleaning: {str(e)}")
                    age_df = date_df
                    logger.warn("Continuing with original age data")

                # cleaning the email column using the function
                # ADDED: Try-catch for email cleaning
                try:
                    email_checker_udf = udf(email_checker,StringType())
                    email_df = age_df.withColumn("email",email_checker_udf(col("email")))
                    logger.info("Email column cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in email cleaning: {str(e)}")
                    email_df = age_df
                    logger.warn("Continuing with original email data")

                #cleaning the phone number column
                # ADDED: Try-catch for phone number cleaning
                try:
                    phone_number_udf = udf(phone_number_checker,StringType())
                    phone_df = email_df.withColumn("phone",phone_number_udf(col("phone")))
                    logger.info("Phone number cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in phone number cleaning: {str(e)}")
                    phone_df = email_df
                    logger.warn("Continuing with original phone data")

                #clening the state column
                # ADDED: Try-catch for state cleaning
                try:
                    state_udf = udf(state_abbreviation,StringType())
                    state_df = phone_df.withColumn("state",state_udf(col("state")))
                    logger.info("State column cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in state cleaning: {str(e)}")
                    state_df = phone_df
                    logger.warn("Continuing with original state data")

                #cleaning the city and the address
                # ADDED: Try-catch for address and city cleaning
                try:
                    address_city_df = address_city_clean(state_df)
                    logger.info("Address and city cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in address/city cleaning: {str(e)}")
                    address_city_df = state_df
                    logger.warn("Continuing with original address/city data")

                #Zip code cleaning
                # ADDED: Try-catch for zip code cleaning
                try:
                    zip_code_df = zip_code_clean(address_city_df)
                    logger.info("Zip code cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in zip code cleaning: {str(e)}")
                    zip_code_df = address_city_df
                    logger.warn("Continuing with original zip code data")

                #cleaning the credit score
                # ADDED: Try-catch for credit score cleaning
                try:
                    credit_score_df = credit_score_clean(zip_code_df)
                    logger.info("Credit score cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in credit score cleaning: {str(e)}")
                    credit_score_df = zip_code_df
                    logger.warn("Continuing with original credit score data")

                #cleaning the annual income
                # ADDED: Try-catch for annual income cleaning
                try:
                    amount_clean_udf = udf(amount_clean,StringType())
                    annual_income_df = credit_score_df.withColumn("annual_income",amount_clean_udf(col("annual_income")))
                    logger.info("Annual income cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in annual income cleaning: {str(e)}")
                    annual_income_df = credit_score_df
                    logger.warn("Continuing with original annual income data")

                #cleaning the employment_status columns
                # ADDED: Try-catch for employment status cleaning
                try:
                    employment_status_udf = udf(employment_status_checker,StringType())
                    employment_status_df = annual_income_df.withColumn("employment_status",employment_status_udf(col("employment_status")))
                    logger.info("Employment status cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in employment status cleaning: {str(e)}")
                    employment_status_df = annual_income_df
                    logger.warn("Continuing with original employment status data")

                #account_open_date cleaning
                # ADDED: Try-catch for account open date cleaning
                try:
                    format_date_string_udf = udf(format_date,StringType())
                    account_open_date_df = employment_status_df.withColumn("account_open_date",format_date_string_udf(col("account_open_date")))
                    logger.info("Account open date cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in account open date cleaning: {str(e)}")
                    account_open_date_df = employment_status_df
                    logger.warn("Continuing with original account open date data")

                #cleaning the account type
                # ADDED: Try-catch for account type cleaning
                try:
                    account_type_udf = udf(account_type_clean,StringType())
                    account_type_df = account_open_date_df.withColumn("account_type",account_type_udf(col("account_type")))
                    logger.info("Account type cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in account type cleaning: {str(e)}")
                    account_type_df = account_open_date_df
                    logger.warn("Continuing with original account type data")

                #cleaning the account_balance
                # ADDED: Try-catch for account balance cleaning
                try:
                    account_balance_df = account_balance_clean(account_type_df)
                    logger.info("Account balance cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in account balance cleaning: {str(e)}")
                    account_balance_df = account_type_df
                    logger.warn("Continuing with original account balance data")

                # Cleaning transaction_date using transaction_date_clean
                # ADDED: Try-catch for transaction date cleaning
                try:
                    transaction_date_clean_udf = udf(format_date, StringType())
                    cleaned_date_df = account_balance_df.withColumn("transaction_date", transaction_date_clean_udf(col("transaction_date")))
                    logger.info("Transaction date cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in transaction date cleaning: {str(e)}")
                    cleaned_date_df = account_balance_df
                    logger.warn("Continuing with original transaction date data")

                #cleaning transaction_amount_clean
                # ADDED: Try-catch for transaction amount cleaning
                try:
                    amount_clean_udf = udf(amount_clean,StringType())
                    transaction_amount_df = cleaned_date_df.withColumn("transaction_amount", amount_clean_udf(col("transaction_amount")))
                    logger.info("Transaction amount cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in transaction amount cleaning: {str(e)}")
                    transaction_amount_df = cleaned_date_df
                    logger.warn("Continuing with original transaction amount data")

                #Cleaning the transaction_type column
                # ADDED: Try-catch for transaction type cleaning
                try:
                    transaction_type_udf = udf(clean_transaction_type_column,StringType())
                    transaction_type_df = transaction_amount_df.withColumn("transaction_type",transaction_type_udf(col("transaction_type")))
                    logger.info("Transaction type cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in transaction type cleaning: {str(e)}")
                    transaction_type_df = transaction_amount_df
                    logger.warn("Continuing with original transaction type data")

                #cleaning the customer_id column transaction_id column and legacy_id column
                # ADDED: Try-catch for universal ID cleaning
                try:
                    universal_id_checker_udf = udf(universal_id_checker,StringType())
                    customer_id_transaction_id_legacy_id_df = transaction_type_df.withColumn("customer_id",universal_id_checker_udf(col("customer_id"))) \
                        .withColumn("transaction_id",universal_id_checker_udf(col("transaction_id"))) \
                        .withColumn("legacy_id",universal_id_checker_udf(col("legacy_id")))
                    logger.info("Universal ID cleaning (customer_id, transaction_id, legacy_id) completed successfully")
                except Exception as e:
                    logger.error(f"Error in universal ID cleaning: {str(e)}")
                    customer_id_transaction_id_legacy_id_df = transaction_type_df
                    logger.warn("Continuing with original ID data")

                #cleaning the merchant name column
                # ADDED: Try-catch for merchant name cleaning
                try:
                    merchant_name_udf = udf(clean_merchant_name,StringType())
                    merchant_name_df = customer_id_transaction_id_legacy_id_df.withColumn("merchant_name",merchant_name_udf(col("merchant_name")))
                    logger.info("Merchant name cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in merchant name cleaning: {str(e)}")
                    merchant_name_df = customer_id_transaction_id_legacy_id_df
                    logger.warn("Continuing with original merchant name data")

                #cleaning the merchant category column
                # ADDED: Try-catch for merchant category cleaning
                try:
                    merchant_category_udf = udf(clean_category,StringType())
                    merchant_category_df = merchant_name_df.withColumn("merchant_category",merchant_category_udf(col("merchant_category")))
                    logger.info("Merchant category cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in merchant category cleaning: {str(e)}")
                    merchant_category_df = merchant_name_df
                    logger.warn("Continuing with original merchant category data")

                #cleaning the payment_method
                # ADDED: Try-catch for payment method cleaning
                try:
                    payment_cat_udf = udf(payment_category_clean,StringType())
                    payment_method_df = merchant_category_df.withColumn("payment_method",payment_cat_udf(col("payment_method")))
                    logger.info("Payment method cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in payment method cleaning: {str(e)}")
                    payment_method_df = merchant_category_df
                    logger.warn("Continuing with original payment method data")

                #cleaning the card_last_four column
                # ADDED: Try-catch for card last four digits cleaning
                try:
                    last_four_udf = udf(clean_last_four_digits_value,StringType())
                    card_last_four_df = payment_method_df.withColumn("card_last_four",last_four_udf(col("card_last_four")))
                    logger.info("Card last four digits cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in card last four digits cleaning: {str(e)}")
                    card_last_four_df = payment_method_df
                    logger.warn("Continuing with original card last four data")

                #cleaning the card_type column
                # ADDED: Try-catch for card type cleaning
                try:
                    card_type_udf = udf(clean_card_type,StringType())
                    card_type_df = card_last_four_df.withColumn("card_type",card_type_udf(col("card_type")))
                    logger.info("Card type cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in card type cleaning: {str(e)}")
                    card_type_df = card_last_four_df
                    logger.warn("Continuing with original card type data")

                #cleaning is_fraud column
                # ADDED: Try-catch for is_fraud boolean cleaning
                try:
                    is_fraud_df = clean_boolean_value(card_type_df)
                    logger.info("Is_fraud boolean cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in is_fraud boolean cleaning: {str(e)}")
                    is_fraud_df = card_type_df
                    logger.warn("Continuing with original is_fraud data")

                #cleaning is_flagged column
                # ADDED: Try-catch for is_flagged boolean cleaning
                try:
                    is_flagged_df = clean_boolean_value(is_fraud_df)
                    logger.info("Is_flagged boolean cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in is_flagged boolean cleaning: {str(e)}")
                    is_flagged_df = is_fraud_df
                    logger.warn("Continuing with original is_flagged data")

                # ADDED: Mid-pipeline validation checkpoint - critical data quality check
                try:
                    logger.info("Performing mid-pipeline validation checkpoint")

                    # Quick validation of critical fields after major transformations
                    current_row_count = is_flagged_df.count()
                    logger.info(f"Records at mid-pipeline checkpoint: {current_row_count:,}")

                    # Check for any critical data loss
                    if current_row_count == 0:
                        logger.error("CRITICAL: No records remaining after cleaning operations")
                        raise Exception("Data pipeline resulted in empty dataset")

                    # Validate key business-critical columns exist and have reasonable data
                    critical_columns = ["customer_id", "transaction_id", "transaction_amount", "transaction_date"]
                    for col_name in critical_columns:
                        if col_name in is_flagged_df.columns:
                            null_count = is_flagged_df.filter(col(col_name).isNull()).count()
                            null_percentage = (null_count / current_row_count) * 100
                            if null_percentage > 50:
                                logger.warn(f"High null percentage in critical column {col_name}: {null_percentage:.2f}%")
                            else:
                                logger.info(f"Critical column {col_name} null percentage: {null_percentage:.2f}%")

                    logger.info("Mid-pipeline validation checkpoint completed successfully")

                except Exception as e:
                    logger.error(f"Error in mid-pipeline validation: {str(e)}")
                    if "empty dataset" in str(e):
                        logger.error("TERMINATING: Cannot continue with empty dataset")
                        sys.exit(-1)
                    else:
                        logger.warn("Continuing despite mid-pipeline validation issues")

                #cleaning risk_score column
                # ADDED: Try-catch for risk score cleaning
                try:
                    amount_clean_udf = udf(amount_clean,StringType())
                    risk_score_df = is_flagged_df.withColumn("risk_score",amount_clean_udf(col("risk_score")))
                    logger.info("Risk score cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in risk score cleaning: {str(e)}")
                    risk_score_df = is_flagged_df
                    logger.warn("Continuing with original risk score data")

                #cleaning notes and internal_comments column
                # ADDED: Try-catch for notes and internal comments cleaning
                try:
                    text_clean_udf = udf(text_clean,StringType())
                    notes_internal_comments_df = risk_score_df.withColumn("notes",text_clean_udf(col("notes"))) \
                        .withColumn("internal_comments",text_clean_udf(col("internal_comments")))
                    logger.info("Notes and internal comments cleaning completed successfully")
                except Exception as e:
                    logger.error(f"Error in notes/internal comments cleaning: {str(e)}")
                    notes_internal_comments_df = risk_score_df
                    logger.warn("Continuing with original notes/internal comments data")
                    logger.info("Data cleaning pipeline completed successfully")
                    logger.info("Final cleaned DataFrame ready for further processing")

                # Remove rows where all non-key columns are null (except 'customer_id' and 'transaction_id')
                try:
                    logger.info("Starting removal of fully null non-key rows")
                    cleaned_df = drop_fully_null_non_key_rows(notes_internal_comments_df)
                    logger.info(f"Null row removal complete. Remaining rows: {cleaned_df.count()}")
                except AnalysisException as ae:
                    cleaned_df = notes_internal_comments_df  # Fallback to original DataFrame
                    logger.error(f"Error           AnalysisException during ETL processing: {ae}")
                except Exception as e:
                    cleaned_df = notes_internal_comments_df  # Fallback to original DataFrame
                    logger.error(f"Error         Unexpected error in ETL pipeline: {e}", exc_info=True)


                # assigning the data types for the column
                # ADDED: Try-catch for final type casting
                try:
                    final_df = cleaned_df.withColumn("customer_id",col("customer_id").cast("varchar(15)")) \
                        .withColumn("first_name", col("first_name").cast("varchar(50)")) \
                        .withColumn("last_name", col("last_name").cast("varchar(50)")) \
                        .withColumn("full_name", col("full_name").cast("varchar(100)")) \
                        .withColumn("gender", col("gender").cast("varchar(10)")) \
                        .withColumn("dob", col("dob").cast("date")) \
                        .withColumn("age", col("age").cast("int")) \
                        .withColumn("email", col("email").cast("varchar(100)")) \
                        .withColumn("phone", col("phone").cast("varchar(15)")) \
                        .withColumn("address",col("address").cast("varchar(100)")) \
                        .withColumn("city",col("city").cast("varchar(20)")) \
                        .withColumn("state",col("state").cast("varchar(15)")) \
                        .withColumn("zip_code",col("zip_code").cast("varchar(6)")) \
                        .withColumn("credit_score", col("credit_score").cast("int")) \
                        .withColumn("annual_income", col("annual_income").cast("double")) \
                        .withColumn("employment_status", col("employment_status").cast("varchar(50)")) \
                        .withColumn("account_open_date",col("account_open_date").cast("date")) \
                        .withColumn("account_type", col("account_type").cast("varchar(100)")) \
                        .withColumn("account_balance", col("account_balance").cast("double")) \
                        .withColumn("transaction_id",col("transaction_id").cast("varchar(15)")) \
                        .withColumn("transaction_date",col("transaction_date").cast("date")) \
                        .withColumn("transaction_amount",col("transaction_amount").cast("double")) \
                        .withColumn("transaction_type",col("transaction_type").cast("varchar(20)")) \
                        .withColumn("merchant_name", col("merchant_name").cast("varchar(100)")) \
                        .withColumn("merchant_category",col("merchant_category").cast("varchar(20)")) \
                        .withColumn("payment_method",col("payment_method").cast("varchar(20)")) \
                        .withColumn("card_last_four",col("card_last_four").cast("varchar(4)"))\
                        .withColumn("card_type",col("card_type").cast("varchar(20)")) \
                        .withColumn("is_flagged",col("is_flagged").cast("boolean")) \
                        .withColumn("is_fraud",col("is_fraud").cast("boolean"))\
                        .withColumn("risk_score",col("risk_score").cast("double")) \
                        .withColumn("notes",col("notes").cast("varchar(1000)")) \
                        .withColumn("internal_comments",col("internal_comments").cast("varchar(1000)")) \
                        .withColumn("legacy_id",col("legacy_id").cast("varchar(15)"))
                    logger.info("Final data type casting completed successfully")
                except Exception as e:
                    logger.error(f"Error in final type casting: {str(e)}")
                    final_df = cleaned_df  # Fallback to uncasted DataFrame
                    logger.warn("Continuing with data without type casting")
                logger.info("Final DataFrame ready for database insertion")

                # saving the data into the postgresql database
                #connecting to the postgresql database
                try:
                    conn = psycopg2.connect(
                        host=pg['host'],
                        port=pg['port'],
                        dbname=pg['database'],
                        user=pg['user'],
                        password=pg['password']
                        )
                    cur = conn.cursor()
                    schema = pg.get('schema', 'public')  # Default to 'public' schema if not specified
                    table_name = pg.get('table_name', 'customer_transactions')

                    logger.info(f"Connected to PostgreSQL database {pg['database']} at {pg['host']}:{pg['port']} as user {pg['user']}")
                    if not table_exists(conn, schema, table_name):
                        logger.info(f"Table {schema}.{table_name} does not exist, creating...")
                        with open("create_table.sql", "r") as f:
                            sql_create = f.read()
                        create_table(conn, sql_create)
                    else:
                        logger.info(f"Table {schema}.{table_name} exists.")

                    # Check if data exists (based on key columns)
                    if data_exists(conn, schema, table_name, final_df):
                        logger.error("Matching data already exists in the table. Skipping insert.")
                    else:
                        logger.info("Inserting new data into the table...")
                        insert_matching_columns(
                            df= final_df,
                            table_name=table_name,
                            conn=conn,
                            schema=schema,
                            pg_password=pg['password'],
                            pg_user=pg['user'],
                            pg_host=pg['host'],
                            pg_port=pg['port'],
                            pg_database=pg['database'])
                        logger.info("Data inserted successfully.")

                except Exception as e:
                    logger.error(f"Error: {e}")

    except Exception as e:
        if logger:
            logger.error(f"Error loading data: {str(e)}")
        else:
            print(f"Error loading data: {str(e)}")
        sys.exit(-1)


    # Safe cleanup

        conn.close()
        cur.close()
        if logger:
            logger.info("Finished Processing!")
        if spark:
            try:
                spark.stop()
            except Exception as e:
                print(f"Error stopping Spark session: {e}")


"""
    # ADDED: Comprehensive final data validation and quality assessment
    try:
        logger.info("Starting comprehensive final data validation")

        # Basic validation checks for final dataset
        total_final_rows = basic_validation_checks(cleaned_df, "final_cleaned_data")
        logger.info(f"Final dataset contains {total_final_rows:,} records")

        # Business rules validation
        logger.info("Validating business rules compliance")
        business_validation_results = validate_business_rules(cleaned_df)
        logger.info("Business rules validation completed")

        # Data quality metrics calculation
        logger.info("Calculating comprehensive data quality metrics")
        quality_metrics = calculate_data_quality_metrics(cleaned_df)
        logger.info("Data quality metrics calculation completed")

        # Before/after comparison if original data is available
        logger.info("Performing before/after comparison")
        compare_before_after(uncleaned_df, cleaned_df)
        logger.info("Before/after comparison completed")

        # Statistical validation
        logger.info("Performing statistical validation")
        statistical_validation(uncleaned_df, cleaned_df)
        logger.info("Statistical validation completed")

        # Sample validation for manual review
        logger.info("Generating samples for manual review")
        sample_validation(cleaned_df, sample_size=100)
        logger.info("Sample validation completed")

        # Log summary of validation results
        logger.info("=== DATA VALIDATION SUMMARY ===")
        logger.info(f"Total records processed: {total_final_rows:,}")
        if business_validation_results:
            for rule, count in business_validation_results.items():
                if count > 0:
                    logger.warn(f"Business rule violation - {rule}: {count:,} records")
                else:
                    logger.info(f"Business rule passed - {rule}: No violations found")

        logger.info("Comprehensive final data validation completed successfully")

    except Exception as e:
        logger.error(f"Error in final data validation: {str(e)}")
        logger.warn("Continuing without comprehensive validation - pipeline completed but validation failed")



"""