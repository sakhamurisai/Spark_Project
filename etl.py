import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

# cluster manager  --.master("local[3]")\

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder\
        .config("spark.driver.extraJavaOptions","-Dlog4j.configuration=file:log4j.properties" )\
        .config(conf=conf)\
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting Hello Spark!")
#Your Processing Code
    if len(sys.argv) != 2:
        logger.error("Usage: Hello_Spark <filename>")
        sys.exit(-1)

    uncleaned_df = load_data(spark, sys.argv[1])
#    print(f"{uncleaned_df.rdd.getNumPartitions()}: these are the number of partitions of the spark dataframe divided internally")
    updated_df = uncleaned_df.dropDuplicates(["customer_id"]) \
        .dropna(subset=["customer_id"])
# Cleaning the gender column
    gender_udf = udf(gender_clean, StringType())
    df = updated_df.withColumn("gender", gender_udf(col("gender")))

# Cleaning the customer_id column data
    customer_id_clean_df = df.filter(col("customer_id").rlike("^ID-\\d{8}$"))\
        .filter(col("transaction_id").rlike("^ID-\\d{8}$"))

#cleaning first_name, last_name columns and full_name column

#Broadcasting in PySpark is used to efficiently share read-only data (like lookup tables, lists, sets)
# with all the worker nodes in your cluster, without sending a full copy of the data to every task.
    # Broadcast sets

    first_name = df.select("first_name").dropna().toPandas()["first_name"].str.lower().tolist()
    last_name = df.select("last_name").dropna().toPandas()["last_name"].str.lower().tolist()

#usually we can use this but the amount data is too large for the local machine to handle so removed all the
    # null values and converted to the pandas then to a list
#first_name_list = [str(x).lower() for x in df.select("first_name").rdd.flatMap(lambda x: x).collect()]
#last_name_list = [str(x).lower() for x in df.select("last_name").rdd.flatMap(lambda x: x).collect()]
#
    broadcast_first = spark.sparkContext.broadcast(set(first_name))
    broadcast_last = spark.sparkContext.broadcast(set(last_name))

    # Create UDF
    full_name_udf = get_full_name_udf(broadcast_first, broadcast_last)

    # Apply to column
    full_name_df = customer_id_clean_df.withColumn("full_name", full_name_udf(col("full_name")))

    #droping the unused column
    drop_column_df = full_name_df.drop("unused_field")

    # Applying the udf function for the first name and the last name
    last_name_and_first_name_udf =udf(name_checker,StringType())
    # applying that function to the first_name and last_name
    first_last_name_df = drop_column_df.withColumn("first_name",last_name_and_first_name_udf(col("first_name")))\
        .withColumn("last_name",last_name_and_first_name_udf(col("last_name")))

# cleaning the date of birth column
    format_date_string_udf = udf(format_date,StringType())
    date_df = first_last_name_df.withColumn("dob",format_date_string_udf(col("dob")))

# cleaning the age column
    date1_df = date_df.withColumn("dob", to_date(col("dob"), "MM-dd-yyyy"))
    age1_df = date1_df.withColumn("age",
                                  when(length(regexp_replace(col("age"),r"[^\d]",""))>0,
                                       regexp_replace(col("age"),r"[^\d]","").try_cast("int")
                                       ).otherwise(None)
                                  )
    age_df = age_clean(age1_df)

# cleaning the email column using the function
    email_checker_udf = udf(email_checker,StringType())
    email_df = age_df.withColumn("email",email_checker_udf(col("email")))

#cleaning the phone number column
    phone_number_udf = udf(phone_number_checker,StringType())
    phone_df = email_df.withColumn("phone",phone_number_udf(col("phone")))

#clening the state column
    state_udf = udf(state_abbreviation,StringType())
    state_df = phone_df.withColumn("state",state_udf(col("state")))

#cleaning the city and the address
    address_city_df = address_city_clean(state_df)
#Zip code cleaning
    zip_code_df = zip_code_clean(address_city_df)
#cleaning the credit score
    credit_score_df = credit_score_clean(zip_code_df)
#cleaning the annual income
    annual_income_udf = udf(amount_clean,StringType())
    annual_income_df = credit_score_df.withColumn("annual_income",annual_income_udf(col("annual_income")))
#cleaning the employment_status columns
    employment_status_udf = udf(employment_status_checker,StringType())
    employment_status_df = annual_income_df.withColumn("employment_status",employment_status_udf(col("employment_status")))
#account_open_date cleaning
    account_open_date_df = employment_status_df.withColumn("account_open_date",format_date_string_udf(col("account_open_date")))
#cleaning the account type
    account_type_udf = udf(account_type_clean,StringType())
    account_type_df = account_open_date_df.withColumn("account_type",account_type_udf(col("account_type")))
#cleaning the account_balance
    account_balance_df = account_balance_clean(account_type_df)
# Cleaning transaction_date using transaction_date_clean
    transaction_date_clean_udf = udf(format_date, StringType())
    cleaned_date_df = account_balance_df.withColumn("transaction_date", transaction_date_clean_udf(col("transaction_date")))
#cleaning transaction_amount_clean
    transaction_amount_udf = udf(amount_clean, StringType())
    transaction_amount_df = cleaned_date_df.withColumn("transaction_amount", transaction_amount_udf(col("transaction_amount")))
#Cleaning the transaction_type column
    transaction_type_udf = udf(clean_transaction_type_column,StringType())
    transaction_type_df = transaction_amount_df.withColumn("transaction_type",transaction_type_udf(col("transaction_type")))
#cleaning the customer_id column transaction_id column and legacy_id column
    universal_id_checker_udf = udf(universal_id_checker,StringType())
    customer_id_transaction_id_legacy_id_df = transaction_type_df.withColumn("customer_id",universal_id_checker_udf(col("customer_id"))) \
        .withColumn("transaction_id",universal_id_checker_udf(col("transaction_id"))) \
        .withColumn("legacy_id",universal_id_checker_udf(col("legacy_id")))
#cleaning the merchant name column
    merchant_name_udf = udf(clean_merchant_name,StringType())
    merchant_name_df = customer_id_transaction_id_legacy_id_df.withColumn("merchant_name",merchant_name_udf(col("merchant_name")))
#cleaning the merchant category column
    merchant_category_udf = udf(clean_category,StringType())
    merchant_category_df = merchant_name_df.withColumn("merchant_category",merchant_category_udf(col("merchant_category")))
#cleaning the payment_method
    payment_cat_udf = udf(payment_category_clean,StringType())
    payment_method_df = merchant_category_df.withColumn("payment_method",payment_cat_udf(col("payment_method")))
#cleaning the card_last_four column
    card_last_four_df = payment_method_df.withColumn("card_last_four",universal_id_checker_udf(col("card_last_four")))

    payment_method_df.groupBy("card_type").count().orderBy(desc("count")).show(20)
    spark.stop()