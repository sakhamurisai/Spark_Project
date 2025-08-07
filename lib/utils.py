import configparser
from datetime import *
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf

exclude_first = {"firstname", "fname", "first_name", "null","Firstname","FName"}
exclude_last = {"lastname", "lname", "last_name", "null","Lastname","LName"}

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('spark.conf')

    for (key,val) in config.items('SPARK_APP_CONFIG'):
        spark_conf.set(key,val)
    return spark_conf

def load_data(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)


def get_full_name_udf(first_names_broadcast, last_names_broadcast):
    def clean_name(name):
        if name is None:
            return None

        name = re.sub(r'[^\w\s]', ' ', name)
        words = name.strip().split()

        if not words:
            return None

        temp_first_name = ""
        temp_last_name = ""
        suffixes = {"jr", "sr"}

        first_names = first_names_broadcast.value
        last_names = last_names_broadcast.value

        for word in words:
            word_l = word.lower()
            if word_l in first_names and word_l not in exclude_first:
                temp_first_name = word
            elif word_l in last_names and word_l not in exclude_last:
                temp_last_name = word
            elif word_l in suffixes:
                temp_last_name += f" {word}"

        full_name = (temp_first_name + " " + temp_last_name).strip()
        return full_name if full_name else None

    return udf(clean_name, StringType())

def name_checker(name):
    pattern = r"^[a-zA-Z]+$"
    if name is not None and re.match(pattern, name.strip()) and name not in exclude_first and name not in exclude_last:
        return name.strip()
    else:
        return "NULL"

def format_date(date_string):
    date_string = str(date_string).strip()
    valid_date_patterns_list = [
        {
            "expression": {
                "pattern": r"^\d{14}$",
                "format": "%Y%m%d%H%M%S"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{4}-\d{2}-\d{2}$",
                "format": "%Y-%m-%d"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{4}/\d{2}/\d{2}$",
                "format": "%Y/%m/%d"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{2}/\d{2}/\d{4}$",
                "format": "%m/%d/%Y"  # or "%d/%m/%Y" based on a region
            }
        },
        {
            "expression": {
                "pattern": r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$",
                "format": "%m/%d/%Y %H:%M"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$",
                "format": "%Y/%m/%d %H:%M"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{2}-[A-Za-z]{3}-\d{2}$",
                "format": "%d-%b-%y"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{2}-[A-Za-z]{3}-\d{4}$",
                "format": "%d-%b-%Y"
            }
        },
        {
            "expression": {
                "pattern": r"^[A-Za-z]{3}-\d{2}-\d{4}$",
                "format": "%b-%d-%Y"
            }
        },
        {
            "expression": {
                "pattern": r"^[A-Za-z]{3} \d{2} \d{4}$",
                "format": "%b %d %Y"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{4}$",
                "format": "%Y"
            }
        },
        {
            "expression": {
                "pattern": r"^\d{1,2} [A-Za-z]+ \d{4}$",
                "format": "%d %B %Y"
            }
        },
        {
            "expression": {
                "pattern": r"^[A-Za-z]+ \d{1,2}, \d{4}$",
                "format": "%B %d, %Y"
            }
        }
    ]

    if date_string is None:
        return None
    for item in valid_date_patterns_list:
        pattern = item["expression"]["pattern"]
        fmt = item["expression"]["format"]
        if re.match(pattern, date_string):
            try:
                dt = datetime.strptime(date_string, fmt)
                return dt.strftime("%m-%d-%Y")
            except Exception:
                continue
    return None

def email_checker(email):
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    if email is not None and re.match(pattern, email.strip()):
        return email.strip()
    else:
        return None

def phone_number_checker(phone_number):
    if phone_number is not None:
        # Remove all non-digit characters
        digits = re.sub(r"\D", "", phone_number)

        # Check if we have exactly 10 digits
        if re.fullmatch(r"\d{10}", digits):
            return re.sub(r"(\d{3})(\d{3})(\d{4})", r"\1-\2-\3", digits)
        else:
            return None
    else:
        return None

def state_abbreviation(state):
    us_states = {
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "IA": "Iowa",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "ME": "Maine",
        "MD": "Maryland",
        "MA": "Massachusetts",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MS": "Mississippi",
        "MO": "Missouri",
        "MT": "Montana",
        "NE": "Nebraska",
        "NV": "Nevada",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NY": "New York",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VT": "Vermont",
        "VA": "Virginia",
        "WA": "Washington",
        "WV": "West Virginia",
        "WI": "Wisconsin",
        "WY": "Wyoming"
    }
    if state is None:
        return None
    else:
        for key, value in us_states.items():
            if state.upper() == value.upper() or state.upper() == key.upper():
                return value
    return None

def address_city_clean(data_frame):
    return data_frame.withColumn("address",
                    initcap(                      # Proper case: "123 main st" → "123 Main St"
                        regexp_replace(           # Remove unwanted characters
                            trim(col("address")), # Trim whitespace
                            r"[^a-zA-Z0-9\s,.-]", ""  # Keep only letters, numbers, spaces, commas, dots, hyphens
                        )
                    )) \
               .withColumn("city",
                    initcap(
                        regexp_replace(
                            trim(col("city")),
                            r"[^a-zA-Z\s]", ""   # Keep only letters and spaces
                        )
                    ))

def zip_code_clean(df):
    return df.withColumn(
        "zip_code",when(
            col("zip_code").isNotNull(),
            regexp_extract(regexp_replace(col("zip_code"), r"[^\d]", ""), r"(\d{5})", 1)
        ).otherwise(when(
            trim(col("zip_code")) == "",  # Handles empty and all-space zip_codes
            None
        ).otherwise(None))
    )

def age_clean(df):
    return df.withColumn(
    "age",
    when(
        (col("dob").isNotNull()) &
        (col("age").isNotNull()) &
        (abs(col("age") - floor(datediff(current_date(), col("dob")) / 365)) < 1),
        col("age")
    ).otherwise(
        when(
            col("age").isNull() & col("dob").isNotNull(),
            floor(datediff(current_date(), col("dob")) / 365)
        ).otherwise(None)
    )
    )

def credit_score_clean(df):
    return df.withColumn(
        "credit_score",
        when(
            col("credit_score").isNotNull(),
            regexp_extract(
                regexp_replace(col("credit_score"), r"[^\d]", ""),
                r"(\d{2,})",
                1)
        )
    )

def gender_clean(gender):
    """
    Normalize a gender string into 'Male', 'Female', 'Non-binary', or 'NULL'.
    Recognizes various textual representations, including 'unknown', 'o', etc. as Non-binary.
    Args:
        gender (str): the gender indicator
    Returns:
        str or None: Standardized gender label or None
    """
    if gender is None:
        return None
    gender_str = str(gender).strip().lower()
    if gender_str in {"m", "male"}:
        return "Male"
    elif gender_str in {"f", "female"}:
        return "Female"
    elif gender_str in {"o", "unknown", "non-binary", "nonbinary", "nb", "other"}:
        return "Non-binary"
    else:
        return "NULL"


def account_type_clean(account_type):
    account_type_list = ["Checking", "CHK", "Student", "Business","SVG","Savings","Joint","Premium",]
    if account_type is None:
        return None
    else:
        for item in account_type_list:
            if account_type.upper() == item.upper():
                return item
            else:
                if account_type == "Retail":
                    return "Business"
                elif account_type == "Online":
                    return "Online"
                else:
                    return None
        return None

def account_balance_clean(df):
    return df.withColumn(
        "account_balance",
        when(
            col("account_balance").isNotNull(),
            regexp_extract(
                regexp_replace(col("account_balance"), r"[^\d.]", ""),
                r"(\d*\.?\d+)",
                1
            ).cast("double")
        ).otherwise(None)
    )

def amount_clean(amount_string):
    if amount_string is not None:
        # Remove non-digit and non-decimal characters
        cleaned = re.sub(r"[^\d.]", "", str(amount_string))

        # Extract the first valid number (integer or float)
        match = re.search(r"\d*\.?\d+", cleaned)
        if match:
            return match.group()
    return None

def universal_id_checker(id_value):
    """
    Checks if the given id_value matches:
    - 'ID-XXXXXXXX'(8 digits, for transaction_id and customer_id)
    - 'L-XXXXXXXX'(8 digits, for legacy_id)
    Returns the cleaned ID string if valid, else None.
    """
    if id_value is None:
        return None
    s = str(id_value).strip()
    pattern = re.compile(r"^(ID|L)-\d{8}$")
    if pattern.match(s):
        return s
    return None

def employment_status_checker(status):
    """
    Returns the status if it's a valid employment status, else None.
    Valid statuses are case-insensitive.
    """
    if status is None:
        return None
    valid_statuses = {
        "employed",
        "unemployed",
        "student",
        "retired",
        "self-employed",
        "homemaker",
        "part-time",
        "full-time",
        "unknown"
    }
    status_clean = str(status).strip().lower()
    # Check normalized status against valid statuses
    for valid in valid_statuses:
        if status_clean == valid:
            return valid.title() if "-" not in valid else valid.capitalize()  # e.g., "Full-Time"
    return None

def clean_transaction_type_column(transaction_type):
    transaction_type_mapping = {
        'W/D': 'Withdrawal',
        'DEP': 'Deposit',
        'PUR': 'Purchase',
        'XFR': 'Transfer',
        'Purchase': 'Purchase',
        'Payment': 'Payment',
        'Deposit': 'Deposit',
        'Withdrawal': 'Withdrawal',
        'Transfer': 'Transfer',
        'W': 'Withdrawal',
    }
    if transaction_type is None:
        return None
    else:
        string = str(transaction_type).strip()
        return transaction_type_mapping.get(string, "Unknown")

def clean_merchant_name(name):
    suffixes_list = ['inc', 'corp', 'ltd', 'llc', 'plc', 'co', 'company', 'group']
    suffix_pattern = re.compile(r'\b(' + '|'.join(suffixes_list) + r')\b', re.IGNORECASE)
    if not name:
        return None

    # Normalize whitespace and punctuation
    name = re.sub(r"[^\w\s]", " ", name)   # remove commas, dots etc.
    name = re.sub(r"\s+", " ", name).strip()

    tokens = name.split()
    suffix = None
    main_tokens = []

    # Find suffix if it exists
    for token in tokens:
        if suffix_pattern.fullmatch(token.lower()):
            suffix = token
        else:
            main_tokens.append(token)

    # Title-case the cleaned main name
    cleaned_name = ' '.join(main_tokens).title()
    # Add a suffix at the end if exists
    if suffix:
        cleaned_name += f' {suffix.upper()}'
    return cleaned_name


def clean_category(cat):
    valid_categories = [
        "TRAVEL", "ENTERTAINMENT", "GROCERIES", "RETAIL",
        "ONLINE", "GAS", "DINING", "OTHER", "UNKNOWN"
    ]
    if cat is None:
        return "Unknown"
    # Remove whitespace, quotes, backslashes, special characters
    cat = cat.strip().upper()
    cat = re.sub(r'[^A-Z]', '', cat)  # Keep only alphabets
    if cat in valid_categories:
        return cat.capitalize()
    else:
        return "Unknown"

def payment_category_clean(payment_cat):
    payment_categories = {
        "CC": "Credit Card",
        "CREDIT CARD": "Credit Card",
        "DC": "Debit Card",
        "DEBIT CARD": "Debit Card",
        "CHK": "Check",
        "CHECK": "Check",
        "ACH": "ACH",
        "NULL": "Unknown"
    }

    if payment_cat is None:
        return None
    # Remove whitespace, quotes, backslashes, special characters
    payment_cat = payment_cat.strip().upper()
    payment_cat = re.sub(r'[^A-Z]', '', payment_cat)  # Keep only alphabets
    return payment_categories.get(payment_cat, "Unknown")

