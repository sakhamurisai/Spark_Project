import pandas as pd

chunk_size = 1000000
file_count = 0
for chunk in pd.read_csv(
        'D:\Projects\spark\spark_beginner_course\Spark_Anaconda_basic\data\data_creatorsynthetic_bank_customers_transactions.csv',
        chunksize=chunk_size):
    chunk.to_csv(
        f"D:\\Projects\\spark\\spark_beginner_course\\Spark_Anaconda_basic\\data\\transaction{file_count}.csv"
        + str(file_count) +
        '.csv',
        index=False)
    file_count += 1