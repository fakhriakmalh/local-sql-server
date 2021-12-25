from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine


dir = "<your directory>"

df_pandas = pd.read_csv(f"{dir}/train_2.csv")
print(len(df_pandas.index))


#connect to local db 
engine = create_engine(f"mssql+pymssql://SA:local_password_fakhri@localhost:1433/localDB", \
                        echo=False)

df_pandas.to_sql("test_table_big",engine)
print("sucess to create new table")
