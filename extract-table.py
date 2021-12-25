from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

#connect to local db 
engine = create_engine(f"mssql+pymssql://SA:local_password_fakhri@localhost:1433/localDB", \
                        echo=False)

df_sql_query=pd.read_sql_table("test_table_big", con=engine)

dir = "<destination directory>"

now = datetime.now()
current_time = now.strftime("%d/%m/%Y %H:%M:%S")
print(f"start writing table = {current_time}")
df_sql_query.to_parquet(f"{dir}/big_table_transport.parquet")

now = datetime.now()
current_time = now.strftime("%d/%m/%Y %H:%M:%S")
print(f"finish writing table = {current_time}")
