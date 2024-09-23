from connect import get_connection
import os
import pandas as pd


engine = get_connection()
directory = os.fsencode("data")

with engine.connect() as connection:
    for file in os.listdir("data"):
        if file.endswith(".csv"):
            name = os.path.join("data", file)
            try:
                df = pd.read_csv(name ,sep=None, engine='python')
            except UnicodeDecodeError:
                df = pd.read_csv(name, sep=None, engine='python', encoding="cp1252")
            try:
                df.to_sql(file[:-4], con=engine, index=False, if_exists='replace')
                print(name, " successfully loaded!")
            except Exception as e:
                print(df.head())
                print(name, " couldnt be loaded into MySQL")
                print(e)
                
        