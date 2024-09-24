from sqlalchemy import create_engine, inspect, text
import pandas as pd


USER = 'root'
PASSWORD = 'root'
HOST = 'mysql57'          # see yml file
PORT = 3306
DATABASE = 'bibdb'
 

def get_connection(database=DATABASE):
    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}"\
            .format(USER, PASSWORD, HOST, PORT, database)
    )

 

if __name__ == '__main__':
    try:
        engine = get_connection('mysql')
        with engine.connect() as connection:
            insp = inspect(engine)
            db_list = insp.get_schema_names()
            print(f"Connection to the {HOST} for user {USER} created successfully.")
            if DATABASE not in db_list:
                sql = text(f"CREATE DATABASE {DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
                result = connection.execute(sql)
                if result:
                    print(f"Database {DATABASE} created!")
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)



# df = pd.read_csv("data/Kunde.csv",sep=',')
# df.to_sql('Kunde', con=engine, index=False, if_exists='replace')    # if_exists='append'

#dic = {"a": [1,2,3], "b":[3,4,5]}
#df = pd.DataFrame(data=dic)
#df.to_sql('test', con=engine, index=False, if_exists='replace')