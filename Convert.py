import pandas as pd
import psycopg2
from sqlalchemy import create_engine

csv_file_path = "C:/Users/GA/Downloads/nyc_yellow_tiny2.csv"
db_host = "127.0.0.1"
db_name = "bdd"
db_user = "postgres"
db_password = "1234"
table_name="cabs3"

df = pd.read_csv(csv_file_path)
ass=pd.DataFrame(df)
for i in range(21):
    if str(df[df.columns[i]].dtypes)=="int64":
        df[df.columns[i]]=df[df.columns[i]].astype(float)
    if i ==1 or i==3:
        b=df[df.columns[i]]
        df[df.columns[i]] = pd.to_datetime(df[df.columns[i]])
        df['Time'] = df[df.columns[i]].dt.time
    a=str(ass[df.columns[i]].name).find('I')
    if a!=-1:
        df.rename(columns={df[df.columns[i]].name:df[df.columns[i]].name[:a]+"_id"},inplace=True)

df['colour']=df['Vendor_id']
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}")
df.to_sql(table_name, engine, index=False, if_exists='replace')
print(f"CSV файл успешно конвертирован в SQL таблицу {table_name}.")