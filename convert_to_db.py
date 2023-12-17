import sqlite3
import pandas as pd

# Загрузите CSV файл в DataFrame
csv_file = 'C:/cabs3.csv'
df = pd.read_csv(csv_file)
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
# Имя для будущей SQLite базы данных
db_name = 'bdd3'

# Установите соединение с базой данных SQLite
conn = sqlite3.connect(db_name)

# Используйте метод to_sql, чтобы записать DataFrame в базу данных SQLite
df.to_sql('cabs3', conn, index=False, if_exists='replace')

# Закройте соединение
conn.close()
