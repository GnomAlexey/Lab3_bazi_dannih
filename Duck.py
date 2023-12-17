import duckdb
import time
conn = duckdb.connect(database=':memory:', read_only=False)
times=[0,0,0,0]
#Создаём таблицу cabs используя данные из cabs3.csv
c = conn.cursor()
c.execute(
  "CREATE TABLE cabs AS SELECT * FROM read_csv_auto('cabs3.csv');"
)

for i in range(10):
    s_time = time.time()
    #Выполняем запрос 1
    c.execute( f"""SELECT colour FROM cabs
    GROUP BY 1;
    """)
    db_arr = c.fetchall()
    e_time=time.time()
    times[0]+=(e_time-s_time)


    #Выполняем запрос 2
    c.execute( f"""SELECT passenger_count, avg(total_amount) FROM cabs
        GROUP BY 1;
    """)
    db_arr = c.fetchall()
    e_time=time.time()
    times[1]+=(e_time-s_time)


    #Выполняем запрос 3
    c.execute( f"""SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM cabs
        GROUP BY 1, 2;
        
    """)
    db_arr = c.fetchall()
    e_time=time.time()
    times[2]+=(e_time-s_time)

    # Выполняем запрос 4
    c.execute(f"""SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM cabs
        GROUP BY 1, 2, 3
        ORDER BY 2, 4 desc;
        
    """)
    db_arr = c.fetchall()
    e_time = time.time()
    times[3] += (e_time - s_time)



for j in range(len(times)):
    times[j]=times[j]/10
print(times)
# Закрытие соединения
conn.close()