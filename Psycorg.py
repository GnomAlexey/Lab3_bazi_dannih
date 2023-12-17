import psycopg2
import time
db_host = "127.0.0.1"
db_name = "bd"
db_user = "postgres"
db_password = "1234"
table_name="cabs"
conn = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password
)
times=[0,0,0,0]
cursor = conn.cursor()
for i in range(10):
    s_time = time.time()
    #Выполняем запрос 1
    selectDB = f"""SELECT colour FROM cabs
    GROUP BY 1;
    """
    cursor.execute(selectDB)
    #Переводим ответ в список
    db_arr = cursor.fetchall()

    e_time=time.time()
    times[0]+=(e_time-s_time)


    #Выполняем запрос 2
    s_time = time.time()
    selectDB = f"""SELECT passenger_count, avg(total_amount) FROM cabs
    GROUP BY 1;
    
    """
    cursor.execute(selectDB)
    #Переводим ответ в список
    db_arr = cursor.fetchall()
    e_time=time.time()
    times[1] += (e_time-s_time)


    #Выполняем запрос 3
    s_time = time.time()
    selectDB = f"""
    SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM cabs
    GROUP BY 1, 2;
    
    """
    cursor.execute(selectDB)
    #Переводим ответ в список
    db_arr = cursor.fetchall()
    e_time=time.time()
    times[2] += (e_time-s_time)


    #Выполняем запрос 4
    s_time = time.time()
    selectDB = f"""
    SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM cabs
    GROUP BY 1, 2, 3
    ORDER BY 2, 4 desc;
    
    """
    cursor.execute(selectDB)
    #Переводим ответ в список
    db_arr = cursor.fetchall()
    e_time=time.time()
    times[3] += (e_time-s_time)


for j in range(len(times)):
    times[j]=times[j]/10
print(times)
cursor.close()
conn.close()