import sqlite3
import time
connection = sqlite3.connect('bdd3')
cursor = connection.cursor()
times=[0,0,0,0]

for i in range(10):
    #Выполняем запрос 1
    s_time = time.time()
    cursor.execute("""SELECT colour FROM cabs3
    GROUP BY 1;""")
    # Выводим результаты
    pr = cursor.fetchall()
    e_time = time.time()
    times[0] += (e_time - s_time)


    #Выполняем запрос 2
    s_time = time.time()
    cursor.execute("""SELECT passenger_count, avg(total_amount) FROM cabs3
        GROUP BY 1;
        """)
    # Выводим результаты
    pr = cursor.fetchall()
    e_time = time.time()
    times[1] += (e_time - s_time)


    #Выполняем запрос 3
    s_time = time.time()
    cursor.execute("""
        SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM cabs3
        GROUP BY 1, 2;
        """)
    # Выводим результаты
    pr = cursor.fetchall()
    e_time = time.time()
    times[2] += (e_time - s_time)


    #Выполняем запрос 4
    s_time = time.time()
    cursor.execute("""
        SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM cabs3
        GROUP BY 1, 2, 3
        ORDER BY 2, 4 desc;
    
        """)

    # Выводим результаты
    pr = cursor.fetchall()
    e_time = time.time()
    times[3] += (e_time - s_time)

for j in range(len(times)):
    times[j]=times[j]/10
print(times)
connection.commit()
connection.close()