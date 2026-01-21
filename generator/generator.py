import psycopg2
import random
import time
import os
from datetime import datetime

DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'water_quality')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

def generate_data():
    # pH: 6.5-8.5 (оптимальный диапазон для пресной воды)
    ph = round(random.uniform(6.5, 8.5), 2)
    # Растворенный кислород: 5-12 мг/л
    do = round(random.uniform(5.0, 12.0), 2)
    # Температура воды: 5-25°C (сезонные колебания)
    temp = round(random.uniform(5.0, 25.0), 2)
    # Мутность: 0-5 NTU (норма для чистой воды)
    turbidity = round(random.uniform(0.0, 5.0), 2)
    return ph, do, temp, turbidity

def insert_data(ph, do, temp, turbidity):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        query = """
        INSERT INTO water_quality_data 
        (ph, dissolved_oxygen, water_temperature, turbidity)
        VALUES (%s, %s, %s, %s)
        """
        cur.execute(query, (ph, do, temp, turbidity))
        conn.commit()
        cur.close()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Записано: pH={ph}, O₂={do} мг/л, "
              f"Темп={temp}°C, Мутность={turbidity} NTU")
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Запуск генератора данных мониторинга качества воды...")
    while True:
        insert_data(*generate_data())
        time.sleep(1)