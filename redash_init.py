import time
import requests
import json
from datetime import datetime

REDASH_URL = "http://redash_server:5000"
INITIAL_WAIT = 45  # Секунд ожидания перед началом настройки
MAX_RETRIES = 20
RETRY_DELAY = 10  # Секунд между попытками

def wait_for_redash():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Ожидание готовности Redash...")
    time.sleep(INITIAL_WAIT)
    
    for i in range(MAX_RETRIES):
        try:
            response = requests.get(f"{REDASH_URL}/api/health", timeout=5)
            if response.status_code == 200:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Redash готов к настройке")
                return True
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Redash еще не готов. Статус: {response.status_code}. Попытка {i+1}/{MAX_RETRIES}")
        except requests.exceptions.RequestException as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка подключения к Redash: {e}. Попытка {i+1}/{MAX_RETRIES}")
        
        time.sleep(RETRY_DELAY)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Redash не стал готовым за отведенное время")
    return False

def create_admin_user():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Создание администратора...")
    try:
        # Сначала пробуем авторизоваться (если пользователь уже существует)
        auth_payload = {
            "email": "admin@example.com",
            "password": "password"
        }
        response = requests.post(f"{REDASH_URL}/api/session", json=auth_payload, timeout=10)
        
        if response.status_code == 200:
            api_key = response.json()['api_key']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Администратор уже существует, получен API ключ")
            return api_key
        
        # Создаем нового администратора
        user_payload = {
            "name": "admin",
            "email": "admin@example.com",
            "password": "password",
            "is_admin": True
        }
        response = requests.post(f"{REDASH_URL}/api/users", json=user_payload, timeout=10)
        
        if response.status_code == 200:
            api_key = response.json()['api_key']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Создан новый администратор")
            return api_key
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка создания администратора: {response.status_code}, {response.text}")
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка при создании администратора: {e}")
        return None

def create_data_source(api_key):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Создание источника данных...")
    headers = {"Authorization": f"Key {api_key}"}
    
    payload = {
        "name": "Water Quality Database",
        "type": "pg",
        "options": {
            "dbname": "water_quality",
            "host": "db",
            "port": 5432,
            "user": "user",
            "password": "password"
        }
    }
    
    try:
        response = requests.post(f"{REDASH_URL}/api/data_sources", 
                               json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data_source = response.json()
            data_source_id = data_source['id']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Источник данных создан, ID: {data_source_id}")
            return data_source_id
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка создания источника данных: {response.status_code}, {response.text}")
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка при создании источника данных: {e}")
        return None

def create_query(api_key, name, query, data_source_id):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Создание запроса: {name}")
    headers = {"Authorization": f"Key {api_key}"}
    
    payload = {
        "name": name,
        "query": query,
        "data_source_id": data_source_id,
        "schedule": {"interval": 300}
    }
    
    try:
        response = requests.post(f"{REDASH_URL}/api/queries", 
                               json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            query_id = response.json()['id']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Запрос '{name}' создан, ID: {query_id}")
            return query_id
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка создания запроса '{name}': {response.status_code}, {response.text}")
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка при создании запроса '{name}': {e}")
        return None

def create_visualization(api_key, query_id, name, description, vis_type, options):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Создание визуализации: {name}")
    headers = {"Authorization": f"Key {api_key}"}
    
    payload = {
        "name": name,
        "description": description,
        "type": vis_type,
        "query_id": query_id,
        "options": options
    }
    
    try:
        response = requests.post(f"{REDASH_URL}/api/visualizations", 
                               json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            vis_id = response.json()['id']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Визуализация '{name}' создана, ID: {vis_id}")
            return vis_id
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка создания визуализации '{name}': {response.status_code}, {response.text}")
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка при создании визуализации '{name}': {e}")
        return None

def create_dashboard(api_key, name):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Создание дашборда: {name}")
    headers = {"Authorization": f"Key {api_key}"}
    
    payload = {
        "name": name,
        "layout": "grid"
    }
    
    try:
        response = requests.post(f"{REDASH_URL}/api/dashboards", 
                               json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            dashboard_id = response.json()['id']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Дашборд '{name}' создан, ID: {dashboard_id}")
            return dashboard_id
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка создания дашборда '{name}': {response.status_code}, {response.text}")
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка при создании дашборда '{name}': {e}")
        return None

def add_widget_to_dashboard(api_key, dashboard_id, visualization_id, column, row, size_x=6, size_y=4):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Добавление виджета на дашборд (col={column}, row={row})")
    headers = {"Authorization": f"Key {api_key}"}
    
    payload = {
        "dashboard_id": dashboard_id,
        "visualization_id": visualization_id,
        "options": {
            "column": column,
            "row": row,
            "sizeX": size_x,
            "sizeY": size_y
        }
    }
    
    try:
        response = requests.post(f"{REDASH_URL}/api/widgets", 
                               json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            widget_id = response.json()['id']
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Виджет добавлен, ID: {widget_id}")
            return widget_id
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка добавления виджета: {response.status_code}, {response.text}")
            return None
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка при добавлении виджета: {e}")
        return None

def get_visualization_id(api_key, query_id):
    headers = {"Authorization": f"Key {api_key}"}
    try:
        response = requests.get(f"{REDASH_URL}/api/visualizations?query_id={query_id}", 
                              headers=headers, timeout=10)
        if response.status_code == 200:
            visualizations = response.json()
            if visualizations:
                return visualizations[0]['id']
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Не найдено визуализаций для запроса ID {query_id}")
        return None
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка получения визуализаций: {e}")
        return None

def wait_for_data(api_key, data_source_id):
    """Ожидание появления данных в базе перед созданием визуализаций"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Ожидание появления данных в базе...")
    headers = {"Authorization": f"Key {api_key}"}
    test_query = "SELECT COUNT(*) as count FROM water_quality_data"
    
    for i in range(15):
        try:
            payload = {
                "query": test_query,
                "data_source_id": data_source_id,
                "max_age": 0
            }
            response = requests.post(f"{REDASH_URL}/api/query_results", 
                                   json=payload, headers=headers, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                count = result['query_result']['data']['rows'][0]['count']
                if count > 0:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] В базе есть данные ({count} записей)")
                    return True
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Данные еще не появились, ожидание... Попытка {i+1}/15")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ошибка проверки данных: {e}")
        
        time.sleep(10)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Данные не появились за отведенное время, продолжаем настройку")
    return False

def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Начало автоматической настройки Redash")
    
    if not wait_for_redash():
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Невозможно продолжить настройку - Redash недоступен")
        return
    
    api_key = create_admin_user()
    if not api_key:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Не удалось создать/получить администратора")
        return
    
    data_source_id = create_data_source(api_key)
    if not data_source_id:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Не удалось создать источник данных")
        return
    
    # Ждем появления данных в базе
    wait_for_data(api_key, data_source_id)
    
    # Создаем запросы
    query1_id = create_query(api_key, "Динамика pH", 
        "SELECT date_trunc('minute', timestamp) AS minute, AVG(ph) AS avg_ph FROM water_quality_data GROUP BY minute ORDER BY minute DESC LIMIT 100",
        data_source_id)
    
    query2_id = create_query(api_key, "Корреляция температуры и кислорода",
        "SELECT water_temperature, dissolved_oxygen FROM water_quality_data ORDER BY timestamp DESC LIMIT 200",
        data_source_id)
    
    query3_id = create_query(api_key, "Статистика мутности",
        "SELECT turbidity FROM water_quality_data WHERE timestamp > NOW() - INTERVAL '1 hour'",
        data_source_id)
    
    if not all([query1_id, query2_id, query3_id]):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Не все запросы были созданы успешно")
        return
    
    # Создаем визуализации
    vis1_id = create_visualization(api_key, query1_id, "Динамика pH (линейный график)", 
        "Среднее значение pH по минутам", "CHART", {
            "globalSeriesType": "line",
            "sortX": True,
            "sortY": False,
            "xAxis": {"type": "datetime"},
            "yAxis": [{"type": "linear"}],
            "columnMapping": {"minute": "x", "avg_ph": "y"},
            "series": {"avg_ph": {"type": "line"}}
        })
    
    vis2_id = create_visualization(api_key, query2_id, "Зависимость кислорода от температуры",
        "Соотношение температуры воды и уровня кислорода", "CHART", {
            "globalSeriesType": "scatter",
            "sortX": True,
            "sortY": False,
            "xAxis": {"type": "linear", "name": "Температура (°C)"},
            "yAxis": [{"type": "linear", "name": "Кислород (мг/л)"}],
            "columnMapping": {"water_temperature": "x", "dissolved_oxygen": "y"},
            "series": {"dissolved_oxygen": {"type": "scatter"}}
        })
    
    vis3_id = create_visualization(api_key, query3_id, "Распределение мутности",
        "Гистограмма значений мутности за последний час", "CHART", {
            "globalSeriesType": "histogram",
            "sortX": False,
            "sortY": False,
            "xAxis": {"type": "linear", "name": "Мутность (NTU)"},
            "yAxis": [{"type": "linear", "name": "Частота"}],
            "columnMapping": {"turbidity": "x"},
            "series": {"turbidity": {"type": "histogram"}}
        })
    
    if not all([vis1_id, vis2_id, vis3_id]):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Не все визуализации были созданы успешно")
        return
    
    # Создаем дашборд
    dashboard_id = create_dashboard(api_key, "Мониторинг качества воды")
    if not dashboard_id:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Не удалось создать дашборд")
        return
    
    # Добавляем виджеты на дашборд
    add_widget_to_dashboard(api_key, dashboard_id, vis1_id, 0, 0)
    add_widget_to_dashboard(api_key, dashboard_id, vis2_id, 6, 0)
    add_widget_to_dashboard(api_key, dashboard_id, vis3_id, 0, 4)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Автоматическая настройка Redash завершена успешно!")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Дашборд доступен по адресу: http://localhost:5000/dashboard/monitoring-kachestva-vody")

if __name__ == "__main__":
    main()