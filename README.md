# Система мониторинга качества воды

## Описание
End-to-end система для генерации, хранения и анализа данных о качестве воды в реке:
- Генерация реалистичных данных каждую секунду
- Хранение в PostgreSQL
- Визуализация через Redash
- Анализ в Jupyter Notebook (опционально)

## Поля данных
1. **pH** - кислотность воды (6.5-8.5)
2. **dissolved_oxygen** - уровень растворенного кислорода (мг/л)
3. **water_temperature** - температура воды (°C)
4. **turbidity** - мутность воды (NTU)

## Запуск системы
```bash
git clone https://github.com/brkdrd/ProjectProjectProject.git
cd water-monitoring
docker-compose up -d

## Использование редаш
Аддрес: http://localhost:5000/dashboard/monitoring-kachestva-vody
Логин: admin@example.com
Пароль: password