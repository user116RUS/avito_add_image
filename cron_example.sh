#!/bin/bash

# Пример скрипта для cron
# Добавьте эту строку в crontab для запуска каждые 5 минут:
# */5 * * * * /path/to/your/project/cron_example.sh

# Переходим в директорию проекта
cd /Users/ramilnurgaleev/Dev/avito_add_image

# Активируем виртуальное окружение (если используется)
# source venv/bin/activate

# Запускаем Django команду
python manage.py update_rows

# Логирование с временной меткой
echo "$(date): update_rows command completed" >> /tmp/avito_cron.log 