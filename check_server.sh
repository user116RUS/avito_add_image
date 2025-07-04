#!/bin/bash

# Проверяем, запущен ли Django сервер
if curl -s http://localhost:8000 > /dev/null 2>&1; then
    echo "✅ Django сервер запущен и доступен на порту 8000"
else
    echo "❌ Django сервер не запущен. Запускаем..."
    # Запускаем сервер в фоновом режиме
    nohup python manage.py runserver 8000 > server.log 2>&1 &
    
    # Ждем, пока сервер запустится
    echo "Ожидаем запуска сервера..."
    for i in {1..30}; do
        if curl -s http://localhost:8000 > /dev/null 2>&1; then
            echo "✅ Django сервер успешно запущен на порту 8000"
            exit 0
        fi
        sleep 1
    done
    
    echo "❌ Не удалось запустить Django сервер"
    exit 1
fi
