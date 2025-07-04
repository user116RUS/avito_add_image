#!/bin/bash

# Скрипт для запуска Django сервера, если он не запущен
PROJECT_DIR="/Users/ramilnurgaleev/Dev/avito_add_image"
PORT=8000
PIDFILE="$PROJECT_DIR/django_server.pid"

# Переходим в директорию проекта
cd "$PROJECT_DIR"

# Функция для проверки, запущен ли Django сервер
check_server() {
    curl -s http://localhost:$PORT > /dev/null 2>&1
    return $?
}

# Функция для запуска сервера
start_server() {
    echo "Запускаем Django сервер на порту $PORT..."
    nohup python manage.py runserver $PORT > django_server.log 2>&1 &
    SERVER_PID=$!
    echo $SERVER_PID > "$PIDFILE"
    echo "Django сервер запущен (PID: $SERVER_PID)"
    
    # Ждём немного, чтобы сервер успел запуститься
    sleep 3
    
    # Проверяем, что сервер действительно запустился
    if curl -s http://localhost:$PORT > /dev/null; then
        echo "✅ Django сервер успешно запущен и доступен"
        return 0
    else
        echo "❌ Ошибка: Django сервер не отвечает"
        return 1
    fi
}

# Функция для остановки сервера
stop_server() {
    if [ -f "$PIDFILE" ]; then
        PID=$(cat "$PIDFILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "Останавливаем Django сервер (PID: $PID)..."
            kill $PID
            rm -f "$PIDFILE"
            echo "Django сервер остановлен"
        else
            echo "Процесс не найден, удаляем PID файл"
            rm -f "$PIDFILE"
        fi
    else
        echo "PID файл не найден, сервер не запущен"
    fi
}

# Обработка аргументов командной строки
case "$1" in
    start)
        if ! check_server; then
            start_server
        fi
        ;;
    stop)
        stop_server
        ;;
    restart)
        stop_server
        sleep 2
        start_server
        ;;
    status)
        if check_server; then
            echo "Django сервер работает"
            exit 0
        else
            echo "Django сервер не запущен"
            exit 1
        fi
        ;;
    *)
        echo "Использование: $0 {start|stop|restart|status}"
        echo "Без аргументов - проверяет и запускает сервер при необходимости"
        
        # Если аргументы не переданы, просто проверяем и запускаем при необходимости
        if ! check_server; then
            start_server
        fi
        ;;
esac

# Проверяем, запущен ли сервер
if check_server; then
    echo "Django сервер уже запущен на порту $PORT"
else
    echo "Запускаем Django сервер..."
    # Запускаем сервер в фоновом режиме
    nohup python manage.py runserver $PORT > server.log 2>&1 &
    
    # Ждем, пока сервер запустится
    echo "Ожидаем запуска сервера..."
    for i in {1..30}; do
        if check_server; then
            echo "Django сервер успешно запущен на порту $PORT"
            exit 0
        fi
        sleep 1
    done
    
    echo "Не удалось запустить Django сервер"
    exit 1
fi 