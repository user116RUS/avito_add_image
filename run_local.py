import os
from datetime import datetime
from main import process_xml, upload_to_google_drive

# Путь к локальному XML-файлу
LOCAL_XML_PATH = "avito-ipkuznetsov.xml"

def run_local_process():
    """Запуск процесса обработки с использованием локального XML-файла"""
    print(f"Начало обработки локального XML-файла: {datetime.now()}")
    
    # Проверяем наличие файла
    if not os.path.exists(LOCAL_XML_PATH):
        print(f"Ошибка: Локальный файл {LOCAL_XML_PATH} не найден!")
        return
    
    # Запускаем обработку с имеющимся файлом
    print(f"Используем существующий XML-файл: {LOCAL_XML_PATH}")
    df, file_url = process_xml()
    
    if file_url:
        print(f"Ссылка на Google-таблицу: {file_url}")
    else:
        print("Не удалось получить ссылку на Google-таблицу!")
    
    print(f"Обработка завершена: {datetime.now()}")

if __name__ == "__main__":
    run_local_process() 