import os
from datetime import datetime
from main import download_xml, process_xml_with_gdrive

def run_with_gdrive():
    """Запуск обработки XML с загрузкой изображений на Google Drive"""
    print(f"Начало обработки с Google Drive: {datetime.now()}")
    # Проверяем, существует ли файл с учетными данными
    if not os.path.exists("google_cred.json"):
        print("Ошибка: Файл google_cred.json с учетными данными не найден.")
        return False
    
    # Загружаем XML-файл
    if download_xml():
        # Обрабатываем XML с загрузкой изображений на Google Drive
        _, file_url = process_xml_with_gdrive()
        print(f"Ссылка на обработанный документ: {file_url}")
        return True
    
    return False

if __name__ == "__main__":
    run_with_gdrive() 