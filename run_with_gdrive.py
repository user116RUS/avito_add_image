import os
from datetime import datetime
import sys
from importlib.machinery import SourceFileLoader

# Загружаем модуль main.py как отдельный модуль
main_module = SourceFileLoader("main_module", "./main.py").load_module()
download_xml = main_module.download_xml
process_xml_with_gdrive = main_module.process_xml_with_gdrive

def run_with_gdrive():
    """Запуск обработки XML с загрузкой изображений на Google Drive"""
    print(f"Начало обработки с Google Drive: {datetime.now()}")
    # Проверяем, существует ли файл с учетными данными
    if not os.path.exists("google_cred.json"):
        print("Ошибка: Файл google_cred.json с учетными данными не найден.")
        return False
    
    # Удаляем старый файл Excel, если он существует, чтобы создать новый
    if os.path.exists(main_module.OUTPUT_EXCEL_PATH):
        try:
            os.remove(main_module.OUTPUT_EXCEL_PATH)
            print(f"Удален старый файл {main_module.OUTPUT_EXCEL_PATH} для создания нового")
        except Exception as e:
            print(f"Не удалось удалить старый файл: {e}")
    
    # Загружаем XML-файл
    if download_xml():
        # Обрабатываем XML с загрузкой изображений на Google Drive
        _, file_url = process_xml_with_gdrive()
        print(f"Ссылка на обработанный документ: {file_url}")
        return True
    
    return False

if __name__ == "__main__":
    run_with_gdrive() 