import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main.management.commands.update_rows import job
import main.management.commands.update_rows as main_module

process_xml_with_yandex_disk = main_module.process_xml_with_yandex_disk

def run_job():
    """Запускает задачу обработки XML"""
    print("Запуск задачи обработки XML...")
    job()

def run_xml_processing():
    """Запускает только обработку XML с Яндекс.Диском"""
    print("Запуск обработки XML с Яндекс.Диском...")
    df, file_url = process_xml_with_yandex_disk()
    if df is not None:
        print(f"Обработка завершена успешно. Ссылка: {file_url}")
    else:
        print("Ошибка при обработке XML")

if __name__ == "__main__":
    run_job() 