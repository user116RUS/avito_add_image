import time
import schedule
from datetime import datetime
from importlib.machinery import SourceFileLoader

# Загружаем модуль main.py как отдельный модуль
main_module = SourceFileLoader("main_module", "main.py").load_module()
download_xml = main_module.download_xml
process_xml_with_gdrive = main_module.process_xml_with_gdrive

def job():
    """Основная функция для запуска процесса обработки"""
    print(f"Начало обработки: {datetime.now()}")
    
    if download_xml():
        df, file_url = process_xml_with_gdrive()
        print(f"Ссылка на обработанный документ: {file_url}")
    print(f"Обработка завершена: {datetime.now()}")

def main():
    """Основная функция демона"""
    print("Демон запущен...")
    
    # Запускаем задачу сразу при старте
    job()
    
    # Планируем выполнение каждый час
    schedule.every(1).hours.do(job)
    
    # Бесконечный цикл для выполнения запланированных задач
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main() 