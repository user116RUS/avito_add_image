import time
import schedule
from datetime import datetime
from main import download_xml, process_xml

def job():
    """Основная функция для запуска процесса обработки"""
    print(f"Начало обработки: {datetime.now()}")
    if download_xml():
        df, file_url = process_xml()
        print(f"Ссылка на обработанный документ: {file_url}")
    print(f"Обработка завершена: {datetime.now()}")

def main():
    """Основная функция для запуска скрипта с ежедневным обновлением"""
    # Сначала запускаем обработку однократно
    job()
    
    # Настраиваем регулярное выполнение задачи (каждый день в 3:00)
    schedule.every().day.at("03:00").do(job)
    
    print("Скрипт запущен в режиме ежедневного обновления в 03:00")
    
    # Бесконечный цикл для выполнения запланированных задач
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверка каждую минуту

if __name__ == "__main__":
    main() 