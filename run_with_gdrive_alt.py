import os
import sys
import subprocess
from datetime import datetime

def run_with_gdrive():
    """Запуск обработки XML с загрузкой изображений на Google Drive"""
    print(f"Начало обработки с Google Drive: {datetime.now()}")
    
    # Проверяем, существует ли файл с учетными данными
    if not os.path.exists("google_cred.json"):
        print("Ошибка: Файл google_cred.json с учетными данными не найден.")
        return False
    
    # Выполняем main.py с нужными параметрами
    try:
        # Проверяем, что main.py существует
        if not os.path.exists("main.py"):
            print("Ошибка: Файл main.py не найден.")
            return False
            
        # Запускаем процесс
        print("Запуск основного скрипта...")
        result = subprocess.run([sys.executable, "main.py"], 
                               capture_output=True, 
                               text=True)
        
        # Выводим результат
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"Ошибка при выполнении main.py: {result.stderr}")
            return False
            
        return True
    except Exception as e:
        print(f"Произошла ошибка при запуске main.py: {e}")
        return False

if __name__ == "__main__":
    run_with_gdrive() 