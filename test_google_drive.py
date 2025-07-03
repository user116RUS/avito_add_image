#!/usr/bin/env python3
"""
Тестовый скрипт для проверки Google Drive соединения после исправления ошибок
"""

import os
import json
from main import (
    create_drive_service, 
    get_service_account_email, 
    create_or_find_folder,
    GOOGLE_CRED_PATH,
    IMAGES_FOLDER_NAME
)

def test_service_account():
    """Тестирует подключение сервисного аккаунта"""
    print("=" * 60)
    print("🧪 ТЕСТИРОВАНИЕ GOOGLE DRIVE СОЕДИНЕНИЯ")
    print("=" * 60)
    
    # 1. Проверка файла credentials
    print("\n1️⃣ Проверка файла credentials...")
    if not os.path.exists(GOOGLE_CRED_PATH):
        print(f"❌ Файл {GOOGLE_CRED_PATH} не найден!")
        print("💡 Поместите файл google_cred.json в корень проекта")
        return False
    
    try:
        with open(GOOGLE_CRED_PATH, 'r') as f:
            cred_data = json.load(f)
        print(f"✅ Файл {GOOGLE_CRED_PATH} найден")
        print(f"📧 Email сервисного аккаунта: {cred_data.get('client_email', 'неизвестно')}")
        print(f"🔗 Project ID: {cred_data.get('project_id', 'неизвестно')}")
    except Exception as e:
        print(f"❌ Ошибка при чтении {GOOGLE_CRED_PATH}: {e}")
        return False
    
    # 2. Инициализация сервиса
    print("\n2️⃣ Инициализация Google Drive сервиса...")
    try:
        drive_service = create_drive_service()
        if not drive_service:
            print("❌ Не удалось создать Google Drive сервис")
            return False
        print("✅ Google Drive сервис успешно инициализирован")
    except Exception as e:
        print(f"❌ Ошибка при инициализации сервиса: {e}")
        return False
    
    # 3. Тест базового API вызова
    print("\n3️⃣ Тестирование базового API вызова...")
    try:
        # Получаем информацию о корневой папке
        about = drive_service.about().get(fields='user').execute()
        user_info = about.get('user', {})
        print(f"✅ API вызов успешен")
        print(f"👤 Пользователь: {user_info.get('displayName', 'неизвестно')}")
        print(f"📧 Email: {user_info.get('emailAddress', 'неизвестно')}")
    except Exception as e:
        print(f"❌ Ошибка API вызова: {e}")
        if "403" in str(e):
            print("💡 Возможно, нужно включить Google Drive API в Google Cloud Console")
        return False
    
    # 4. Тест создания папки
    print(f"\n4️⃣ Тестирование создания папки '{IMAGES_FOLDER_NAME}'...")
    try:
        folder_id = create_or_find_folder(drive_service, IMAGES_FOLDER_NAME)
        if folder_id:
            print(f"✅ Папка '{IMAGES_FOLDER_NAME}' доступна (ID: {folder_id})")
        else:
            print(f"❌ Не удалось создать/найти папку '{IMAGES_FOLDER_NAME}'")
            return False
    except Exception as e:
        error_str = str(e)
        if "storageQuotaExceeded" in error_str:
            print("❌ Ошибка квоты хранения!")
            print("💡 РЕШЕНИЯ:")
            print("   1. Создайте папку в своем Google Drive")
            print("   2. Дайте доступ сервисному аккаунту к этой папке")
            print(f"   3. Добавьте email: {get_service_account_email()}")
            return False
        else:
            print(f"❌ Ошибка при создании папки: {e}")
            return False
    
    # 5. Тест создания тестового файла
    print("\n5️⃣ Тестирование создания файла...")
    try:
        # Создаем простой тестовый файл
        test_file_content = "Тестовый файл для проверки Google Drive"
        test_file_path = "test_file.txt"
        
        with open(test_file_path, 'w', encoding='utf-8') as f:
            f.write(test_file_content)
        
        # Загружаем в Google Drive
        from googleapiclient.http import MediaFileUpload
        
        file_metadata = {
            'name': 'test_file.txt',
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(test_file_path, mimetype='text/plain')
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        test_file_id = file.get('id')
        print(f"✅ Тестовый файл создан (ID: {test_file_id})")
        
        # Удаляем тестовый файл
        drive_service.files().delete(fileId=test_file_id).execute()
        os.remove(test_file_path)
        print("🗑️ Тестовый файл удален")
        
    except Exception as e:
        error_str = str(e)
        if "storageQuotaExceeded" in error_str:
            print("❌ Ошибка квоты хранения при создании файла!")
            print("💡 Сервисный аккаунт не может создавать файлы без доступа к папке")
            print(f"📧 Дайте доступ к папке для: {get_service_account_email()}")
            return False
        else:
            print(f"❌ Ошибка при создании тестового файла: {e}")
            return False
    
    print("\n" + "=" * 60)
    print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    print("✅ Google Drive готов к использованию")
    print("=" * 60)
    return True

if __name__ == "__main__":
    success = test_service_account()
    if not success:
        print("\n💡 РЕКОМЕНДАЦИИ:")
        print("1. Проверьте настройки сервисного аккаунта")
        print("2. Убедитесь что Google Drive API включен")
        print("3. Дайте доступ к папке сервисному аккаунту")
        print("4. Или установите GOOGLE_DRIVE_FOLDER_ID = None для автосоздания")
    exit(0 if success else 1) 