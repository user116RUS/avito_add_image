# Настройка Google Service Account для Google Drive API

## Пошаговая инструкция

### 1. Создание проекта в Google Cloud Console

1. Перейдите в [Google Cloud Console](https://console.cloud.google.com/)
2. Создайте новый проект или выберите существующий
3. Запишите **Project ID** - он понадобится позже

### 2. Включение Google Drive API

1. В меню слева найдите "APIs & Services" → "Library"
2. Найдите "Google Drive API" и нажмите на него
3. Нажмите кнопку "Enable" для включения API

### 3. Создание Service Account

1. В меню слева перейдите в "APIs & Services" → "Credentials"
2. Нажмите "Create Credentials" → "Service Account"
3. Заполните:
   - **Service account name**: `avito-images-service`
   - **Service account ID**: (заполнится автоматически)
   - **Description**: `Service account for Avito images processing`
4. Нажмите "Create and Continue"

### 4. Настройка ролей (опционально)

1. В разделе "Grant this service account access to project" можете пропустить
2. Нажмите "Continue"
3. В разделе "Grant users access to this service account" также пропустите
4. Нажмите "Done"

### 5. Создание ключа

1. В списке Service Accounts найдите созданный аккаунт
2. Нажмите на него (или на иконку карандаша для редактирования)
3. Перейдите на вкладку "Keys"
4. Нажмите "Add Key" → "Create new key"
5. Выберите формат "JSON"
6. Нажмите "Create"
7. Файл автоматически скачается на ваш компьютер

### 6. Установка файла ключей

1. Переименуйте скачанный файл в `google_cred.json`
2. Поместите файл в корневую папку проекта (там где находится `main.py`)

### 7. Настройка доступа к Google Drive

Поскольку Service Account - это "виртуальный пользователь", у него нет доступа к вашему личному Google Drive. Есть два варианта:

#### Вариант A: Поделиться папкой с Service Account
1. Откройте файл `google_cred.json`
2. Найдите поле `client_email` (например: `avito-images-service@your-project.iam.gserviceaccount.com`)
3. В Google Drive создайте папку для изображений
4. Нажмите правой кнопкой на папку → "Поделиться"
5. Добавьте email из `client_email` как редактора
6. Скопируйте ID папки из URL (часть после `/folders/`)
7. В файле `main.py` установите `GOOGLE_DRIVE_FOLDER_ID = "ваш_folder_id"`

#### Вариант B: Использовать личный Google Drive (Domain-wide delegation)
Это более сложный вариант, требующий дополнительных настроек домена.

### 8. Проверка настройки

Запустите тестовый скрипт:

```bash
python test_service_account.py
```

Если все настроено правильно, вы увидите:
```
🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!
Service Account готов к использованию.
```

## Структура файла google_cred.json

Файл должен содержать следующие поля:

```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "service-account@project.iam.gserviceaccount.com",
  "client_id": "client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

## Преимущества Service Account над OAuth 2.0

1. **Нет необходимости в интерактивной аутентификации** - скрипт может работать автоматически
2. **Постоянный доступ** - не нужно обновлять токены
3. **Лучше для серверных приложений** - не привязан к конкретному пользователю
4. **Простота развертывания** - достаточно одного JSON файла

## Возможные проблемы и решения

### Ошибка "google_cred.json не найден"
- Убедитесь, что файл находится в корне проекта
- Проверьте правильность названия файла

### Ошибка "403 Forbidden"
- Убедитесь, что Google Drive API включен в проекте
- Проверьте, что Service Account имеет доступ к нужной папке

### Ошибка "400 Bad Request"
- Проверьте формат файла google_cred.json
- Убедитесь, что файл не поврежден

## Безопасность

- ⚠️ **НЕ ПУБЛИКУЙТЕ** файл `google_cred.json` в публичных репозиториях
- Добавьте `google_cred.json` в `.gitignore`
- Храните файл в безопасном месте
- При компрометации - удалите ключ в Google Cloud Console и создайте новый 