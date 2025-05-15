import os
import time
import xml.etree.ElementTree as ET
import requests
from PIL import Image as PILImage
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse
from datetime import datetime
import schedule
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import openpyxl
from openpyxl.drawing.image import Image
from pathlib import Path
import uuid
import sys

# Конфигурация
XML_URL = "https://media.cm.expert/stock/export/cmexpert/avito.ru/all/all/d55d8ef7288a43a9824c95ca76ef7767.xml"
LOCAL_XML_PATH = "avito_cmexpert.xml"
OUTPUT_EXCEL_PATH = "avito_cmexpert_new.xlsx"
GOOGLE_CRED_PATH = "google_cred.json"
MAX_ITEMS = 99999 # Ограничиваем для демонстрации
IMAGES_FOLDER_NAME = "avito_images"  # Название папки для изображений на Google Drive

# Новый текст описания
NEW_DESCRIPTION = """</p><p><strong>Автозапчасти на Волнянского</strong> - более 10 000 в наличии + любые под заказ. Оригинальные и проверенные аналоги!</p>
<p>✔ У нас дешевле, чем в крупных интернет магазинах<br /> ✔ Гарантия до 3-х лет (срок зависит от вида и бренда запчасти)<br /> ✔ Быстрый и легкий возврат товара из наличия в любое время<br /> ✔ Дисконтная карта со скидкой 7% при покупке от 10 тыс. руб.<br /> ✔ Найдем запчасти даже без вин!<br /> <br /> <strong>📣Скидка 5%</strong> на товары в нашем магазине по адресу: г.Тула, ул. Волнянского, 1. (кроме представленных на Avito)</p>
<p>🚚<strong>Доставка по РФ</strong> через Авито: Почта России, СДЭК, Boxberry<br /> + Ежедневная отправка<br /> + Надежная упаковка (ничего не повредится)<br /> + Проверка при получении + гарантия</p>
<p>❗️❗️<strong>Не знаете артикул или какая запчасть точно нужна?</strong><br /> Присылайте фото или свой вопрос по запчасти, мы подберем нужную запчасть</p>
<p>📞Звоните или напишите нам в чат, чтобы уточнить по наличию запчасти в магазине. Если нужной детали нет, доставим в магазин за 2 часа (крупные детали до 2-х дн).</p>"""

# Пути к изображениям для наложения
OVERLAY_IMAGES = [
    "images/1.png",
    "images/2.png",
    "images/3.png",
    "images/4.png",
    "images/5.png",
    "images/6.png"
]

# Путь к изображению для наложения водяного знака
WATERMARK_PATH = "images/1.png"

# Пути к изображениям магазина
SHOP_IMAGES = [
    "shop/photo_1_2025-04-10_16-52-54.jpg",
    "shop/photo_3_2025-04-10_16-52-54.jpg",
    "shop/photo_7_2025-04-10_16-52-54.jpg"
]

def download_xml(max_retries=5, retry_delay=10):
    """
    Загрузка XML-файла с сервера с поддержкой повторных попыток
    
    max_retries: максимальное количество попыток
    retry_delay: задержка между попытками в секундах
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Добавляем случайный User-Agent, чтобы избежать блокировки
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive'
            }
            
            print(f"Попытка {attempt} из {max_retries} загрузить XML-файл...")
            response = requests.get(XML_URL, headers=headers, timeout=60)
            
            if response.status_code == 200:
                with open(LOCAL_XML_PATH, 'wb') as f:
                    f.write(response.content)
                print(f"XML-файл загружен: {LOCAL_XML_PATH}")
                return True
            elif response.status_code == 429:
                # Если сервер вернул 429, ждем дольше
                wait_time = retry_delay * attempt
                print(f"Ошибка 429 (Too Many Requests). Ожидание {wait_time} секунд перед повторной попыткой...")
                time.sleep(wait_time)
            else:
                print(f"Ошибка загрузки XML-файла. Код ответа: {response.status_code}")
                if attempt < max_retries:
                    print(f"Ожидание {retry_delay} секунд перед повторной попыткой...")
                    time.sleep(retry_delay)
                
        except Exception as e:
            print(f"Исключение при загрузке XML-файла: {e}")
            if attempt < max_retries:
                print(f"Ожидание {retry_delay} секунд перед повторной попыткой...")
                time.sleep(retry_delay)
    
    # Все попытки исчерпаны
    print("Все попытки загрузки XML исчерпаны.")
    
    # Проверяем, есть ли локальная копия XML-файла
    if os.path.exists(LOCAL_XML_PATH):
        print(f"Используем локальную копию файла: {LOCAL_XML_PATH}")
        return True
    
    return False

def create_output_dir():
    """Создание директории для обработанных изображений"""
    output_dir = "processed_images"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def overlay_image(base_image_url, overlay_path, output_path):
    """Наложение одного изображения на другое с сохранением соотношения сторон"""
    try:
        # Загрузка базового изображения
        response = requests.get(base_image_url)
        if response.status_code != 200:
            print(f"Ошибка загрузки изображения {base_image_url}, код: {response.status_code}")
            return None
            
        base_img = PILImage.open(BytesIO(response.content)).convert("RGBA")
        
        # Открытие изображения для наложения и конвертация в RGBA
        overlay_img = PILImage.open(overlay_path).convert("RGBA")
        
        # Получаем размеры базового изображения
        base_width, base_height = base_img.size
        
        # Изменяем размер наложения, сохраняя соотношение сторон
        overlay_width, overlay_height = overlay_img.size
        ratio = min(base_width / overlay_width, base_height / overlay_height)
        new_overlay_width = int(overlay_width * ratio)
        new_overlay_height = int(overlay_height * ratio)
        
        # Изменение размера наложения с сохранением соотношения сторон
        overlay_img = overlay_img.resize((new_overlay_width, new_overlay_height), PILImage.LANCZOS)
        
        # Вычисляем позицию для размещения наложения внизу изображения
        # Горизонтально центрируем, а вертикально смещаем вниз
        paste_x = (base_width - new_overlay_width) // 2
        
        # Минимальный отступ от нижнего края - ставим 0 для максимального опускания вниз
        bottom_margin = 0  # Отступ от нижнего края отсутствует
        paste_y = base_height - new_overlay_height - bottom_margin
        
        # Проверка, чтобы изображение не вышло за пределы
        if paste_y < 0:
            paste_y = 0
        
        # Создаем новое изображение с правильными каналами и прозрачностью
        result = PILImage.new("RGBA", base_img.size, (0, 0, 0, 0))
        result.paste(base_img, (0, 0))
        result.paste(overlay_img, (paste_x, paste_y), overlay_img)
        
        # Конвертация в RGB для сохранения в JPEG
        result = result.convert("RGB")
        
        # Сохранение результата
        result.save(output_path)
        return output_path
    except Exception as e:
        print(f"Ошибка при наложении изображения: {e}")
        import traceback
        traceback.print_exc()
        return None

def add_shop_image(base_image_url, shop_image_path, output_path):
    """Добавляет изображение магазина к первому изображению товара в виде коллажа"""
    try:
        # Загрузка базового изображения товара
        response = requests.get(base_image_url)
        if response.status_code != 200:
            print(f"Ошибка загрузки изображения {base_image_url}, код: {response.status_code}")
            return None
            
        base_img = PILImage.open(BytesIO(response.content)).convert("RGB")
        
        # Открытие изображения магазина
        shop_img = PILImage.open(shop_image_path).convert("RGB")
        
        # Получаем размеры базового изображения
        base_width, base_height = base_img.size
        
        # Создаем новое изображение-коллаж, достаточно широкое для двух изображений
        # Ширина = ширина базового изображения * 2 (с небольшим отступом)
        # Высота = высота базового изображения
        collage_width = base_width * 2 + 20  # 20 пикселей отступ между изображениями
        collage_height = base_height
        
        # Изменяем размер изображения магазина, чтобы оно соответствовало высоте базового изображения
        shop_width, shop_height = shop_img.size
        new_shop_height = base_height
        new_shop_width = int(shop_width * (new_shop_height / shop_height))
        shop_img = shop_img.resize((new_shop_width, new_shop_height), PILImage.LANCZOS)
        
        # Создаем коллаж (белый фон)
        collage = PILImage.new("RGB", (collage_width, collage_height), (255, 255, 255))
        
        # Размещаем базовое изображение слева
        collage.paste(base_img, (0, 0))
        
        # Размещаем изображение магазина справа
        collage.paste(shop_img, (base_width + 20, 0))
        
        # Сохраняем результат
        collage.save(output_path)
        return output_path
    except Exception as e:
        print(f"Ошибка при создании коллажа: {e}")
        import traceback
        traceback.print_exc()
        return None

def upload_image_to_gdrive(drive_service, file_path, max_retries=3, retry_delay=5):
    """
    Загружает изображение на Google Drive и возвращает публичную ссылку
    
    drive_service: Инициализированный сервис Google Drive API
    file_path: Путь к локальному файлу
    max_retries: Максимальное количество попыток загрузки
    retry_delay: Задержка между попытками в секундах
    
    Возвращает: публичную ссылку на изображение
    """
    print(f"Начинаю загрузку файла {file_path} на Google Drive")
    
    if drive_service is None:
        print("ОШИБКА: drive_service is None - сервис Google Drive не инициализирован")
        return None
        
    if not os.path.exists(file_path):
        print(f"ОШИБКА: Файл {file_path} не существует")
        return None
        
    for attempt in range(1, max_retries + 1):
        try:
            file_name = os.path.basename(file_path)
            
            # Проверим, существует ли папка для изображений
            folder_id = None
            try:
                print(f"Поиск папки {IMAGES_FOLDER_NAME} на Google Drive")
                response = drive_service.files().list(
                    q=f"name='{IMAGES_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
                    spaces='drive',
                    fields='files(id, name)'
                ).execute()
                
                if not response.get('files'):
                    # Создаем папку для изображений
                    print(f"Папка {IMAGES_FOLDER_NAME} не найдена, создаю новую")
                    folder_metadata = {
                        'name': IMAGES_FOLDER_NAME,
                        'mimeType': 'application/vnd.google-apps.folder'
                    }
                    folder = drive_service.files().create(
                        body=folder_metadata,
                        fields='id'
                    ).execute()
                    folder_id = folder.get('id')
                    print(f"Создана папка с ID: {folder_id}")
                    
                    # Устанавливаем доступ на редактирование для папки
                    drive_service.permissions().create(
                        fileId=folder_id,
                        body={
                            'type': 'anyone',
                            'role': 'writer',  # изменено с 'reader' на 'writer'
                        }
                    ).execute()
                    print("Права доступа к папке установлены")
                else:
                    folder_id = response.get('files')[0].get('id')
                    print(f"Найдена существующая папка с ID: {folder_id}")
            except Exception as e:
                print(f"Ошибка при работе с папкой на Google Drive: {e}")
                # Если не удалось получить/создать папку, загружаем файл в корень
                folder_id = None
            
            # Загружаем файл в папку или корень
            file_metadata = {
                'name': file_name
            }
            
            # Добавляем папку, если она создана/получена
            if folder_id:
                file_metadata['parents'] = [folder_id]
                print(f"Файл будет загружен в папку {folder_id}")
            else:
                print("Файл будет загружен в корневую папку")
            
            # Используем меньший таймаут для предотвращения зависаний
            print(f"Подготовка файла {file_path} для загрузки")
            media = MediaFileUpload(file_path, resumable=True, chunksize=1024*1024)
            print("Начало загрузки файла")
            file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            file_id = file.get('id')
            print(f"Файл загружен с ID: {file_id}")
            
            # Устанавливаем доступ на редактирование для файла
            print("Установка прав доступа для файла")
            drive_service.permissions().create(
                fileId=file_id,
                body={
                    'type': 'anyone',
                    'role': 'writer',  # изменено с 'reader' на 'writer'
                }
            ).execute()
            print("Права доступа установлены")
            
            # Получаем прямую ссылку для просмотра - это прямая ссылка на содержимое
            # Формат прямой ссылки для файлов на Google Drive
            direct_url = f"https://drive.google.com/uc?export=view&id={file_id}"
            print(f"Сгенерирована ссылка на файл: {direct_url}")
            
            # Если успешно - возвращаем ссылку и завершаем функцию
            return direct_url
            
        except Exception as e:
            print(f"Ошибка при загрузке изображения на Google Drive (попытка {attempt} из {max_retries}): {e}")
            import traceback
            traceback.print_exc()
            if attempt < max_retries:
                print(f"Повторная попытка через {retry_delay} секунд...")
                time.sleep(retry_delay)
    
    # Если все попытки неудачны, возвращаем None
    print(f"Не удалось загрузить изображение {file_path} на Google Drive после {max_retries} попыток.")
    return None

def process_images(ad_element, output_dir, ad_id, gdrive_service=None, shop_image_path=None):
    """Сбор исходных ссылок на изображения без обработки и загрузки на Google Drive"""
    print(f"Сбор ссылок на изображения для {ad_id}")
    
    # Попробуем получить изображения различными способами
    images = ad_element.findall(".//Image")
    
    if not images:
        # Попробуем другой способ поиска изображений
        images_section = ad_element.find("Images")
        if images_section is not None:
            images = images_section.findall("Image")
            if images:
                print(f"Найдены изображения через Images/Image: {len(images)}")
    
    if not images:
        print(f"Для {ad_id} не найдены изображения в XML")
        
        # Попробуем получить изображения напрямую из атрибутов url
        try:
            # Проверим, есть ли элемент Images и что в нем
            images_section = ad_element.find("Images")
            if images_section is not None:
                print(f"Содержимое секции Images для {ad_id}:")
                for child in images_section:
                    if 'url' in child.attrib:
                        # Создаем список URL из атрибутов
                        original_urls = [child.attrib['url'] for child in images_section if 'url' in child.attrib]
                        print(f"Найдены URL изображений через атрибуты: {original_urls}")
                        return original_urls
        except Exception as e:
            print(f"Ошибка при поиске изображений в атрибутах: {e}")
            import traceback
            traceback.print_exc()
        
        return []  # Нет изображений для обработки
    
    original_urls = []  # Список исходных URL изображений

    # Сбор всех URL изображений
    for i, img in enumerate(images):
        # Сначала проверяем текст элемента
        img_url = img.text
        
        # Если текст пустой, пробуем получить URL из атрибута
        if not img_url and 'url' in img.attrib:
            img_url = img.attrib['url']
            
        if img_url:
            original_urls.append(img_url)
    
    print(f"Найдено {len(original_urls)} изображений для {ad_id}")
    return original_urls

def process_image_urls(original_urls, output_dir, ad_id, gdrive_service=None, shop_image_path=None):
    """Обработка URL изображений для объявления"""
    if not original_urls:
        return []

    processed_urls = []  # Список URL обработанных изображений

    # Обработка изображений
    for i, img_url in enumerate(original_urls):
        if not img_url:
            continue

        # Первые 6 изображений обрабатываем и загружаем на Google Drive
        if i < 6:
            # Определение пути сохранения
            output_filename = f"{ad_id}_{i+1}.jpg"
            output_path = os.path.join(output_dir, output_filename)

            # Определяем, нужно ли использовать add_shop_image для первого изображения
            if i == 0 and shop_image_path and os.path.exists(shop_image_path):
                print(f"Добавление изображения магазина к первому изображению для объявления {ad_id}")
                result_path = add_shop_image(img_url, shop_image_path, output_path)
            else:
                # Выбираем подходящий оверлей в зависимости от порядкового номера изображения
                # Используем остаток от деления на длину списка, чтобы не выйти за границы
                overlay_index = i % len(OVERLAY_IMAGES)
                overlay_path = OVERLAY_IMAGES[overlay_index]
                print(f"Используем overlay {overlay_path} для изображения {i+1} объявления {ad_id}")
                
                result_path = overlay_image(img_url, overlay_path, output_path)
            
            if result_path:
                # Загрузка в Google Drive, если сервис предоставлен
                if gdrive_service:
                    try:
                        print(f"Начинаем загрузку изображения {output_filename} на Google Drive")
                        file_url = upload_image_to_gdrive(gdrive_service, result_path)
                        if file_url:
                            processed_urls.append(file_url)
                            print(f"Изображение {output_filename} загружено в Google Drive: {file_url}")
                        else:
                            print(f"Ошибка: не удалось получить URL для изображения {output_filename}")
                            # В случае ошибки загружаем локальный путь как запасной вариант
                            processed_urls.append(output_path)
                    except Exception as e:
                        print(f"Исключение при загрузке в Google Drive: {e}")
                        import traceback
                        traceback.print_exc()
                        # В случае исключения загружаем локальный путь
                        processed_urls.append(output_path)
                else:
                    # Если Google Drive не используется, сохраняем локальный путь
                    processed_urls.append(output_path)
                    print(f"Google Drive не используется, сохранен локальный путь: {output_path}")
        else:
            # Для изображений после 6-го сохраняем исходный URL без обработки
            processed_urls.append(img_url)
            print(f"Изображение {i+1} для объявления {ad_id} сохранено с исходным URL без обработки: {img_url}")
    
    # Добавляем изображения магазина, если осталось место (максимум 10 изображений)
    remaining_slots = 10 - len(processed_urls)
    if remaining_slots > 0 and SHOP_IMAGES:
        print(f"Осталось {remaining_slots} слотов для изображений магазина")
        
        # Добавляем столько изображений магазина, сколько поместится
        shop_images_to_add = min(remaining_slots, len(SHOP_IMAGES))
        print(f"Добавляем {shop_images_to_add} изображений магазина")
        
        for i in range(shop_images_to_add):
            shop_img_path = SHOP_IMAGES[i]
            shop_output_path = os.path.join(output_dir, f"{ad_id}_shop_{i+1}.jpg")
            
            try:
                # Копируем файлы магазина
                with open(shop_img_path, 'rb') as src, open(shop_output_path, 'wb') as dst:
                    dst.write(src.read())
                print(f"Скопировано изображение магазина {shop_img_path} -> {shop_output_path}")
                
                # Загружаем изображение магазина в Google Drive, если доступно
                if gdrive_service:
                    try:
                        shop_url = upload_image_to_gdrive(gdrive_service, shop_output_path)
                        if shop_url:
                            processed_urls.append(shop_url)
                            print(f"Изображение магазина загружено в Google Drive: {shop_url}")
                        else:
                            # Если не удалось загрузить, используем локальный путь
                            processed_urls.append(shop_output_path)
                            print(f"Не удалось загрузить изображение магазина в Google Drive, используем локальный путь")
                    except Exception as e:
                        print(f"Ошибка при загрузке изображения магазина в Google Drive: {e}")
                        processed_urls.append(shop_output_path)
                else:
                    processed_urls.append(shop_output_path)
                    print(f"Google Drive не используется, сохранен локальный путь для изображения магазина: {shop_output_path}")
            except Exception as e:
                print(f"Ошибка при копировании изображения магазина {shop_img_path}: {e}")
    
    print(f"Обработка изображений для {ad_id} завершена, результат: {processed_urls}")
    return processed_urls

def resize_image(image_path, max_size=160):
    """
    Изменяет размер изображения, сохраняя соотношение сторон, чтобы наибольшая сторона была max_size пикселей.
    """
    with PILImage.open(image_path) as img:
        ratio = min(max_size / img.width, max_size / img.height)
        new_size = (int(img.width * ratio), int(img.height * ratio))
        resized_img = img.resize(new_size, PILImage.LANCZOS)
        return resized_img

def save_to_excel(df, output_path=OUTPUT_EXCEL_PATH):
    """Сохранение DataFrame в Excel-файл"""
    
    # Проверяем, существует ли уже файл Excel
    if os.path.exists(output_path):
        # Загружаем существующие данные
        existing_data = pd.read_excel(output_path)
        
        # Создаем копию существующих данных
        merged_df = existing_data.copy()
        
        # Проверяем новые данные из df на отсутствие в существующей таблице по Id
        if 'Id' in df.columns and 'Id' in existing_data.columns:
            # Получаем список существующих Id
            existing_ids = set(existing_data['Id'].astype(str).tolist())
            
            # Фильтруем новые данные, оставляя только отсутствующие
            new_rows = df[~df['Id'].astype(str).isin(existing_ids)]
            
            # Если есть новые строки, добавляем их в конец существующей таблицы
            if len(new_rows) > 0:
                print(f"Добавление {len(new_rows)} новых строк к существующим {len(existing_data)}")
                
                # Добавляем новые строки в конец
                merged_df = pd.concat([existing_data, new_rows], ignore_index=True)
                
                # Сохраняем обновленную таблицу
                merged_df.to_excel(output_path, index=False)
                
                return output_path, True  # Файл был обновлен
            else:
                print("Нет новых строк для добавления")
                return output_path, False  # Файл не был обновлен
        else:
            print("Отсутствует столбец 'Id' в исходных данных или в новых данных")
            # Если нет Id в одном из DataFrame, просто добавляем новые строки в конец
            merged_df = pd.concat([existing_data, df], ignore_index=True)
            merged_df.to_excel(output_path, index=False)
            return output_path, True  # Файл был обновлен
    else:
        # Если файл не существует, создаем новый
        df.to_excel(output_path, index=False)
        print(f"Создан новый Excel-файл: {output_path}")
        return output_path, True  # Файл был создан

def upload_to_google_drive(file_path, force_update=True):
    """
    Загрузка файла на Google Drive и возврат ссылки на документ
    
    file_path: путь к файлу для загрузки
    force_update: если True, то существующий файл будет обновлен;
                  если False, то существующий файл не будет обновлен
    """
    try:
        # Аутентификация с помощью сервисного аккаунта
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CRED_PATH, 
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        # Создание сервиса Drive API
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Название файла в Google Drive
        file_name = os.path.basename(file_path)
        
        # Проверка, существует ли файл с таким именем
        response = drive_service.files().list(
            q=f"name='{file_name}' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        file_id = None
        
        if not response.get('files'):
            # Создание нового файла
            file_metadata = {
                'name': file_name,
                'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }
            media = MediaFileUpload(file_path, resumable=True)
            file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            file_id = file.get("id")
            print(f'Файл загружен на Google Drive, ID: {file_id}')
        else:
            # Файл уже существует
            file_id = response.get('files')[0].get('id')
            
            if force_update:
                # Обновляем существующий файл только если требуется обновление
                media = MediaFileUpload(file_path, resumable=True)
                file = drive_service.files().update(
                    fileId=file_id,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f'Файл обновлен на Google Drive, ID: {file_id}')
            else:
                print(f'Используется существующий файл на Google Drive, ID: {file_id}')
        
        # Установка доступа на редактирование для всех, у кого есть ссылка
        drive_service.permissions().create(
            fileId=file_id,
            body={
                'type': 'anyone',
                'role': 'writer',  # изменено с 'reader' на 'writer'
            }
        ).execute()
        print(f'Установлены права на редактирование для всех, у кого есть ссылка')
        
        # Формирование ссылки на документ
        file_url = f"https://docs.google.com/spreadsheets/d/{file_id}/edit?usp=sharing"
        
        return file_url
            
    except Exception as e:
        print(f"Ошибка при загрузке на Google Drive: {e}")
        import traceback
        traceback.print_exc()
        return None

def sync_excel_from_gdrive():
    """Скачивание актуальной версии Excel-файла с Google Drive перед обработкой"""
    try:
        # Проверка наличия учетных данных
        if not os.path.exists(GOOGLE_CRED_PATH):
            print("Файл с учетными данными Google API не найден")
            return False
            
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CRED_PATH, 
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Название файла в Google Drive
        file_name = os.path.basename(OUTPUT_EXCEL_PATH)
        
        # Поиск файла на Google Drive
        response = drive_service.files().list(
            q=f"name='{file_name}' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        if not response.get('files'):
            print(f"Файл {file_name} не найден на Google Drive")
            return False
            
        file_id = response.get('files')[0].get('id')
        
        # Скачивание файла
        request = drive_service.files().get_media(fileId=file_id)
        
        # Сохраняем текущий файл как резервную копию, если он существует
        if os.path.exists(OUTPUT_EXCEL_PATH):
            backup_path = f"{OUTPUT_EXCEL_PATH}.bak"
            try:
                os.rename(OUTPUT_EXCEL_PATH, backup_path)
                print(f"Создана резервная копия: {backup_path}")
            except Exception as e:
                print(f"Не удалось создать резервную копию: {e}")
        
        # Сохраняем файл с Google Drive
        with open(OUTPUT_EXCEL_PATH, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Скачивание {int(status.progress() * 100)}%")
                
        print(f"Файл {file_name} успешно скачан с Google Drive")
        return True
        
    except Exception as e:
        print(f"Ошибка при синхронизации с Google Drive: {e}")
        import traceback
        traceback.print_exc()
        return False

def update_brands_for_existing_products(xml_path, excel_path, product_ids):
    """
    Обновляет значения поля Brand для указанных товаров в Excel на основе данных из XML
    
    Args:
        xml_path (str): Путь к XML-файлу
        excel_path (str): Путь к Excel-файлу
        product_ids (list): Список ID товаров для обновления
        
    Returns:
        bool: True, если были сделаны изменения, иначе False
    """
    if not os.path.exists(xml_path) or not os.path.exists(excel_path):
        print(f"Файл XML ({xml_path}) или Excel ({excel_path}) не найден")
        return False
        
    try:
        # Загружаем данные из Excel
        df = pd.read_excel(excel_path)
        
        # Флаг для отслеживания изменений
        changes_made = False
        
        # Парсим XML для получения актуальных значений брендов
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Словарь для хранения актуальных значений брендов
        brands_from_xml = {}
        
        # Собираем данные из XML
        for ad in root.findall("Ad"):
            ad_id_elem = ad.find("Id")
            brand_elem = ad.find("Brand")
            
            # Проверяем наличие Id и Brand
            if ad_id_elem is not None and ad_id_elem.text is not None and brand_elem is not None and brand_elem.text is not None:
                ad_id = ad_id_elem.text
                brand = brand_elem.text
                
                # Сохраняем только для указанных ID
                if ad_id in product_ids:
                    brands_from_xml[ad_id] = brand
                    print(f"Из XML: товар {ad_id}, бренд: {brand}")
        
        # Обновляем значения в Excel
        for product_id in product_ids:
            if product_id in brands_from_xml:
                # Ищем соответствующую строку в DataFrame
                product_mask = df['Id'].astype(str) == product_id
                if any(product_mask):
                    # Получаем текущее значение бренда в Excel
                    excel_brand = df.loc[product_mask, 'Brand'].iloc[0]
                    xml_brand = brands_from_xml[product_id]
                    
                    # Если значения отличаются, обновляем
                    if excel_brand != xml_brand:
                        print(f"Обновление бренда для товара {product_id}: с '{excel_brand}' на '{xml_brand}'")
                        df.loc[product_mask, 'Brand'] = xml_brand
                        changes_made = True
                    else:
                        print(f"Бренд для товара {product_id} совпадает: '{excel_brand}'")
                else:
                    print(f"Товар {product_id} не найден в Excel")
        
        # Если были сделаны изменения, сохраняем обновленную таблицу
        if changes_made:
            df.to_excel(excel_path, index=False)
            print(f"Сохранена обновленная таблица с исправленными брендами")
            return True
        else:
            print("Нет необходимости в обновлении брендов")
            return False
            
    except Exception as e:
        print(f"Ошибка при обновлении брендов: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_xml(use_gdrive_for_images=True):
    """Обработка XML-файла и создание Excel-таблицы"""
    # Синхронизация с Google Drive только если файл уже существует
    if os.path.exists(OUTPUT_EXCEL_PATH):
        sync_excel_from_gdrive()
    
    # Создание директории для изображений
    output_dir = create_output_dir()
    
    # Инициализация Google Drive API, если нужно
    gdrive_service = None
    if use_gdrive_for_images and os.path.exists(GOOGLE_CRED_PATH):
        try:
            # Аутентификация с помощью сервисного аккаунта
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_CRED_PATH, 
                scopes=['https://www.googleapis.com/auth/drive']
            )
            
            # Создание сервиса Drive API
            gdrive_service = build('drive', 'v3', credentials=credentials)
            print("Google Drive API успешно инициализирован")
        except Exception as e:
            print(f"Ошибка при инициализации Google Drive API: {e}")
            import traceback
            traceback.print_exc()
    
    # Проверяем, существует ли уже файл Excel и обновляем бренды для проблемных товаров
    if os.path.exists(OUTPUT_EXCEL_PATH):
        # Список ID товаров с несоответствиями в поле Brand
        problematic_product_ids = ['bz143', 'bz149', 'bz150']
        
        # Вызываем функцию обновления брендов
        update_result = update_brands_for_existing_products(
            LOCAL_XML_PATH, 
            OUTPUT_EXCEL_PATH, 
            problematic_product_ids
        )
        
        # Если были сделаны изменения, загружаем обновленную таблицу на Google Drive
        if update_result:
            print("Обновление брендов: загрузка обновленного Excel на Google Drive")
            file_url = upload_to_google_drive(OUTPUT_EXCEL_PATH, force_update=True)
            print(f"Таблица с обновленными брендами загружена на Google Drive: {file_url}")
    
    # Проверяем, существует ли уже файл Excel с данными
    existing_ids = set()
    existing_products_with_missing_images = {}
    existing_data = None
    
    if os.path.exists(OUTPUT_EXCEL_PATH):
        try:
            # Загружаем существующие данные для определения ID товаров, которые уже обработаны
            existing_data = pd.read_excel(OUTPUT_EXCEL_PATH)
            print(f"Загружены существующие данные из {OUTPUT_EXCEL_PATH}, строк: {len(existing_data)}")
            
            if 'Id' in existing_data.columns:
                # Получаем список существующих Id
                existing_ids = set(existing_data['Id'].astype(str).tolist())
                print(f"Найдено {len(existing_ids)} существующих товаров")
        except Exception as e:
            print(f"Ошибка при чтении существующего Excel-файла: {e}")
    
    # Парсинг XML
    tree = ET.parse(LOCAL_XML_PATH)
    root = tree.getroot()
    
    # Получаем список ID товаров из XML
    xml_ids = set()
    for ad in root.findall("Ad"):
        ad_id_elem = ad.find("Id")
        if ad_id_elem is not None and ad_id_elem.text is not None:
            xml_ids.add(ad_id_elem.text)
    
    # Проверяем, какие товары из Excel отсутствуют в XML
    if existing_data is not None and len(existing_ids) > 0:
        removed_ids = existing_ids - xml_ids
        if removed_ids:
            print(f"Найдено {len(removed_ids)} товаров, которые были удалены из XML:")
            for removed_id in removed_ids:
                print(f"- {removed_id}")
            
            # Удаляем строки с отсутствующими товарами
            existing_data = existing_data[~existing_data['Id'].astype(str).isin(removed_ids)]
            print(f"Удалено {len(removed_ids)} товаров из Excel-таблицы")
            
            # Сохраняем обновленную таблицу
            existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
            print(f"Обновленная таблица сохранена в {OUTPUT_EXCEL_PATH}")
            
            # Загружаем обновленную таблицу на Google Drive
            file_url = upload_to_google_drive(OUTPUT_EXCEL_PATH, force_update=True)
            print(f"Обновленная таблица загружена на Google Drive")
    
    # Ищем и удаляем нежелательный текст в описаниях
    print("Ищем и удаляем нежелательный текст в описаниях...")
    unwanted_suffix = "</p><p>__________________________<br />Режим работы : 9.00-19.00<br />Отправляем всеми ТК СДЕК BOXBERRY Яндекс Почта России DPD Авито <br />Максимально упаковываем товар перед отправкой</p>"
    for ad in root.findall("Ad"):
        description = ad.find("Description")
        if description is not None and description.text:
            if description.text.endswith(unwanted_suffix):
                description.text = description.text[:-len(unwanted_suffix)]
                print(f"Удален нежелательный текст из описания товара {ad.find('Id').text if ad.find('Id') is not None else 'без ID'}")
            elif "</p><p>__________________________<br />" in description.text:
                # Находим начало нежелательного текста
                start_idx = description.text.find("</p><p>__________________________<br />")
                if start_idx != -1:
                    # Удаляем весь текст с этого места до конца
                    description.text = description.text[:start_idx] + "</p>"
                    print(f"Удален частичный нежелательный текст из описания товара {ad.find('Id').text if ad.find('Id') is not None else 'без ID'}")
    
    # Сбор всех возможных параметров из ВСЕХ объявлений XML (не только тех, которые будут обрабатываться)
    all_parameters = set()
    print("Сбор всех возможных параметров из объявлений...")
    for ad in root.findall("Ad"):
        for elem in ad:
            all_parameters.add(elem.tag)
    
    print(f"Найдено {len(all_parameters)} уникальных параметров в XML")
    
    # Список стандартных параметров, которые всегда должны быть
    standard_parameters = [
        "Id", "AdType", "Category", "Address", "ContactPhone", 
        "GoodsType", "ProductType", "SparePartType", "Title", 
        "Description", "Price", "Availability", "Condition", "Brand", "OEM",
        "TechnicSparePartType", "TransmissionSparePartType", "EngineSparePartType"
    ]
    
    # Добавляем стандартные параметры, которых может не быть в XML
    for param in standard_parameters:
        all_parameters.add(param)
    
    # Добавляем наши кастомные параметры
    all_parameters.add("InternetCalls")
    all_parameters.add("CallsDevices")
    all_parameters.add("ImageUrls")
    
    # Удаляем поле Images из параметров, так как оно не нужно в Excel
    if "Images" in all_parameters:
        all_parameters.remove("Images")
        print("Удалено поле Images из списка параметров")
    
    print(f"Итоговое количество параметров с учетом стандартных и кастомных: {len(all_parameters)}")
    
    # Данные для таблицы
    data = []
    
    # Счетчик обработанных товаров
    processed_count = 0
    skipped_count = 0
    
    # Обработка каждого объявления с ограничением
    for ad in root.findall("Ad"):
        ad_id_elem = ad.find("Id")
        
        # Проверяем наличие элемента Id
        if ad_id_elem is None or ad_id_elem.text is None:
            continue
            
        ad_id = ad_id_elem.text
        
        # Явная проверка и фиксация значения Brand для проблемных товаров
        if ad_id in ['bz143', 'bz149', 'bz150']:
            brand_elem = ad.find("Brand")
            if brand_elem is not None and brand_elem.text:
                print(f"Товар {ad_id} имеет бренд {brand_elem.text} в XML")
        
        # Пропускаем уже существующие товары
        if ad_id in existing_ids:
            skipped_count += 1
            print(f"Пропуск объявления {ad_id} (уже существует в таблице)")
            continue
            
        # Обрабатываем только с ограничением на количество
        if processed_count >= MAX_ITEMS:
            continue
            
        processed_count += 1
        print(f"Обработка объявления {ad_id} ({processed_count}/{MAX_ITEMS - skipped_count})")
        
        # Замена описания
        description = ad.find("Description")
        if description is not None and description.text:
            print(f"Обработка описания для {ad_id}")
            # Проверяем, содержит ли текст CDATA
            if "<![CDATA[" in description.text and "]]>" in description.text:
                # Извлекаем содержимое CDATA
                cdata_start = description.text.find("<![CDATA[") + 9
                cdata_end = description.text.rfind("]]>")
                cdata_content = description.text[cdata_start:cdata_end]
                
                # Ищем маркер "Lada;"
                lada_index = cdata_content.find("Lada;")
                if lada_index != -1:
                    print(f"Найден маркер 'Lada;' в позиции {lada_index}")
                    # Всегда вставляем описание сразу после "Lada;"
                    new_cdata_content = cdata_content[:lada_index + 5] + NEW_DESCRIPTION + cdata_content[lada_index + 5:]
                    description.text = f"<![CDATA[{new_cdata_content}]]>"
                    print("Описание успешно вставлено после 'Lada;'")
                else:
                    # Если нет "Lada;", ищем последний </p><p>
                    last_p_tag = cdata_content.rfind("</p><p>")
                    if last_p_tag != -1:
                        print(f"Найден тег </p><p> в позиции {last_p_tag}")
                        # Вставляем после последнего тега </p><p>
                        tag_end = last_p_tag + len("</p><p>")
                        new_cdata_content = cdata_content[:tag_end] + NEW_DESCRIPTION + cdata_content[tag_end:]
                        description.text = f"<![CDATA[{new_cdata_content}]]>"
                        print("Описание успешно вставлено после тега </p><p>")
                    else:
                        print("Не найдены ни 'Lada;', ни </p><p>. Добавление в конец.")
                        # Если нет тегов, вставляем в конец
                        description.text = f"<![CDATA[{cdata_content}{NEW_DESCRIPTION}]]>"
            else:
                print("Текст не содержит CDATA")
                # Если нет CDATA, просто добавляем описание в конец
                lada_index = description.text.find("Lada;")
                if lada_index != -1:
                    print(f"Найден маркер 'Lada;' в позиции {lada_index}")
                    # Вставляем описание сразу после "Lada;"
                    # Уже закодированное в исходном файле описание
                    description.text = description.text[:lada_index + 5] + NEW_DESCRIPTION + description.text[lada_index + 5:]
                    print("Описание успешно вставлено после 'Lada;'")
                else:
                    print("Добавление описания в конец")
                    # Если нет "Lada;", добавляем в конец
                    description.text = description.text + NEW_DESCRIPTION
        
        # Сбор исходных URL изображений (без обработки)
        original_image_urls = process_images(ad, output_dir, ad_id, gdrive_service)
        
        # Обработка изображений и получение URL обработанных изображений
        processed_image_urls = process_image_urls(original_image_urls, output_dir, ad_id, gdrive_service)
        
        # Формируем строку со всеми URL изображений, разделенными |
        image_urls_string = "|".join(processed_image_urls) if processed_image_urls else "|".join(original_image_urls)
        
        # Собираем данные для Excel
        row_data = {
            # Указываем пустое значение для всех возможных параметров
            param: "" for param in all_parameters
        }
        
        # Заполняем значения из объявления
        for elem in ad:
            if elem.tag in row_data and elem.text is not None:
                # Очищаем CDATA если есть
                if "<![CDATA[" in elem.text and "]]>" in elem.text:
                    cdata_start = elem.text.find("<![CDATA[") + 9
                    cdata_end = elem.text.rfind("]]>")
                    row_data[elem.tag] = elem.text[cdata_start:cdata_end]
                else:
                    row_data[elem.tag] = elem.text
        
        # Добавляем наши кастомные значения
        row_data["InternetCalls"] = "Да"
        row_data["CallsDevices"] = "6078268665"
        row_data["ImageUrls"] = image_urls_string
        
        data.append(row_data)
    
    # Сохраняем обновленный XML
    output_xml_path = "avito_processed.xml"
    tree.write(output_xml_path, encoding="utf-8", xml_declaration=True)
    print(f"Обработанный XML сохранен: {output_xml_path}")
    
    if not data:
        print("Нет новых товаров для добавления")
        
        # Возвращаем существующую ссылку если нет новых товаров
        file_url = None
        if os.path.exists(OUTPUT_EXCEL_PATH):
            # Проверяем, есть ли файл на Google Drive
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    GOOGLE_CRED_PATH, 
                    scopes=['https://www.googleapis.com/auth/drive']
                )
                drive_service = build('drive', 'v3', credentials=credentials)
                
                # Название файла в Google Drive
                file_name = os.path.basename(OUTPUT_EXCEL_PATH)
                
                # Проверка, существует ли файл с таким именем
                response = drive_service.files().list(
                    q=f"name='{file_name}' and trashed=false",
                    spaces='drive',
                    fields='files(id, name)'
                ).execute()
                
                if response.get('files'):
                    file_id = response.get('files')[0].get('id')
                    file_url = f"https://docs.google.com/spreadsheets/d/{file_id}/edit?usp=sharing"
            except Exception as e:
                print(f"Ошибка при получении ссылки на документ: {e}")
        
        if file_url:
            return pd.DataFrame(), file_url
        else:
            return pd.DataFrame(), None
    
    # Создаем DataFrame для предварительного просмотра
    df = pd.DataFrame(data)
    
    # Выводим информацию о созданном DataFrame
    print(f"Создан DataFrame с {len(df)} строками и {len(df.columns)} столбцами")
    print("Столбцы в DataFrame:")
    for i, col in enumerate(df.columns):
        print(f"{i+1}. {col}")
    
    # Проверяем, есть ли в DataFrame нужные столбцы
    for param in standard_parameters:
        if param not in df.columns:
            print(f"Внимание: Столбец '{param}' отсутствует в DataFrame! Добавляем его...")
            df[param] = ""  # Добавляем пустой столбец
    
    # Создаем Excel-файл
    excel_path, was_updated = save_to_excel(df)
    
    # Загружаем файл на Google Drive только если он был обновлен
    if was_updated:
        file_url = upload_to_google_drive(excel_path, force_update=True)
        print(f"Таблица обновлена и загружена на Google Drive")
    else:
        # Если файл не был обновлен, получаем существующую ссылку
        file_url = upload_to_google_drive(excel_path, force_update=False)
        print(f"Таблица не изменилась, используем существующую ссылку")
    
    return df, file_url

# Создаем алиас для запуска с Google Drive для изображений
def process_xml_with_gdrive():
    """Обработка XML-файла с загрузкой изображений на Google Drive"""
    return process_xml(use_gdrive_for_images=True)

def add_image(product_id, image_url, output_dir=None, gdrive_service=None):
    """
    Добавляет изображение к существующему товару в Excel-таблице
    
    Args:
        product_id (str): ID товара
        image_url (str): URL изображения для добавления
        output_dir (str, optional): Директория для сохранения обработанных изображений
        gdrive_service (object, optional): Сервис Google Drive для загрузки изображений
        
    Returns:
        bool: True, если изображение успешно добавлено, иначе False
    """
    if output_dir is None:
        output_dir = create_output_dir()
        
    try:
        # Загружаем существующую таблицу
        if not os.path.exists(OUTPUT_EXCEL_PATH):
            print(f"Файл {OUTPUT_EXCEL_PATH} не найден")
            return False
            
        existing_data = pd.read_excel(OUTPUT_EXCEL_PATH)
        
        # Ищем товар по ID
        product_mask = existing_data['Id'] == product_id
        if not any(product_mask):
            print(f"Товар с ID {product_id} не найден в таблице")
            return False
            
        product_index = existing_data.index[product_mask][0]
        
        # Получаем текущие URL изображений
        current_images = existing_data.at[product_index, 'ImageUrls']
        current_images = str(current_images) if pd.notna(current_images) else ""
        
        # Обрабатываем новое изображение
        try:
            # Создаем объект ad_element с изображением для передачи в process_image_urls
            image_urls = [image_url]
            
            # Обрабатываем изображения
            processed_images = process_image_urls(image_urls, output_dir, product_id, gdrive_service)
            
            if not processed_images:
                print(f"Не удалось обработать изображение {image_url}")
                return False
                
            # Получаем URL обработанного изображения
            processed_url = processed_images[0]
            
            # Если у товара еще нет изображений, просто добавляем новое
            if not current_images or current_images == "nan" or current_images.strip() == "":
                existing_data.at[product_index, 'ImageUrls'] = processed_url
            else:
                # Добавляем новое изображение к существующим
                image_list = current_images.split("|")
                
                # Проверяем, не дублируется ли URL (это может быть, если изображение уже было добавлено)
                if processed_url not in image_list:
                    image_list.append(processed_url)
                    existing_data.at[product_index, 'ImageUrls'] = "|".join(image_list)
                else:
                    print(f"Изображение {processed_url} уже существует для товара {product_id}")
            
            # Сохраняем обновленные данные обратно в Excel
            existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
            
            # Обновляем файл на Google Drive
            upload_to_google_drive(OUTPUT_EXCEL_PATH)
            
            print(f"Изображение успешно добавлено к товару {product_id}")
            return True
            
        except Exception as e:
            print(f"Ошибка при обработке изображения: {e}")
            return False
            
    except Exception as e:
        print(f"Ошибка при добавлении изображения к товару {product_id}: {e}")
        return False

def update_all_brands():
    """
    Обновляет все значения брендов в Excel-файле на основе данных из XML.
    Эту функцию можно запустить отдельно для синхронизации.
    """
    print(f"Запуск обновления всех брендов: {datetime.now()}")
    
    # Проверяем наличие файлов
    if not os.path.exists(LOCAL_XML_PATH) or not os.path.exists(OUTPUT_EXCEL_PATH):
        print(f"Файл XML ({LOCAL_XML_PATH}) или Excel ({OUTPUT_EXCEL_PATH}) не найден")
        return False
    
    try:
        # Скачиваем последнюю версию XML
        download_success = download_xml()
        if not download_success:
            print("Не удалось скачать XML, используем локальную копию")
        
        # Синхронизация с Google Drive
        sync_excel_from_gdrive()
        
        # Загружаем данные из Excel
        df = pd.read_excel(OUTPUT_EXCEL_PATH)
        
        # Флаг для отслеживания изменений
        changes_made = False
        
        # Парсим XML для получения актуальных значений брендов
        tree = ET.parse(LOCAL_XML_PATH)
        root = tree.getroot()
        
        # Словарь для хранения актуальных значений брендов
        brands_from_xml = {}
        
        # Собираем данные из XML
        print("Сбор данных о брендах из XML...")
        for ad in root.findall("Ad"):
            ad_id_elem = ad.find("Id")
            brand_elem = ad.find("Brand")
            
            # Проверяем наличие Id и Brand
            if ad_id_elem is not None and ad_id_elem.text is not None and brand_elem is not None and brand_elem.text is not None:
                ad_id = ad_id_elem.text
                brand = brand_elem.text
                
                # Сохраняем бренд
                brands_from_xml[ad_id] = brand
        
        print(f"Собрано {len(brands_from_xml)} брендов из XML")
        
        # Обновляем значения в Excel
        updated_count = 0
        for index, row in df.iterrows():
            product_id = str(row['Id'])
            
            if product_id in brands_from_xml:
                excel_brand = str(row['Brand']) if pd.notna(row['Brand']) else ""
                xml_brand = brands_from_xml[product_id]
                
                # Если значения отличаются, обновляем
                if excel_brand != xml_brand:
                    print(f"Обновление бренда для товара {product_id}: с '{excel_brand}' на '{xml_brand}'")
                    df.at[index, 'Brand'] = xml_brand
                    changes_made = True
                    updated_count += 1
        
        print(f"Всего обновлено брендов: {updated_count}")
        
        # Если были сделаны изменения, сохраняем обновленную таблицу
        if changes_made:
            df.to_excel(OUTPUT_EXCEL_PATH, index=False)
            print(f"Сохранена обновленная таблица с исправленными брендами")
            
            # Загружаем на Google Drive
            file_url = upload_to_google_drive(OUTPUT_EXCEL_PATH, force_update=True)
            print(f"Таблица с обновленными брендами загружена на Google Drive: {file_url}")
            return True
        else:
            print("Все бренды актуальны, обновление не требуется")
            return False
            
    except Exception as e:
        print(f"Ошибка при обновлении всех брендов: {e}")
        import traceback
        traceback.print_exc()
        return False

def job():
    """Основная функция для запуска процесса обработки"""
    print(f"Начало обработки: {datetime.now()}")
    
    # Сначала скачиваем актуальную версию Excel
    sync_excel_from_gdrive()
    
    if download_xml():
        # Обновляем бренды для известных проблемных товаров
        problematic_product_ids = ['bz143', 'bz149', 'bz150']
        update_brands_for_existing_products(LOCAL_XML_PATH, OUTPUT_EXCEL_PATH, problematic_product_ids)
        
        # Основная обработка
        df, file_url = process_xml_with_gdrive()
        print(f"Ссылка на обработанный документ: {file_url}")
    print(f"Обработка завершена: {datetime.now()}")

def check_gdrive_storage():
    """
    Проверяет свободное место на Google Диске и возвращает информацию о квоте хранилища
    
    Returns:
        dict: Словарь с информацией о хранилище или None в случае ошибки
    """
    try:
        # Проверка наличия учетных данных
        if not os.path.exists(GOOGLE_CRED_PATH):
            print("Файл с учетными данными Google API не найден")
            return None
            
        # Аутентификация с помощью сервисного аккаунта
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CRED_PATH, 
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        # Создание сервиса Drive API
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Получение информации о хранилище
        about = drive_service.about().get(fields='storageQuota').execute()
        
        # Извлечение данных о квоте хранилища
        storage_quota = about.get('storageQuota', {})
        
        # Преобразование байтов в более читаемый формат
        def format_size(size_bytes):
            if size_bytes is None:
                return "Неизвестно"
            
            # Преобразование строки в число, если это строка
            if isinstance(size_bytes, str):
                try:
                    size_bytes = int(size_bytes)
                except ValueError:
                    return size_bytes
            
            # Размеры в байтах
            for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
                if size_bytes < 1024.0 or unit == 'ТБ':
                    break
                size_bytes /= 1024.0
            return f"{size_bytes:.2f} {unit}"
        
        # Форматирование данных
        usage = storage_quota.get('usage')
        usage_in_drive = storage_quota.get('usageInDrive')
        usage_in_trash = storage_quota.get('usageInTrash')
        limit = storage_quota.get('limit')
        
        formatted_data = {
            'использовано_всего': format_size(usage),
            'использовано_на_диске': format_size(usage_in_drive),
            'использовано_в_корзине': format_size(usage_in_trash),
            'общий_лимит': format_size(limit),
        }
        
        # Вычисление свободного места, если есть лимит
        if limit is not None and usage is not None:
            try:
                limit_int = int(limit)
                usage_int = int(usage)
                free_space = limit_int - usage_int
                formatted_data['свободно'] = format_size(free_space)
                formatted_data['заполнено_процентов'] = f"{(usage_int / limit_int * 100):.2f}%"
            except (ValueError, ZeroDivisionError):
                formatted_data['свободно'] = "Не удалось рассчитать"
                formatted_data['заполнено_процентов'] = "Не удалось рассчитать"
        
        return formatted_data
        
    except Exception as e:
        print(f"Ошибка при получении информации о хранилище Google Drive: {e}")
        import traceback
        traceback.print_exc()
        return None

def display_gdrive_storage_info():
    """
    Отображает информацию о свободном месте на Google Диске в виде текстового отчета
    """
    storage_info = check_gdrive_storage()
    
    if storage_info is None:
        print("Не удалось получить информацию о хранилище Google Drive.")
        return
    
    print("\n====== Информация о хранилище Google Drive ======")
    print(f"Всего использовано: {storage_info.get('использовано_всего', 'Неизвестно')}")
    print(f"Использовано на диске: {storage_info.get('использовано_на_диске', 'Неизвестно')}")
    print(f"Использовано в корзине: {storage_info.get('использовано_в_корзине', 'Неизвестно')}")
    print(f"Общий лимит: {storage_info.get('общий_лимит', 'Неизвестно')}")
    print(f"Свободно: {storage_info.get('свободно', 'Неизвестно')}")
    print(f"Заполнено: {storage_info.get('заполнено_процентов', 'Неизвестно')}")
    print("================================================\n")

# Расширяем функцию main для возможности проверки хранилища через аргумент командной строки
def main():
    """Основная функция для запуска скрипта"""
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        if sys.argv[1] == "--update-brands":
            # Запускаем только обновление брендов
            print("Запуск только обновления брендов")
            update_all_brands()
            return
        elif sys.argv[1] == "--check-storage":
            # Проверка свободного места на Google Drive
            print("Проверка свободного места на Google Drive")
            display_gdrive_storage_info()
            return
    
    # Стандартный запуск
    # Сначала запускаем обработку однократно
    job()
    
    # Настраиваем регулярное выполнение задачи (каждый час)
    schedule.every(1).hours.do(job)
    
    # Бесконечный цикл для выполнения запланированных задач
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверка каждую минуту

if __name__ == "__main__":
    main()
