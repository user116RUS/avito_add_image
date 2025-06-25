import os
import time
import xml.etree.ElementTree as ET
import requests
from PIL import Image as PILImage
from PIL import ImageEnhance, ImageFilter
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse
from datetime import datetime, timedelta
import schedule
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import openpyxl
from openpyxl.drawing.image import Image
from pathlib import Path
import uuid
import re
import json
import random
import numpy as np
from PIL.ExifTags import TAGS, GPSTAGS
import piexif
import shutil

# Конфигурация
XML_URL = "https://baz-on.ru/export/c4447/32a54/avito-ipkuznetsov.xml"
LOCAL_XML_PATH = "few_cities-7.xml"
OUTPUT_EXCEL_PATH = "few_cities_new.xlsx"
GOOGLE_CRED_PATH = "google_cred.json"
MAX_ITEMS = 99999999 # Убираем ограничение для продакшена
IMAGES_FOLDER_NAME = "cities_7"  # Название папки для изображений на Google Drive
GOOGLE_DRIVE_FOLDER_ID = '1oKQSNeMFPM2a0RpbOggjzmUktgfOQZ97'  # ID папки на Google Drive (если None, используется IMAGES_FOLDER_NAME)
SHOP_IMAGES_CACHE_FILE = "shop_images_cache.json"  # Файл для кэширования ссылок на изображения магазина

# Список городов для дублирования
CITY_LIST = [
    "Керчь",
    "Нижний Тагил", 
    "Коломна",
    "Михайлов",
    "Елец",
    "Новомосковск",
    "Липецк",
    "Пятигорск",
    "Киров",
    "Орск",
    "Пенза"
]

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
    "images/4.png"
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
        
        # Минимальный отступ от нижнего края - всего 0.5% высоты (уменьшено с 2%)
        bottom_margin = int(base_height * 0.005)  # 0.5% от высоты для минимального отступа снизу
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
            folder_id = GOOGLE_DRIVE_FOLDER_ID  # Используем ID папки, если он указан
            
            if folder_id is None:
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
            else:
                print(f"Используется указанная папка с ID: {folder_id}")
            
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
    """Обработка изображений для объявления"""
    print(f"Запуск process_images для {ad_id}, gdrive_service: {'Инициализирован' if gdrive_service else 'None'}")
    
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
            # Распечатаем содержимое элемента для отладки
            print(f"Содержимое элемента ad для {ad_id}:")
            for elem in ad_element:
                print(f"  - {elem.tag}: {elem.text if elem.text else 'None'}")
            
            # Проверим, есть ли элемент Images и что в нем
            images_section = ad_element.find("Images")
            if images_section is not None:
                print(f"Содержимое секции Images для {ad_id}:")
                for child in images_section:
                    print(f"  - {child.tag}: {child.text if child.text else 'None'}, атрибуты: {child.attrib}")
                    if 'url' in child.attrib:
                        # Создаем список URL из атрибутов
                        original_urls = [child.attrib['url'] for child in images_section if 'url' in child.attrib]
                        print(f"Найдены URL изображений через атрибуты: {original_urls}")
                        
                        # Обработка изображений по найденным URL
                        return process_image_urls(original_urls, output_dir, ad_id, gdrive_service, shop_image_path)
        except Exception as e:
            print(f"Ошибка при поиске изображений в атрибутах: {e}")
            import traceback
            traceback.print_exc()
        
        return []  # Нет изображений для обработки

    os.makedirs(output_dir, exist_ok=True)
    
    original_urls = []  # Список исходных URL изображений
    processed_urls = []  # Список URL обработанных изображений

    # Сбор всех URL изображений
    for i, img in enumerate(images):
        # Сначала проверяем текст элемента
        img_url = img.text
        
        # Если текст пустой, пробуем получить URL из атрибута
        if not img_url and 'url' in img.attrib:
            img_url = img.attrib['url']
            
        if img_url:
            original_urls.append(img_url)
    
    print(f"Найдено {len(original_urls)} изображений для {ad_id}: {original_urls}")
    
    return process_image_urls(original_urls, output_dir, ad_id, gdrive_service, shop_image_path)

def process_image_urls(original_urls, output_dir, ad_id, gdrive_service=None, shop_image_path=None):
    """Обработка URL изображений для объявления"""
    if not original_urls:
        return []

    processed_urls = []  # Список URL обработанных изображений

    # Обработка изображений
    for i, img_url in enumerate(original_urls):
        if not img_url:
            continue

        # Определение пути сохранения
        output_filename = f"{ad_id}_{i+1}.jpg"
        output_path = os.path.join(output_dir, output_filename)

        # Определяем, нужно ли использовать add_shop_image для первого изображения
        if i == 0 and shop_image_path and os.path.exists(shop_image_path):
            print(f"Добавление изображения магазина к первому изображению для объявления {ad_id}")
            result_path = add_shop_image(img_url, shop_image_path, output_path)
        elif i < 4:  # Накладываем водяной знак только на первые 4 изображения
            # Выбираем подходящий оверлей в зависимости от порядкового номера изображения
            # Используем остаток от деления на длину списка, чтобы не выйти за границы
            overlay_index = i % len(OVERLAY_IMAGES)
            overlay_path = OVERLAY_IMAGES[overlay_index]
            print(f"Используем overlay {overlay_path} для изображения {i+1} объявления {ad_id}")
            
            result_path = overlay_image(img_url, overlay_path, output_path)
        else:
            # Для остальных изображений просто сохраняем без водяного знака
            try:
                print(f"Сохраняем изображение {i+1} без водяного знака для объявления {ad_id}")
                response = requests.get(img_url)
                if response.status_code == 200:
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    result_path = output_path
                else:
                    print(f"Ошибка загрузки изображения {img_url}, код: {response.status_code}")
                    result_path = None
            except Exception as e:
                print(f"Ошибка при сохранении изображения без водяного знака: {e}")
                result_path = None
        
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
    
    # Изображения магазина теперь добавляются в основной функции process_xml,
    # поэтому здесь мы их не добавляем
    
    print(f"Обработка изображений для {ad_id} завершена, результат: {processed_urls}")
    return processed_urls

def save_to_excel(df, output_path=OUTPUT_EXCEL_PATH):
    """Сохранение DataFrame в Excel-файл с форматированием исходных строк"""
    
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
                
                # Сохраняем DataFrame во временный файл Excel
                temp_output = f"temp_{output_path}"
                merged_df.to_excel(temp_output, index=False)
                
                try:
                    # Открываем Excel-файл с помощью openpyxl для форматирования
                    wb = openpyxl.load_workbook(temp_output)
                    ws = wb.active
                    
                    # Заливаем желтым цветом строки с исходными товарами (без суффикса "-")
                    try:
                        # Пробуем создать стиль заливки стандартным способом
                        yellow_fill = openpyxl.styles.PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
                    except Exception as e:
                        print(f"Ошибка при создании стиля заливки: {e}")
                        # Альтернативный способ создания заливки
                        try:
                            from openpyxl.styles import Fill
                            yellow_fill = openpyxl.styles.PatternFill(patternType='solid', fgColor='FFFF00')
                        except Exception as e2:
                            print(f"Ошибка при создании альтернативного стиля заливки: {e2}")
                            yellow_fill = None
                    
                    # Продолжаем только если удалось создать стиль заливки
                    if yellow_fill:
                        # Находим индекс столбца с Id
                        id_col_index = None
                        for i, cell in enumerate(ws[1]):
                            if cell.value == 'Id':
                                id_col_index = i + 1  # openpyxl использует индексацию с 1
                                break
                        
                        if id_col_index:
                            # Проходим по всем строкам и заливаем исходные товары
                            row_count = 0
                            for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):  # Начинаем с 2, пропуская заголовок
                                cell = row[id_col_index - 1]  # Получаем ячейку с Id
                                try:
                                    if cell.value and "-" not in str(cell.value):  # Если это исходный товар (без суффикса "-")
                                        for cell in row:
                                            try:
                                                cell.fill = yellow_fill
                                                row_count += 1
                                            except Exception as cell_e:
                                                print(f"Ошибка при заливке ячейки: {cell_e}")
                                                # Альтернативный подход через прямую установку атрибута
                                                try:
                                                    cell._style.fill = yellow_fill
                                                except:
                                                    pass
                                except Exception as row_e:
                                    print(f"Ошибка при обработке строки {row_idx}: {row_e}")
                            
                            print(f"Залито желтым цветом {row_count} ячеек в оригинальных строках")
                    else:
                        print("Не удалось создать стиль заливки, форматирование не применено")
                    
                    # Сохраняем отформатированный файл
                    try:
                        wb.save(output_path)
                        print(f"Отформатированный Excel-файл сохранен: {output_path}")
                    except Exception as save_e:
                        print(f"Ошибка при сохранении отформатированного файла: {save_e}")
                        # Если не удалось сохранить отформатированный файл, используем оригинальный
                        merged_df.to_excel(output_path, index=False)
                        print(f"Сохранен неотформатированный Excel-файл: {output_path}")
                except Exception as format_e:
                    print(f"Ошибка при форматировании Excel-файла: {format_e}")
                    # Сохраняем без форматирования
                    merged_df.to_excel(output_path, index=False)
                    print(f"Сохранен неотформатированный Excel-файл: {output_path}")
                
                # Удаляем временный файл
                try:
                    os.remove(temp_output)
                except Exception as e:
                    print(f"Ошибка при удалении временного файла: {e}")
                
                return output_path, True  # Файл был обновлен
            else:
                print("Нет новых строк для добавления")
                return output_path, False  # Файл не был обновлен
        else:
            print("Отсутствует столбец 'Id' в исходных данных или в новых данных")
            # Если нет Id в одном из DataFrame, просто добавляем новые строки в конец
            
            # Сохраняем во временный файл
            temp_output = f"temp_{output_path}"
            merged_df.to_excel(temp_output, index=False)
            
            try:
                # Открываем Excel-файл с помощью openpyxl для форматирования
                wb = openpyxl.load_workbook(temp_output)
                ws = wb.active
                
                # Заливаем желтым цветом строки с исходными товарами (без суффикса "-")
                try:
                    # Пробуем создать стиль заливки стандартным способом
                    yellow_fill = openpyxl.styles.PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
                except Exception as e:
                    print(f"Ошибка при создании стиля заливки: {e}")
                    # Альтернативный способ создания заливки
                    try:
                        from openpyxl.styles import Fill
                        yellow_fill = openpyxl.styles.PatternFill(patternType='solid', fgColor='FFFF00')
                    except Exception as e2:
                        print(f"Ошибка при создании альтернативного стиля заливки: {e2}")
                        yellow_fill = None
                
                # Продолжаем только если удалось создать стиль заливки
                if yellow_fill:
                    # Находим индекс столбца с Id
                    id_col_index = None
                    for i, cell in enumerate(ws[1]):
                        if cell.value == 'Id':
                            id_col_index = i + 1
                            break
                    
                    if id_col_index:
                        # Проходим по всем строкам и заливаем исходные товары
                        row_count = 0
                        for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
                            cell = row[id_col_index - 1]
                            try:
                                if cell.value and "-" not in str(cell.value):
                                    for cell in row:
                                        try:
                                            cell.fill = yellow_fill
                                            row_count += 1
                                        except Exception as cell_e:
                                            print(f"Ошибка при заливке ячейки: {cell_e}")
                                            # Альтернативный подход
                                            try:
                                                cell._style.fill = yellow_fill
                                            except:
                                                pass
                            except Exception as row_e:
                                print(f"Ошибка при обработке строки {row_idx}: {row_e}")
                        
                        print(f"Залито желтым цветом {row_count} ячеек в оригинальных строках")
                else:
                    print("Не удалось создать стиль заливки, форматирование не применено")
                
                # Сохраняем отформатированный файл
                try:
                    wb.save(output_path)
                    print(f"Отформатированный Excel-файл сохранен: {output_path}")
                except Exception as save_e:
                    print(f"Ошибка при сохранении отформатированного файла: {save_e}")
                    # Если не удалось сохранить отформатированный файл, используем оригинальный
                    merged_df.to_excel(output_path, index=False)
                    print(f"Сохранен неотформатированный Excel-файл: {output_path}")
            except Exception as format_e:
                print(f"Ошибка при форматировании Excel-файла: {format_e}")
                # Сохраняем без форматирования
                merged_df.to_excel(output_path, index=False)
                print(f"Сохранен неотформатированный Excel-файл: {output_path}")
            
            # Удаляем временный файл
            try:
                os.remove(temp_output)
            except Exception as e:
                print(f"Ошибка при удалении временного файла: {e}")
            
            return output_path, True  # Файл был обновлен
    else:
        # Если файл не существует, создаем новый
        # Сохраняем во временный файл
        temp_output = f"temp_{output_path}"
        df.to_excel(temp_output, index=False)
        
        try:
            # Открываем Excel-файл с помощью openpyxl для форматирования
            wb = openpyxl.load_workbook(temp_output)
            ws = wb.active
            
            # Заливаем желтым цветом строки с исходными товарами (без суффикса "-")
            try:
                # Пробуем создать стиль заливки стандартным способом
                yellow_fill = openpyxl.styles.PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
            except Exception as e:
                print(f"Ошибка при создании стиля заливки: {e}")
                # Альтернативный способ создания заливки
                try:
                    from openpyxl.styles import Fill
                    yellow_fill = openpyxl.styles.PatternFill(patternType='solid', fgColor='FFFF00')
                except Exception as e2:
                    print(f"Ошибка при создании альтернативного стиля заливки: {e2}")
                    yellow_fill = None
            
            # Продолжаем только если удалось создать стиль заливки
            if yellow_fill:
                # Находим индекс столбца с Id
                id_col_index = None
                for i, cell in enumerate(ws[1]):
                    if cell.value == 'Id':
                        id_col_index = i + 1
                        break
                
                if id_col_index:
                    # Проходим по всем строкам и заливаем исходные товары
                    row_count = 0
                    for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
                        cell = row[id_col_index - 1]
                        try:
                            if cell.value and "-" not in str(cell.value):
                                for cell in row:
                                    try:
                                        cell.fill = yellow_fill
                                        row_count += 1
                                    except Exception as cell_e:
                                        print(f"Ошибка при заливке ячейки: {cell_e}")
                                        # Альтернативный подход
                                        try:
                                            cell._style.fill = yellow_fill
                                        except:
                                            pass
                        except Exception as row_e:
                            print(f"Ошибка при обработке строки {row_idx}: {row_e}")
                    
                    print(f"Залито желтым цветом {row_count} ячеек в оригинальных строках")
            else:
                print("Не удалось создать стиль заливки, форматирование не применено")
            
            # Сохраняем отформатированный файл
            try:
                wb.save(output_path)
                print(f"Отформатированный Excel-файл сохранен: {output_path}")
            except Exception as save_e:
                print(f"Ошибка при сохранении отформатированного файла: {save_e}")
                # Если не удалось сохранить отформатированный файл, используем оригинальный
                df.to_excel(output_path, index=False)
                print(f"Сохранен неотформатированный Excel-файл: {output_path}")
        except Exception as format_e:
            print(f"Ошибка при форматировании Excel-файла: {format_e}")
            # Сохраняем без форматирования
            df.to_excel(output_path, index=False)
            print(f"Сохранен неотформатированный Excel-файл: {output_path}")
        
        # Удаляем временный файл
        try:
            os.remove(temp_output)
        except Exception as e:
            print(f"Ошибка при удалении временного файла: {e}")
        
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
        
        # Проверяем доступ к папке, если указан ID
        folder_id = None
        if GOOGLE_DRIVE_FOLDER_ID:
            if check_folder_access(drive_service, GOOGLE_DRIVE_FOLDER_ID):
                folder_id = GOOGLE_DRIVE_FOLDER_ID
                print(f"Excel-файл будет сохранен в папке с ID: {folder_id}")
        
        # Создаем запрос для поиска файла
        query = f"name='{file_name}' and trashed=false"
        
        # Если у нас есть folder_id, ищем файл только в этой папке
        if folder_id:
            query += f" and '{folder_id}' in parents"
            
        # Проверка, существует ли файл с таким именем
        response = drive_service.files().list(
            q=query,
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
            
            # Если у нас есть folder_id, добавляем его в метаданные
            if folder_id:
                file_metadata['parents'] = [folder_id]
                
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
    """Скачивание актуальной версии Excel-файла с Google Drive и объединение с локальными изменениями"""
    try:
        # Проверка наличия учетных данных
        if not os.path.exists(GOOGLE_CRED_PATH):
            print("❌ Файл с учетными данными Google API не найден")
            return False
            
        # Сохраняем локальные данные перед скачиванием, если файл существует
        local_data = None
        if os.path.exists(OUTPUT_EXCEL_PATH):
            try:
                local_data = pd.read_excel(OUTPUT_EXCEL_PATH)
                print(f"💾 Сохранены локальные данные: {len(local_data)} строк")
            except Exception as e:
                print(f"⚠️ Ошибка при чтении локального файла: {e}")
        
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CRED_PATH, 
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Название файла в Google Drive
        file_name = os.path.basename(OUTPUT_EXCEL_PATH)
        
        # Создаем запрос для поиска файла
        query = f"name='{file_name}' and trashed=false"
        
        # Если у нас есть GOOGLE_DRIVE_FOLDER_ID, ищем файл только в этой папке
        if GOOGLE_DRIVE_FOLDER_ID:
            # Проверяем доступ к папке
            if check_folder_access(drive_service, GOOGLE_DRIVE_FOLDER_ID):
                query += f" and '{GOOGLE_DRIVE_FOLDER_ID}' in parents"
                print(f"🔍 Поиск Excel-файла в папке с ID: {GOOGLE_DRIVE_FOLDER_ID}")
        
        # Поиск файла на Google Drive
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, parents)'
        ).execute()
        
        if not response.get('files'):
            print(f"📁 Файл {file_name} не найден на Google Drive")
            # Если файла нет на Google Drive, используем локальные данные
            return local_data is not None
            
            
        file_id = response.get('files')[0].get('id')
        
        # Выводим информацию о родительской папке
        parents = response.get('files')[0].get('parents', [])
        if parents:
            for parent_id in parents:
                try:
                    parent = drive_service.files().get(fileId=parent_id, fields='id, name').execute()
                    print(f"📂 Файл находится в папке: {parent.get('name')} (ID: {parent_id})")
                except Exception as e:
                    print(f"⚠️ Не удалось получить информацию о родительской папке: {e}")
        
        # Создаем резервную копию локального файла перед скачиванием
        if os.path.exists(OUTPUT_EXCEL_PATH):
            backup_path = f"{OUTPUT_EXCEL_PATH}.local_backup"
            try:
                shutil.copy2(OUTPUT_EXCEL_PATH, backup_path)
                print(f"💾 Создана резервная копия локального файла: {backup_path}")
            except Exception as e:
                print(f"⚠️ Не удалось создать резервную копию локального файла: {e}")
        
        # Скачивание файла с Google Drive во временный файл
        temp_gdrive_path = f"{OUTPUT_EXCEL_PATH}.gdrive_temp"
        request = drive_service.files().get_media(fileId=file_id)
        
        print("⬇️ Скачивание файла с Google Drive...")
        with open(temp_gdrive_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"📊 Скачивание {int(status.progress() * 100)}%")
                
        print(f"✅ Файл {file_name} успешно скачан с Google Drive")
        
        # Загружаем данные из скачанного файла
        gdrive_data = None
        try:
            gdrive_data = pd.read_excel(temp_gdrive_path)
            print(f"📊 Загружены данные из Google Drive: {len(gdrive_data)} строк")
        except Exception as e:
            print(f"❌ Ошибка при чтении файла с Google Drive: {e}")
            # Удаляем временный файл
            if os.path.exists(temp_gdrive_path):
                os.remove(temp_gdrive_path)
            return False
        
        # Объединяем локальные данные с данными из Google Drive
        merged_data = merge_with_gdrive_changes(local_data, gdrive_data)
        
        # Сохраняем объединенные данные в основной файл
        if merged_data is not None:
            merged_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
            print(f"💾 Сохранены объединенные данные в {OUTPUT_EXCEL_PATH}: {len(merged_data)} строк")
        
        # Удаляем временный файл
        if os.path.exists(temp_gdrive_path):
            os.remove(temp_gdrive_path)
            
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при синхронизации с Google Drive: {e}")
        import traceback
        traceback.print_exc()
        return False

def clean_uniqualized_images_folder():
    """Очищает папку uniqualized_images после загрузки всех изображений"""
    unique_images_dir = "uniqualized_images"
    
    if os.path.exists(unique_images_dir):
        try:
            # Получаем список всех файлов в папке
            files = os.listdir(unique_images_dir)
            deleted_count = 0
            
            for file in files:
                file_path = os.path.join(unique_images_dir, file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        deleted_count += 1
                except Exception as e:
                    print(f"Ошибка при удалении файла {file_path}: {e}")
            
            # Пытаемся удалить саму папку, если она пустая
            try:
                os.rmdir(unique_images_dir)
                print(f"Папка {unique_images_dir} успешно очищена и удалена. Удалено файлов: {deleted_count}")
            except OSError:
                print(f"Папка {unique_images_dir} очищена (удалено файлов: {deleted_count}), но не удалена (возможно, не пустая)")
                
        except Exception as e:
            print(f"Ошибка при очистке папки {unique_images_dir}: {e}")
    else:
        print(f"Папка {unique_images_dir} не существует, очистка не требуется")

def clean_processed_images_folder():
    """Очищает папку processed_images после загрузки всех изображений на Google Drive"""
    processed_images_dir = "processed_images"
    
    if os.path.exists(processed_images_dir):
        try:
            # Получаем список всех файлов в папке
            files = os.listdir(processed_images_dir)
            deleted_count = 0
            
            for file in files:
                file_path = os.path.join(processed_images_dir, file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        deleted_count += 1
                except Exception as e:
                    print(f"Ошибка при удалении файла {file_path}: {e}")
            
            # Пытаемся удалить саму папку, если она пустая
            try:
                os.rmdir(processed_images_dir)
                print(f"Папка {processed_images_dir} успешно очищена и удалена. Удалено файлов: {deleted_count}")
            except OSError:
                print(f"Папка {processed_images_dir} очищена (удалено файлов: {deleted_count}), но не удалена (возможно, не пустая)")
                
        except Exception as e:
            print(f"Ошибка при очистке папки {processed_images_dir}: {e}")
    else:
        print(f"Папка {processed_images_dir} не существует, очистка не требуется")

def duplicate_rows(data_frame):
    """
    Создает 7 дублей для каждой строки с изменением ID и адреса
    
    data_frame: DataFrame с исходными данными
    
    Возвращает: DataFrame с исходными строками и их дублями
    """
    if data_frame.empty:
        return data_frame
        
    print(f"Создание дублей для {len(data_frame)} строк")
    
    # Ключевые слова, при наличии которых товар не размножается на другие города
    exclude_keywords = ['резонатор', 'глушитель', 'приемные трубы']
    
    # Список для хранения всех строк (исходных и дублей)
    all_rows = []
    
    # Инициализируем Google Drive API для загрузки уникализированных изображений
    gdrive_service = None
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_CRED_PATH, 
            scopes=['https://www.googleapis.com/auth/drive']
        )
        gdrive_service = build('drive', 'v3', credentials=credentials)
        print("Google Drive API инициализирован для загрузки уникализированных изображений.")
    except Exception as e:
        print(f"Ошибка при инициализации Google Drive API: {e}")
        print("Уникализированные изображения будут сохранены локально.")
    
    # Создаем директорию для уникализированных изображений
    unique_images_dir = "uniqualized_images"
    os.makedirs(unique_images_dir, exist_ok=True)
    
    # Загружаем ссылки на изображения магазина из кэша
    shop_image_urls = load_shop_images_cache()
    if shop_image_urls is None:
        # Если кэш не найден, создаем пустой список
        shop_image_urls = []
        print("Не удалось загрузить ссылки на изображения магазина из кэша, используется пустой список")
    else:
        print(f"Загружено {len(shop_image_urls)} ссылок на изображения магазина из кэша")
    
    # Для каждой строки в исходном DataFrame
    for _, row in data_frame.iterrows():
        # Добавляем исходную строку (оригинал без изменений)
        original_row = row.to_dict()
        
        # Устанавливаем значение Delivery для оригинальной строки
        # Если это товар в Туле по адресу "Тула, улица Волнянского, 1", то ПВЗ, иначе пустое значение
        if original_row.get('Address') == 'Тула, улица Волнянского, 1':
            original_row['Delivery'] = 'ПВЗ'
        else:
            original_row['Delivery'] = ''
        
        all_rows.append(original_row)
        
        # Проверяем название товара на наличие ключевых слов
        title = row.get('Title', '')
        if title and pd.notna(title):
            title_lower = title.lower()
            should_skip_duplication = any(keyword in title_lower for keyword in exclude_keywords)
            
            if should_skip_duplication:
                print(f"Товар '{title}' (ID: {row['Id']}) содержит ключевое слово - пропускаем размножение на города")
                continue
        
        # Получаем исходный ID
        original_id = row['Id']
        
        # Получаем список URL изображений, если они есть
        original_image_urls = []
        if 'ImageUrls' in row and row['ImageUrls'] and pd.notna(row['ImageUrls']):
            original_image_urls = row['ImageUrls'].split('|')
        
        # Создаем 7 дублей с изменениями
        for i in range(1, 8):
            # Создаем копию строки
            duplicate = row.to_dict()
            
            # Изменяем ID (добавляем -1, -2, и т.д.)
            duplicate['Id'] = f"{original_id}-{i}"
            
            # Изменяем адрес на город из списка
            city_index = (i - 1) % len(CITY_LIST)  # Используем остаток от деления, чтобы не выйти за границы списка
            city = CITY_LIST[city_index]
            duplicate['Address'] = city
            
            # Устанавливаем значение Delivery для копий - всегда "Выключена"
            duplicate['Delivery'] = 'Выключена'
            
            # Модифицируем описание, заменяя блок с доставкой
            if 'Description' in duplicate and duplicate['Description']:
                description = duplicate['Description']
                
                # Различные варианты начала текста с доставкой
                delivery_variations = [
                    "<p><strong>Автозапчасти на Волнянского</strong>",
                    "<p><strong>Автозапчасти на Волнянского </strong>",
                    '<p><strong>Автозапчасти на Волнянского</strong>'
                ]
                
                # Различные варианты текста с артикулом
                article_variations = [
                    "<p>📞Звоните или напишите нам в чат",
                    "<p>📞Звоните или напишите нам в чат",
                    '<p>📞Звоните или напишите нам в чат'
                ]
                
                # Текст для поиска (полные блоки)
                old_text_1 = "<p><strong>Автозапчасти на Волнянского</strong> - более 10 000 в наличии + любые под заказ. Оригинальные и проверенные аналоги!</p>"
                old_text_2 = "<p>📞Звоните или напишите нам в чат, чтобы уточнить по наличию запчасти в магазине. Если нужной детали нет, доставим в магазин за 2 часа (крупные детали до 2-х дн).</p>"
                
                # Новый текст с указанием города
                new_text = f"""<p>🚚<strong> Доставка в {city}</strong> через Авито: Почта России, СДЭК, Boxberry<br /> + Ежедневная отправка<br /> + Надежная упаковка (ничего не повредится)<br /> + Проверка при получении + гарантия</p><p>❗️Напишите в чат перед оформлением заказа❗️</p><p><strong>Автозапчасти на Волнянского</strong> - более 10 000 в наличии + любые под заказ. Оригинальные и проверенные аналоги!</p>
<p>✔ У нас дешевле, чем в крупных интернет магазинах<br /> ✔ Гарантия до 3-х лет (срок зависит от вида и бренда запчасти)<br /> ✔ Быстрый и легкий возврат товара из наличия в любое время<br /> ✔ Дисконтная карта со скидкой 7% при покупке от 10 тыс. руб.<br /> ✔ Найдем запчасти даже без вин!<br /> <br /> <strong>📣Скидка 5%</strong> на товары в нашем магазине по адресу: г.Тула, ул. Волнянского, 1. (кроме представленных на Avito)</p>
<p>📍В наличии на складе в г. Тула, улица Волнянского, 1</p>
<p>📞Звоните или напишите нам в чат, чтобы уточнить по наличию запчасти в магазине. Если нужной детали нет, доставим в магазин за 2 часа (крупные детали до 2-х дн).</p>"""
                
                # Метод 1: Попытка заменить полные блоки
                if old_text_1 in description and old_text_2 in description:
                    # Находим начало первого блока и конец второго
                    start_idx = description.find(old_text_1)
                    end_idx = description.find(old_text_2) + len(old_text_2)
                    
                    # Проверяем, что индексы найдены
                    if start_idx != -1 and end_idx != -1:
                        # Заменяем весь блок от начала первого до конца второго
                        new_description = description[:start_idx] + new_text + description[end_idx:]
                        duplicate['Description'] = new_description
                        print(f"Метод 1: Заменен текст в описании для товара {duplicate['Id']} с городом {city}")
                        
                # Метод 2: Поиск по вариациям начала блоков
                else:
                    start_idx = -1
                    end_idx = -1
                    
                    # Ищем начало текста с автозапчастями
                    for variation in delivery_variations:
                        if variation in description:
                            start_idx = description.find(variation)
                            break
                    
                    # Если нашли начало, ищем конец блока с телефоном
                    if start_idx != -1:
                        # Ищем начало блока с телефоном
                        article_start_idx = -1
                        for variation in article_variations:
                            if variation in description[start_idx:]:
                                article_start_idx = description.find(variation, start_idx)
                                break
                        
                        # Если нашли телефон, ищем его конец
                        if article_start_idx != -1:
                            # Ищем конец абзаца после телефона
                            article_end_idx = description.find("</p>", article_start_idx)
                            if article_end_idx != -1:
                                end_idx = article_end_idx + 4  # +4 для включения </p>
                    
                    # Если нашли оба индекса, выполняем замену
                    if start_idx != -1 and end_idx != -1:
                        new_description = description[:start_idx] + new_text + description[end_idx:]
                        duplicate['Description'] = new_description
                        print(f"Метод 2: Заменен текст в описании для товара {duplicate['Id']} с городом {city}")
            
            # Уникализируем изображения только для дублей товаров (не для оригинальных)
            if original_image_urls:
                # Список для новых уникализированных URL
                unique_image_urls = []
                
                # Определяем, какие изображения являются изображениями магазина (последние в списке)
                shop_images = []
                product_images = original_image_urls.copy()
                
                # Ищем изображения магазина по URL
                for url in reversed(original_image_urls):
                    if shop_image_urls and url in shop_image_urls:
                        shop_images.insert(0, url)  # Добавляем в начало списка
                        product_images.remove(url)  # Удаляем из списка изображений продукта
                    else:
                        break  # Прекращаем поиск, если нашли изображение, которое не является изображением магазина
                
                print(f"Для товара {duplicate['Id']}: найдено {len(product_images)} изображений продукта и {len(shop_images)} изображений магазина")
                
                # Обрабатываем изображения продукта
                # Для дублей товаров (товары с суффиксами -1, -2 и т.д.) уникализируем изображения
                print(f"Товар {duplicate['Id']} является дублем, уникализируем изображения для города")
                for j, img_url in enumerate(product_images):
                    unique_url = process_image_for_derived_products(
                        img_url, 
                        unique_images_dir, 
                        original_id, 
                        city_index + j,  # Добавляем j для большей вариации
                        gdrive_service
                    )
                    if unique_url:
                        unique_image_urls.append(unique_url)
                    else:
                        # Если уникализация не удалась, используем исходный URL
                        unique_image_urls.append(img_url)
                
                # Добавляем изображения магазина без изменений
                unique_image_urls.extend(shop_images)
                
                # Обновляем ImageUrls в дубле
                duplicate['ImageUrls'] = "|".join(unique_image_urls)
                print(f"Для товара {duplicate['Id']}: обновлены URL изображений")
            
            # Добавляем дубль в список всех строк
            all_rows.append(duplicate)
    
    # Создаем новый DataFrame из всех строк
    result_df = pd.DataFrame(all_rows)
    
    # Очищаем папку с уникализированными изображениями после завершения всех операций
    clean_uniqualized_images_folder()
    
    print(f"Создано {len(result_df)} строк (исходные + дубли)")
    return result_df

def load_shop_images_cache():
    """Загружает сохраненные ссылки на изображения магазина из кэша"""
    if os.path.exists(SHOP_IMAGES_CACHE_FILE):
        try:
            with open(SHOP_IMAGES_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                print(f"Загружен кэш изображений магазина, найдено {len(cache)} ссылок")
                
                # Проверяем валидность кэша - проверяем наличие всех изображений из SHOP_IMAGES
                if 'shop_images' in cache and len(cache['shop_images']) == len(SHOP_IMAGES):
                    all_urls_valid = True
                    for url in cache['shop_images']:
                        # Проверяем, что строка похожа на URL
                        if not url.startswith('http'):
                            all_urls_valid = False
                            break
                    
                    if all_urls_valid:
                        print("Кэш валиден, используем сохраненные ссылки")
                        return cache['shop_images']
                
                print("Кэш невалиден или устарел, требуется повторная загрузка изображений")
                return None
        except Exception as e:
            print(f"Ошибка при загрузке кэша изображений магазина: {e}")
            return None
    else:
        print(f"Файл кэша {SHOP_IMAGES_CACHE_FILE} не найден")
        return None

def save_shop_images_cache(shop_image_urls):
    """Сохраняет ссылки на изображения магазина в кэш"""
    if shop_image_urls:
        try:
            cache = {'shop_images': shop_image_urls, 'timestamp': datetime.now().isoformat()}
            with open(SHOP_IMAGES_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            print(f"Кэш изображений магазина сохранен, {len(shop_image_urls)} ссылок")
            return True
        except Exception as e:
            print(f"Ошибка при сохранении кэша изображений магазина: {e}")
    return False

def process_xml(use_gdrive_for_images=True):
    """
    Обработка XML-файла и создание Excel-таблицы с сохранением пользовательских изменений из Google Drive.
    
    Логика работы:
    1. Синхронизация с Google Drive с сохранением всех пользовательских изменений
    2. Обновление только полей Price, Brand, ImageUrls из XML
    3. Добавление новых позиций из XML
    4. Удаление позиций, отсутствующих в XML
    """
    # Синхронизация с Google Drive (теперь с сохранением пользовательских изменений)
    sync_excel_from_gdrive()
    
    # Создание директории для изображений
    output_dir = create_output_dir()
    
    # Инициализация Google Drive API для изображений
    gdrive_service = None
    if use_gdrive_for_images:
        try:
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_CRED_PATH, 
                scopes=['https://www.googleapis.com/auth/drive']
            )
            gdrive_service = build('drive', 'v3', credentials=credentials)
            print("Google Drive API инициализирован для загрузки изображений.")
        except Exception as e:
            print(f"Ошибка при инициализации Google Drive API: {e}")
            print("Изображения будут обработаны без загрузки на Google Drive.")
    
    # Сначала пробуем загрузить ссылки на изображения магазина из кэша
    shop_image_urls = load_shop_images_cache()
    
    # Если не удалось загрузить из кэша, загружаем изображения магазина
    if shop_image_urls is None and gdrive_service and SHOP_IMAGES:
        shop_image_urls = []
        print("Предварительная загрузка изображений магазина...")
        for i, shop_img_path in enumerate(SHOP_IMAGES):
            if os.path.exists(shop_img_path):
                shop_output_path = os.path.join(output_dir, f"shop_image_{i+1}.jpg")
                
                try:
                    # Копируем файлы магазина
                    with open(shop_img_path, 'rb') as src, open(shop_output_path, 'wb') as dst:
                        dst.write(src.read())
                    print(f"Скопировано изображение магазина {shop_img_path} -> {shop_output_path}")
                    
                    # Загружаем изображение магазина в Google Drive
                    shop_url = upload_image_to_gdrive(gdrive_service, shop_output_path)
                    if shop_url:
                        shop_image_urls.append(shop_url)
                        print(f"Изображение магазина предварительно загружено в Google Drive: {shop_url}")
                    else:
                        print(f"Не удалось загрузить изображение магазина в Google Drive")
                except Exception as e:
                    print(f"Ошибка при предварительной загрузке изображения магазина {shop_img_path}: {e}")
        
        print(f"Предварительно загружено {len(shop_image_urls)} изображений магазина")
        
        # Сохраняем ссылки в кэш для будущих запусков
        if shop_image_urls:
            save_shop_images_cache(shop_image_urls)
    
    # Проверяем, существует ли уже файл Excel с данными
    existing_ids = set()
    existing_products_with_missing_images = {}
    existing_data = None
    
    if os.path.exists(OUTPUT_EXCEL_PATH):
        try:
            # Загружаем существующие данные для определения ID товаров, которые уже обработаны
            existing_data = pd.read_excel(OUTPUT_EXCEL_PATH)
            print(f"Загружены существующие данные из {OUTPUT_EXCEL_PATH}, строк: {len(existing_data)}")
            
            # Очищаем "осиротевшие" производные товары
            existing_data, was_cleaned = clean_orphaned_derived_products(existing_data)
            if was_cleaned:
                # Сохраняем очищенные данные
                existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
                print(f"Сохранены очищенные данные в {OUTPUT_EXCEL_PATH}")
                
                # Загружаем очищенный файл на Google Drive
                file_url = upload_to_google_drive(OUTPUT_EXCEL_PATH, force_update=True)
                print(f"Очищенная таблица загружена на Google Drive")
            
            if 'Id' in existing_data.columns:
                # Получаем список существующих Id
                existing_ids = set(existing_data['Id'].astype(str).tolist())
                print(f"Найдено {len(existing_ids)} существующих товаров")
                
                # Проверяем наличие изображений в существующих товарах
                if 'ImageUrls' in existing_data.columns:
                    for index, row in existing_data.iterrows():
                        product_id = str(row['Id'])
                        image_urls = str(row['ImageUrls']) if pd.notna(row['ImageUrls']) else ""
                        
                        # Если у товара нет изображений, добавляем его в список для обработки
                        if not image_urls or image_urls == "nan" or image_urls.strip() == "":
                            existing_products_with_missing_images[product_id] = index
                    
                    if existing_products_with_missing_images:
                        print(f"Найдено {len(existing_products_with_missing_images)} существующих товаров без изображений")
                    else:
                        print("Все существующие товары имеют изображения")
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
        # Находим только базовые ID (без суффиксов) из XML
        base_xml_ids = set()
        for xml_id in xml_ids:
            # Только базовые ID (без суффиксов)
            if "-" not in xml_id:
                base_xml_ids.add(xml_id)
        
        # Находим только базовые ID (без суффиксов) из Excel
        base_excel_ids = set()
        derived_excel_ids = {}  # Словарь для группировки производных ID по базовым
        
        for excel_id in existing_ids:
            if "-" not in excel_id:
                base_excel_ids.add(excel_id)
            else:
                # Если это производный ID (с суффиксом)
                base_id = excel_id.split("-")[0]
                if base_id not in derived_excel_ids:
                    derived_excel_ids[base_id] = []
                derived_excel_ids[base_id].append(excel_id)
        
        # Находим базовые ID, которые были удалены из XML
        removed_base_ids = base_excel_ids - base_xml_ids
        
        if removed_base_ids:
            print(f"Найдено {len(removed_base_ids)} базовых товаров, которые были удалены из XML:")
            for removed_id in removed_base_ids:
                print(f"- {removed_id}")
            
            # Собираем все ID, которые нужно удалить (базовые и их производные)
            all_ids_to_remove = set()
            
            for removed_id in removed_base_ids:
                # Добавляем базовый ID
                all_ids_to_remove.add(removed_id)
                
                # Добавляем все производные ID этого базового ID
                if removed_id in derived_excel_ids:
                    all_ids_to_remove.update(derived_excel_ids[removed_id])
                    print(f"Удаляются производные ID для {removed_id}: {derived_excel_ids[removed_id]}")
            
            # Дополнительная проверка для поиска связанных строк, которые могут не соответствовать шаблону baseId-suffix
            # Например, если ID в Excel был изменен вручную
            if 'Address' in existing_data.columns:
                # Получаем строки с базовыми ID для определения городов
                base_rows = existing_data[existing_data['Id'].astype(str).isin(removed_base_ids)]
                
                # Для каждого удаленного базового товара проверяем возможные связанные товары
                for _, base_row in base_rows.iterrows():
                    base_id = str(base_row['Id'])
                    # Ищем строки с тем же заголовком или описанием, которые могут быть дублями
                    if 'Title' in existing_data.columns and pd.notna(base_row['Title']):
                        title = base_row['Title']
                        similar_title_rows = existing_data[
                            (existing_data['Title'] == title) & 
                            (~existing_data['Id'].astype(str).isin(all_ids_to_remove))
                        ]
                        if not similar_title_rows.empty:
                            similar_ids = similar_title_rows['Id'].astype(str).tolist()
                            print(f"Найдены возможные связанные товары с тем же заголовком для {base_id}: {similar_ids}")
                            all_ids_to_remove.update(similar_ids)
            
            # Удаляем все связанные записи из DataFrame
            old_len = len(existing_data)
            existing_data = existing_data[~existing_data['Id'].astype(str).isin(all_ids_to_remove)]
            new_len = len(existing_data)
            print(f"Удалено {old_len - new_len} записей (базовые товары и их дубли)")
        
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
    
    # Обновляем поля Price и Brand для существующих записей
    price_brand_updated = False
    if existing_data is not None and not existing_data.empty:
        existing_data, price_brand_updated = update_existing_records(existing_data, root)
        
        # Если были обновления, сохраняем файл
        if price_brand_updated:
            existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
            print(f"Сохранены обновленные Price и Brand в {OUTPUT_EXCEL_PATH}")
            
            # Загружаем обновленную таблицу на Google Drive
            file_url = upload_to_google_drive(OUTPUT_EXCEL_PATH, force_update=True)
            print(f"Обновленная таблица с новыми Price и Brand загружена на Google Drive")
    
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
        "TechnicSparePartType", "TransmissionSparePartType", "EngineSparePartType",
        "Delivery"
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
    processed_images_dict = {}  # Словарь для хранения путей к обработанным изображениям
    
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
        
        # Проверяем, является ли этот товар существующим товаром без изображений
        if ad_id in existing_products_with_missing_images:
            print(f"Товар {ad_id} уже существует в таблице, но не имеет изображений. Добавляем изображения.")
            # Обработка изображений
            processed_images = process_images(ad, output_dir, ad_id, gdrive_service)
            if processed_images:
                # Добавляем ссылки на изображения магазина, если они есть
                all_images = list(processed_images)
                if shop_image_urls:
                    # Проверяем, сколько ещё можно добавить изображений (максимум 10)
                    remaining_slots = 10 - len(all_images)
                    if remaining_slots > 0:
                        # Добавляем столько изображений магазина, сколько поместится
                        shop_images_to_add = min(remaining_slots, len(shop_image_urls))
                        print(f"Добавляем {shop_images_to_add} предварительно загруженных изображений магазина")
                        all_images.extend(shop_image_urls[:shop_images_to_add])
                
                # Формируем строку со всеми URL изображений, разделенными |
                image_urls_string = "|".join(all_images)
                
                # Обновляем запись в существующем DataFrame
                row_index = existing_products_with_missing_images[ad_id]
                existing_data.at[row_index, 'ImageUrls'] = image_urls_string
                print(f"Добавлены изображения для товара {ad_id}")
                
                # Если есть секция Images, заменяем её в XML
                images_element = ad.find("Images")
                if images_element is not None:
                    # Удаляем существующие изображения
                    for img in images_element.findall("Image"):
                        images_element.remove(img)
                        
                    # Добавляем новые изображения в XML
                    for i, img_path in enumerate(all_images):
                        # Получаем соответствующий URL
                        img_url = img_path if isinstance(img_path, str) else img_path[0]
                        
                        # Создаём элемент для XML
                        img_elem = ET.SubElement(images_element, "Image")
                        img_elem.text = img_url
                        img_elem.set("url", img_url)
            continue
        
        # Обрабатываем только товары начинающиеся с "bz" и с ограничением на количество
        if not ad_id.startswith("bz") or (MAX_ITEMS is not None and processed_count >= MAX_ITEMS):
            continue
        
        # Проверка существующих записей
        is_existing_product = ad_id in existing_ids
        
        # Проверяем, есть ли производные ID (с суффиксами) для этого базового ID
        has_derived_ids = False
        for existing_id in existing_ids:
            if existing_id.startswith(ad_id + "-"):
                has_derived_ids = True
                break
        
        # Пропускаем товары, которые уже имеют производные записи
        if is_existing_product and has_derived_ids:
            skipped_count += 1
            print(f"Пропуск объявления {ad_id} (уже существует в таблице вместе с производными ID)")
            continue
        
        processed_count += 1
        max_items_display = "∞" if MAX_ITEMS is None else str(MAX_ITEMS - skipped_count)
        print(f"Обработка объявления {ad_id} ({processed_count}/{max_items_display})")
        
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
        
        # Обработка изображений
        processed_images = process_images(ad, output_dir, ad_id, gdrive_service)
        processed_images_dict[ad_id] = processed_images
        
        # Если есть секция Images, заменяем её в XML
        all_images = list(processed_images)
        
        # Добавляем ссылки на изображения магазина, если они есть
        if shop_image_urls:
            # Проверяем, сколько ещё можно добавить изображений (максимум 10)
            remaining_slots = 10 - len(all_images)
            if remaining_slots > 0:
                # Добавляем столько изображений магазина, сколько поместится
                shop_images_to_add = min(remaining_slots, len(shop_image_urls))
                print(f"Добавляем {shop_images_to_add} предварительно загруженных изображений магазина")
                all_images.extend(shop_image_urls[:shop_images_to_add])
        
        if all_images:
            images_element = ad.find("Images")
            if images_element is not None:
                # Удаляем существующие изображения
                for img in images_element.findall("Image"):
                    images_element.remove(img)
                    
                # Добавляем новые изображения в XML
                for i, img_path in enumerate(all_images):
                    # Получаем соответствующий URL
                    img_url = img_path if isinstance(img_path, str) else img_path[0]
                    
                    # Создаём элемент для XML
                    img_elem = ET.SubElement(images_element, "Image")
                    img_elem.text = img_url
                    img_elem.set("url", img_url)
        
        # Формируем строку со всеми URL изображений, разделенными |
        image_urls_string = "|".join(all_images)
        
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
        row_data["CallsDevices"] = "3889715587"
        row_data["ImageUrls"] = image_urls_string
        
        # Устанавливаем значение Delivery по умолчанию для оригинальных товаров
        # Логика: если Address - "Тула, улица Волнянского, 1", то "ПВЗ", иначе пустое значение
        if row_data.get("Address") == "Тула, улица Волнянского, 1":
            row_data["Delivery"] = "ПВЗ"
        else:
            row_data["Delivery"] = ""
        
        data.append(row_data)
    
    # Сохраняем обновленный XML
    output_xml_path = "avito_processed.xml"
    tree.write(output_xml_path, encoding="utf-8", xml_declaration=True)
    print(f"Обработанный XML сохранен: {output_xml_path}")
    
    # Проверяем, были ли обновлены существующие товары с отсутствующими изображениями
    if existing_products_with_missing_images and os.path.exists(OUTPUT_EXCEL_PATH):
        # Сохраняем обновленный DataFrame с добавленными изображениями
        existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
        print(f"Обновлен Excel-файл с добавленными изображениями для {len(existing_products_with_missing_images)} товаров")
        
        # Загружаем файл на Google Drive
        file_url = upload_to_google_drive(OUTPUT_EXCEL_PATH, force_update=True)
        print(f"Обновленная таблица загружена на Google Drive")
        
        if not data:  # Если нет новых товаров для добавления
            return pd.DataFrame(), file_url
    
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
                
                # Создаем запрос для поиска файла
                query = f"name='{file_name}' and trashed=false"
                
                # Если у нас есть GOOGLE_DRIVE_FOLDER_ID, ищем файл только в этой папке
                if GOOGLE_DRIVE_FOLDER_ID:
                    if check_folder_access(drive_service, GOOGLE_DRIVE_FOLDER_ID):
                        query += f" and '{GOOGLE_DRIVE_FOLDER_ID}' in parents"
                
                # Проверка, существует ли файл с таким именем
                response = drive_service.files().list(
                    q=query,
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
    
    # Создаем DataFrame для новых данных
    new_df = pd.DataFrame(data)
    
    # Создаем дубли строк с изменением ID и адреса только для новых товаров
    duplicated_df = duplicate_rows(new_df)
    
    # Объединяем с существующими данными, если они есть
    final_df = duplicated_df
    if existing_data is not None and not existing_data.empty:
        print(f"Объединяем {len(duplicated_df)} новых строк (с дубликатами) с {len(existing_data)} существующими строками")
        final_df = pd.concat([existing_data, duplicated_df], ignore_index=True)
    
    # Выводим информацию о созданном DataFrame
    print(f"Итоговый DataFrame содержит {len(final_df)} строк и {len(final_df.columns)} столбцов")
    print("Столбцы в DataFrame:")
    for i, col in enumerate(final_df.columns):
        print(f"{i+1}. {col}")
    
    # Проверяем, есть ли в DataFrame нужные столбцы
    for param in standard_parameters:
        if param not in final_df.columns:
            print(f"Внимание: Столбец '{param}' отсутствует в DataFrame! Добавляем его...")
            final_df[param] = ""  # Добавляем пустой столбец
    
    # Создаем Excel-файл без вставки изображений, только ссылки
    excel_path, was_updated = save_to_excel(final_df)
    
    # Загружаем файл на Google Drive только если он был обновлен
    if was_updated:
        file_url = upload_to_google_drive(excel_path, force_update=True)
        print(f"Таблица обновлена и загружена на Google Drive")
    else:
        # Если файл не был обновлен, получаем существующую ссылку
        file_url = upload_to_google_drive(excel_path, force_update=False)
        print(f"Таблица не изменилась, используем существующую ссылку")
    
    # Очищаем папку с обработанными изображениями после завершения всех операций
    clean_processed_images_folder()
    
    return final_df, file_url

# Создаем алиас для запуска с Google Drive для изображений
def process_xml_with_gdrive():
    """Обработка XML-файла с загрузкой изображений на Google Drive"""
    return process_xml(use_gdrive_for_images=True)

def job():
    """Основная функция для запуска процесса обработки"""
    print(f"🚀 Начало обработки: {datetime.now()}")
    
    # Сначала скачиваем актуальную версию Excel
    print("📥 Синхронизация с Google Drive...")
    sync_success = sync_excel_from_gdrive()
    if sync_success:
        print("✅ Синхронизация с Google Drive завершена успешно")
    else:
        print("⚠️ Синхронизация с Google Drive завершена с предупреждениями")
    
    # Проверяем консистентность товаров в Excel
    if os.path.exists(OUTPUT_EXCEL_PATH):
        print("🔍 Проверка консистентности товаров в Excel...")
        consistency_changes = check_excel_consistency()
        if consistency_changes:
            print("✅ Консистентность восстановлена, изменения сохранены")
        else:
            print("✅ Данные консистентны, изменений не требуется")
    
    print("📄 Загрузка и обработка XML...")
    if download_xml():
        print("✅ XML-файл успешно загружен")
        df, file_url = process_xml_with_gdrive()
        print(f"✅ Обработка XML завершена")
        print(f"🔗 Ссылка на обработанный документ: {file_url}")
        
        # Дополнительная очистка папки processed_images на случай, если что-то осталось
        print("🧹 Очистка временных файлов...")
        clean_processed_images_folder()
        print("✅ Очистка завершена")
    else:
        print("❌ Ошибка при загрузке XML-файла")
        
    print(f"🏁 Обработка завершена: {datetime.now()}")
    print("=" * 50)

def check_folder_access(drive_service, folder_id):
    """
    Проверяет доступ к папке на Google Drive и устанавливает права доступа если необходимо
    
    drive_service: Инициализированный сервис Google Drive API
    folder_id: ID папки на Google Drive
    
    Возвращает: True если папка доступна, False если нет
    """
    if not folder_id:
        return False
        
    try:
        # Проверяем существование папки
        folder = drive_service.files().get(fileId=folder_id, fields='id, name').execute()
        print(f"Папка найдена: {folder.get('name')} (ID: {folder.get('id')})")
        
        # Проверяем права доступа
        permissions = drive_service.permissions().list(fileId=folder_id).execute()
        
        # Проверяем, есть ли публичный доступ
        has_public_access = False
        for permission in permissions.get('permissions', []):
            if permission.get('type') == 'anyone':
                has_public_access = True
                break
                
        # Если нет публичного доступа, устанавливаем его
        if not has_public_access:
            print("Устанавливаем публичный доступ на папку...")
            drive_service.permissions().create(
                fileId=folder_id,
                body={
                    'type': 'anyone',
                    'role': 'writer',
                }
            ).execute()
            print("Права доступа установлены")
            
        return True
    except Exception as e:
        print(f"Ошибка при проверке доступа к папке: {e}")
        return False

def main():
    """Основная функция для запуска скрипта"""
    # Проверяем доступ к папке Google Drive, если указан ID
    if GOOGLE_DRIVE_FOLDER_ID:
        try:
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_CRED_PATH, 
                scopes=['https://www.googleapis.com/auth/drive']
            )
            drive_service = build('drive', 'v3', credentials=credentials)
            
            # Проверяем доступ к папке
            if not check_folder_access(drive_service, GOOGLE_DRIVE_FOLDER_ID):
                print(f"ВНИМАНИЕ: Не удалось получить доступ к папке с ID {GOOGLE_DRIVE_FOLDER_ID}")
                print("Будет использоваться автоматическое создание папки или корневая папка")
        except Exception as e:
            print(f"Ошибка при проверке доступа к папке Google Drive: {e}")
    
    # Проверяем консистентность товаров в Excel перед началом работы
    if os.path.exists(OUTPUT_EXCEL_PATH):
        print("Проверка консистентности товаров в Excel перед началом работы...")
        check_excel_consistency()
    
    # Сначала запускаем обработку однократно
    job()
    
    # Настраиваем регулярное выполнение задачи (каждые 5 минут)
    schedule.every(5).minutes.do(job)
    
    # Бесконечный цикл для выполнения запланированных задач
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверка каждую минуту

def clean_orphaned_derived_products(existing_data):
    """
    Удаляет производные товары (с суффиксами), если их базовый товар был удален
    
    existing_data: DataFrame с существующими товарами
    
    Возвращает: очищенный DataFrame и флаг, были ли изменения
    """
    if existing_data is None or existing_data.empty or 'Id' not in existing_data.columns:
        return existing_data, False
    
    # Получаем все ID из DataFrame
    all_ids = existing_data['Id'].astype(str).tolist()
    
    # Разделяем на базовые и производные ID
    base_ids = set()
    derived_ids_map = {}  # Словарь, связывающий производные ID с их базовыми ID
    
    for product_id in all_ids:
        if "-" not in product_id:
            base_ids.add(product_id)
        else:
            # Это производный ID
            base_id = product_id.split("-")[0]
            derived_ids_map[product_id] = base_id
    
    # Проверяем каждый производный ID
    orphaned_ids = []
    for derived_id, base_id in derived_ids_map.items():
        if base_id not in base_ids:
            orphaned_ids.append(derived_id)
    
    if orphaned_ids:
        print(f"Найдено {len(orphaned_ids)} производных товаров без базового товара")
        # Группируем по базовым ID для логирования
        orphaned_by_base = {}
        for orphaned_id in orphaned_ids:
            base_id = orphaned_id.split("-")[0]
            if base_id not in orphaned_by_base:
                orphaned_by_base[base_id] = []
            orphaned_by_base[base_id].append(orphaned_id)
        
        # Выводим информацию по группам
        for base_id, orphans in orphaned_by_base.items():
            print(f"Базовый товар {base_id} отсутствует, удаляем его производные: {orphans}")
        
        # Удаляем строки с устаревшими производными товарами
        old_len = len(existing_data)
        cleaned_data = existing_data[~existing_data['Id'].astype(str).isin(orphaned_ids)]
        new_len = len(cleaned_data)
        print(f"Удалено {old_len - new_len} устаревших производных товаров")
        return cleaned_data, True
    
    return existing_data, False

def check_excel_consistency(excel_file_path=OUTPUT_EXCEL_PATH):
    """
    Проверяет консистентность в Excel-файле между базовыми товарами и их дублями.
    Если базовый товар отсутствует, но есть его дубли, они будут удалены.
    
    excel_file_path: путь к Excel-файлу
    
    Возвращает: True, если были внесены изменения, иначе False
    """
    if not os.path.exists(excel_file_path):
        print(f"Файл {excel_file_path} не существует, проверка не выполнена")
        return False
    
    try:
        # Загружаем Excel-файл
        df = pd.read_excel(excel_file_path)
        print(f"Загружен Excel-файл {excel_file_path}, строк: {len(df)}")
        
        if 'Id' not in df.columns:
            print("В Excel-файле отсутствует столбец 'Id', проверка невозможна")
            return False
        
        # Получаем все ID
        all_ids = df['Id'].astype(str).tolist()
        
        # Разделяем на базовые и производные ID
        base_ids = set()
        derived_ids = []
        
        for product_id in all_ids:
            if "-" not in product_id:
                base_ids.add(product_id)
            else:
                derived_ids.append(product_id)
        
        # Проверяем каждый производный ID
        orphaned_ids = []
        for derived_id in derived_ids:
            parts = derived_id.split("-")
            if len(parts) >= 2:
                base_id = parts[0]
                if base_id not in base_ids:
                    orphaned_ids.append(derived_id)
        
        if orphaned_ids:
            print(f"Найдено {len(orphaned_ids)} производных товаров без базового товара")
            
            # Группируем по базовым ID для улучшения логирования
            orphaned_by_base = {}
            for orphaned_id in orphaned_ids:
                base_id = orphaned_id.split("-")[0]
                if base_id not in orphaned_by_base:
                    orphaned_by_base[base_id] = []
                orphaned_by_base[base_id].append(orphaned_id)
            
            # Выводим информацию по группам
            for base_id, orphans in orphaned_by_base.items():
                print(f"Базовый товар {base_id} отсутствует, его производные будут удалены: {orphans}")
            
            # Удаляем строки с "осиротевшими" производными ID
            old_len = len(df)
            df = df[~df['Id'].astype(str).isin(orphaned_ids)]
            new_len = len(df)
            
            print(f"Удалено {old_len - new_len} строк с 'осиротевшими' производными товарами")
            
            # Сохраняем обновленный Excel-файл
            df.to_excel(excel_file_path, index=False)
            print(f"Обновленный Excel-файл сохранен: {excel_file_path}")
            
            # Загружаем на Google Drive
            file_url = upload_to_google_drive(excel_file_path, force_update=True)
            if file_url:
                print(f"Обновленный Excel-файл загружен на Google Drive: {file_url}")
            
            return True
        else:
            print("В Excel-файле нет 'осиротевших' производных товаров")
            return False
        
    except Exception as e:
        print(f"Ошибка при проверке консистентности Excel-файла: {e}")
        import traceback
        traceback.print_exc()
        return False

def uniqualize_image(input_image_path_or_url, output_path, city_index):
    """
    Создает уникализированную версию изображения, изменяя метаданные и визуальные параметры
    
    input_image_path_or_url: путь к исходному изображению или URL
    output_path: путь для сохранения уникализированного изображения
    city_index: индекс города, используется для вариации параметров
    
    Возвращает: путь к уникализированному изображению
    """
    try:
        print(f"Уникализация изображения для города с индексом {city_index}")
        
        # Определяем, является ли вход URL или локальным путем
        is_url = input_image_path_or_url.startswith('http')
        
        if is_url:
            # Загрузка изображения из URL
            response = requests.get(input_image_path_or_url)
            if response.status_code != 200:
                print(f"Ошибка загрузки изображения по URL {input_image_path_or_url}, код: {response.status_code}")
                return None
                
            img = PILImage.open(BytesIO(response.content))
        else:
            # Загрузка локального изображения
            img = PILImage.open(input_image_path_or_url)
        
        # Конвертируем в RGB, если это не RGB
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # 1. Изменение контраста и яркости
        # Используем индекс города для вариации параметров
        contrast_factor = 1.0 + (city_index % 3 + 1) * 0.05  # Варьируется от 1.05 до 1.15
        brightness_factor = 1.0 + (city_index % 5 - 2) * 0.02  # Варьируется от 0.96 до 1.04
        
        # Применяем изменения контраста
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(contrast_factor)
        
        # Применяем изменения яркости
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(brightness_factor)
        
        # 2. Добавление шума
        # Создаем массив NumPy из изображения
        img_array = np.array(img)
        
        # Генерируем шум на основе индекса города
        noise_level = (city_index % 4 + 2) * 2  # Варьируется от 4 до 10
        noise = np.random.normal(0, noise_level, img_array.shape)
        
        # Применяем шум к изображению
        noisy_img_array = np.clip(img_array + noise, 0, 255).astype(np.uint8)
        img = PILImage.fromarray(noisy_img_array)
        
        # 3. Легкое размытие (для некоторых изображений)
        if city_index % 3 == 0:
            blur_radius = (city_index % 2) * 0.3 + 0.1  # Варьируется от 0.1 до 0.4
            img = img.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        
        # 4. Небольшой поворот для некоторых изображений
        if city_index % 4 == 0:
            rotation_angle = (city_index % 3 - 1) * 0.5  # Варьируется от -0.5 до 0.5 градусов
            img = img.rotate(rotation_angle, resample=PILImage.BICUBIC, expand=False)
        
        # Сохраняем измененное изображение
        img.save(output_path, quality=95)
        
        # 5. Изменение метаданных (EXIF)
        try:
            # Создаем базовые EXIF данные
            exif_dict = {'0th': {}, 'Exif': {}, 'GPS': {}, '1st': {}}
            
            # Устанавливаем дату создания (сдвигаем на 1-2 дня относительно текущей)
            days_shift = city_index % 3 + 1  # 1, 2 или 3 дня
            creation_date = (datetime.now() - timedelta(days=days_shift)).strftime("%Y:%m:%d %H:%M:%S")
            exif_dict['0th'][piexif.ImageIFD.DateTime] = creation_date
            exif_dict['Exif'][piexif.ExifIFD.DateTimeOriginal] = creation_date
            exif_dict['Exif'][piexif.ExifIFD.DateTimeDigitized] = creation_date
            
            # Варьируем имя камеры
            camera_models = [
                "iPhone 13", "Samsung Galaxy S21", "Google Pixel 6", 
                "Xiaomi Mi 11", "Sony Alpha", "Canon EOS R5"
            ]
            camera_model = camera_models[city_index % len(camera_models)]
            exif_dict['0th'][piexif.ImageIFD.Model] = camera_model
            
            # Варьируем производителя
            manufacturers = ["Apple", "Samsung", "Google", "Xiaomi", "Sony", "Canon"]
            manufacturer = manufacturers[city_index % len(manufacturers)]
            exif_dict['0th'][piexif.ImageIFD.Make] = manufacturer
            
            # Добавляем случайные GPS координаты для некоторых изображений
            if city_index % 3 == 0:
                # Координаты некоторых городов России (примерные)
                city_coords = [
                    (55.7558, 37.6173),  # Москва
                    (59.9343, 30.3351),  # Санкт-Петербург
                    (56.8431, 60.6454),  # Екатеринбург
                    (55.0415, 82.9346),  # Новосибирск
                    (56.3287, 44.0020),  # Нижний Новгород
                    (53.1950, 50.1982),  # Самара
                    (51.5406, 46.0086),  # Саратов
                    (45.0448, 38.9760)   # Краснодар
                ]
                
                # Выбираем координаты и добавляем небольшое случайное смещение
                base_lat, base_lon = city_coords[city_index % len(city_coords)]
                lat = base_lat + (random.random() - 0.5) * 0.01  # Смещение ±0.005 градуса
                lon = base_lon + (random.random() - 0.5) * 0.01
                
                def to_deg(value, loc):
                    """Конвертирует десятичные градусы в градусы, минуты, секунды в формате рациональных чисел"""
                    if value < 0:
                        loc_value = -value
                    else:
                        loc_value = value
                    
                    deg = int(loc_value)
                    d = loc_value - deg
                    min = int(d * 60)
                    sec = int((d - min / 60) * 3600 * 100)
                    
                    # Возвращаем кортежи в формате ((числитель, знаменатель), ...)
                    return ((deg, 1), (min, 1), (sec, 100))
                
                try:
                    # Добавляем GPS данные в словарь EXIF
                    exif_dict['GPS'][piexif.GPSIFD.GPSVersionID] = (2, 2, 0, 0)
                    exif_dict['GPS'][piexif.GPSIFD.GPSLatitudeRef] = 'N' if lat >= 0 else 'S'
                    exif_dict['GPS'][piexif.GPSIFD.GPSLongitudeRef] = 'E' if lon >= 0 else 'W'
                    
                    # Преобразуем координаты в формат рациональных чисел
                    exif_dict['GPS'][piexif.GPSIFD.GPSLatitude] = to_deg(abs(lat), 'lat')
                    exif_dict['GPS'][piexif.GPSIFD.GPSLongitude] = to_deg(abs(lon), 'lon')
                except Exception as e:
                    print(f"Ошибка при добавлении GPS данных: {e}")
                    # Удаляем GPS данные, чтобы не вызвать ошибку при сохранении
                    exif_dict['GPS'] = {}
            
            # Собираем EXIF данные и добавляем их к изображению
            try:
                exif_bytes = piexif.dump(exif_dict)
                piexif.insert(exif_bytes, output_path)
                print(f"EXIF метаданные успешно изменены для изображения {output_path}")
            except Exception as e:
                print(f"Ошибка при сохранении EXIF данных: {e}")
                # Если не удалось сохранить все метаданные, пробуем сохранить только основные
                try:
                    # Создаем более простой EXIF словарь без GPS данных
                    simple_exif = {'0th': {}, 'Exif': {}, '1st': {}}
                    simple_exif['0th'][piexif.ImageIFD.DateTime] = creation_date
                    simple_exif['Exif'][piexif.ExifIFD.DateTimeOriginal] = creation_date
                    
                    exif_bytes = piexif.dump(simple_exif)
                    piexif.insert(exif_bytes, output_path)
                    print(f"Упрощенные EXIF метаданные сохранены для изображения {output_path}")
                except Exception as e2:
                    print(f"Не удалось сохранить даже упрощенные EXIF метаданные: {e2}")
            
        except Exception as e:
            print(f"Ошибка при изменении EXIF метаданных: {e}")
            # Продолжаем выполнение, так как изображение уже было сохранено с визуальными изменениями
        
        return output_path
        
    except Exception as e:
        print(f"Ошибка при уникализации изображения: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_image_for_derived_products(original_image_url, output_dir, base_ad_id, city_index, gdrive_service=None):
    """
    Обрабатывает изображение для производных товаров с уникализацией
    
    original_image_url: URL исходного изображения
    output_dir: директория для сохранения обработанных изображений
    base_ad_id: базовый ID товара
    city_index: индекс города (для вариации параметров уникализации)
    gdrive_service: сервис Google Drive API для загрузки
    
    Возвращает: URL уникализированного изображения (на Google Drive или локальный путь)
    """
    # Формируем уникальное имя файла
    output_filename = f"{base_ad_id}_derived_{city_index}_{uuid.uuid4().hex[:8]}.jpg"
    output_path = os.path.join(output_dir, output_filename)
    
    # Уникализируем изображение
    result_path = uniqualize_image(original_image_url, output_path, city_index)
    
    if result_path:
        # Загрузка в Google Drive, если сервис предоставлен
        if gdrive_service:
            try:
                print(f"Начинаем загрузку уникализированного изображения {output_filename} на Google Drive")
                file_url = upload_image_to_gdrive(gdrive_service, result_path)
                if file_url:
                    print(f"Уникализированное изображение {output_filename} загружено в Google Drive: {file_url}")
                    return file_url
                else:
                    print(f"Ошибка: не удалось получить URL для изображения {output_filename}")
                    # В случае ошибки возвращаем локальный путь
                    return output_path
            except Exception as e:
                print(f"Исключение при загрузке в Google Drive: {e}")
                import traceback
                traceback.print_exc()
                # В случае исключения возвращаем локальный путь
                return output_path
        else:
            # Если Google Drive не используется, возвращаем локальный путь
            return output_path
    
    # В случае ошибки возвращаем исходный URL
    return original_image_url

def update_existing_records(existing_data, xml_root):
    """
    Обновляет поля Price и Brand для существующих записей на основе данных из XML
    
    existing_data: DataFrame с существующими данными
    xml_root: корневой элемент XML дерева
    
    Возвращает: обновленный DataFrame и флаг изменений
    """
    if existing_data is None or existing_data.empty:
        return existing_data, False
    
    print("Обновление полей Price и Brand для существующих товаров...")
    
    # Создаем словарь для быстрого поиска данных из XML
    xml_data = {}
    for ad in xml_root.findall("Ad"):
        ad_id_elem = ad.find("Id")
        if ad_id_elem is not None and ad_id_elem.text is not None:
            ad_id = ad_id_elem.text
            
            # Извлекаем Price и Brand из XML
            price_elem = ad.find("Price")
            brand_elem = ad.find("Brand")
            
            xml_data[ad_id] = {
                'Price': price_elem.text if price_elem is not None and price_elem.text else "",
                'Brand': brand_elem.text if brand_elem is not None and brand_elem.text else ""
            }
    
    changes_made = False
    updated_count = 0
    
    # Обновляем данные в existing_data
    for index, row in existing_data.iterrows():
        row_id = str(row['Id'])
        
        # Определяем базовый ID (без суффикса для дублей)
        base_id = row_id.split('-')[0] if '-' in row_id else row_id
        
        # Проверяем, есть ли данные в XML для этого базового ID
        if base_id in xml_data:
            xml_record = xml_data[base_id]
            row_updated = False
            
            # Обновляем Price если есть новое значение
            if 'Price' in existing_data.columns and xml_record['Price']:
                if pd.isna(row['Price']) or str(row['Price']) != xml_record['Price']:
                    existing_data.at[index, 'Price'] = xml_record['Price']
                    changes_made = True
                    row_updated = True
                    print(f"Обновлена цена для товара {row_id}: {xml_record['Price']}")
            
            # Обновляем Brand если есть новое значение
            if 'Brand' in existing_data.columns and xml_record['Brand']:
                if pd.isna(row['Brand']) or str(row['Brand']) != xml_record['Brand']:
                    existing_data.at[index, 'Brand'] = xml_record['Brand']
                    changes_made = True
                    row_updated = True
                    print(f"Обновлен бренд для товара {row_id}: {xml_record['Brand']}")
            
            # Увеличиваем счетчик, если запись была обновлена
            if row_updated:
                updated_count += 1
    
    if changes_made:
        print(f"Обновлено {updated_count} записей с новыми данными Price и/или Brand")
    else:
        print("Все поля Price и Brand актуальны, обновлений не требуется")
    
    return existing_data, changes_made

def merge_with_gdrive_changes(local_data, gdrive_data):
    """
    Объединяет локальные данные с изменениями из Google Drive таблицы.
    Сохраняет все ручные изменения пользователя, обновляя только определенные поля из XML.
    
    local_data: DataFrame с локальными данными (может быть None)
    gdrive_data: DataFrame с данными из Google Drive
    
    Возвращает: объединенный DataFrame с сохраненными пользовательскими изменениями
    """
    if gdrive_data is None or gdrive_data.empty:
        print("📄 Данные из Google Drive пусты, используются локальные данные")
        return local_data
    
    if local_data is None or local_data.empty:
        print("📄 Локальные данные отсутствуют, используются данные из Google Drive")
        return gdrive_data
    
    print("🔄 Объединение локальных данных с изменениями из Google Drive...")
    
    # Поля, которые могут обновляться из XML (остальные сохраняются из Google Drive)
    xml_updatable_fields = {'Price', 'Brand', 'ImageUrls'}
    
    # Поля, которые всегда сохраняются из Google Drive (пользовательские изменения)
    user_editable_fields = set(gdrive_data.columns) - xml_updatable_fields - {'Id'}
    
    print(f"📊 Поля, обновляемые из XML: {xml_updatable_fields}")
    print(f"🔒 Поля, сохраняемые из Google Drive: {user_editable_fields}")
    
    # Создаем копию данных из Google Drive как основу
    merged_data = gdrive_data.copy()
    
    # Создаем словарь локальных данных для быстрого поиска по ID
    local_dict = {}
    if 'Id' in local_data.columns:
        for index, row in local_data.iterrows():
            local_dict[str(row['Id'])] = row
    
    # Обновляем записи в merged_data локальными данными только для специфичных полей
    updated_count = 0
    for index, row in merged_data.iterrows():
        row_id = str(row['Id'])
        
        if row_id in local_dict:
            local_row = local_dict[row_id]
            row_updated = False
            
            # Обновляем только XML-специфичные поля из локальных данных
            for field in xml_updatable_fields:
                if field in merged_data.columns and field in local_data.columns:
                    local_value = local_row[field]
                    if pd.notna(local_value) and str(local_value).strip() != '':
                        # Проверяем, отличается ли значение
                        current_value = row[field]
                        if pd.isna(current_value) or str(current_value) != str(local_value):
                            merged_data.at[index, field] = local_value
                            row_updated = True
                            print(f"  ↻ Обновлено поле {field} для товара {row_id}")
            
            if row_updated:
                updated_count += 1
    
    # Добавляем новые записи из локальных данных, которых нет в Google Drive
    gdrive_ids = set(str(id_val) for id_val in gdrive_data['Id'].astype(str).tolist()) if 'Id' in gdrive_data.columns else set()
    local_ids = set(str(id_val) for id_val in local_data['Id'].astype(str).tolist()) if 'Id' in local_data.columns else set()
    
    new_ids = local_ids - gdrive_ids
    if new_ids:
        print(f"➕ Найдено {len(new_ids)} новых записей в локальных данных")
        for new_id in new_ids:
            if new_id in local_dict:
                new_row = local_dict[new_id].to_dict()
                merged_data = pd.concat([merged_data, pd.DataFrame([new_row])], ignore_index=True)
                print(f"  ➕ Добавлена новая запись: {new_id}")
    
    print(f"✅ Объединение завершено: обновлено {updated_count} записей, добавлено {len(new_ids) if new_ids else 0} новых записей")
    
    return merged_data

if __name__ == "__main__":
    main()
