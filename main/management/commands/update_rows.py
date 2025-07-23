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
# Заменяем Google Drive импорты на Яндекс.Диск
import yadisk
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
OUTPUT_EXCEL_PATH = "few_cities_ya_prod.xlsx" 
YANDEX_DISK_TOKEN = "y0__xCrwb2zBhjg9zggkqjj5xNpZfReuH1ncGXXjdTb_Z-ydk1LPw" # Токен Яндекс.Диска из переменной окружения
disk = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)
MAX_ITEMS = 99999999999999 # Убираем ограничение для продакшена
YANDEX_DISK_FOLDER_PATH = '/avito_excel/'   # Путь к папке на Яндекс.Диске
SHOP_IMAGES_CACHE_FILE = "shop_images_cache.json"  # Файл для кэширования ссылок на изображения магазина

# Конфигурация для локального хранения изображений
LOCAL_IMAGES_DIR = "media/processed_images"  # Папка для обработанных изображений
LOCAL_UNIQUE_IMAGES_DIR = "media/uniqualized_images"  # Папка для уникализированных изображений
SERVER_BASE_URL = "https://custflow-admin.store"  # Базовый URL сервера (изменить на продакшн URL)

# Область доступа для Google Drive API
# SCOPES = ['https://www.googleapis.com/auth/drive']

def check_django_server():
    """Проверяет, запущен ли Django сервер"""
    try:
        response = requests.get(f"{SERVER_BASE_URL}/admin/", timeout=5)
        return True
    except requests.exceptions.RequestException:
        return False

def ensure_django_server_running():
    """Запускает Django сервер если он не запущен"""
    if not check_django_server():
        print("🚀 Django сервер не запущен, запускаем...")
        
        def run_server():
            import subprocess
            import sys
            
            try:
                # Запускаем сервер в фоновом режиме
                subprocess.Popen([
                    sys.executable, "manage.py", "runserver", "localhost:8000"
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                # Ждем запуска сервера
                for _ in range(30):
                    time.sleep(1)
                    if check_django_server():
                        print("✅ Django сервер успешно запущен")
                        return True
                        
                print("⚠️ Django сервер не запустился в течение 30 секунд")
                return False
                
            except Exception as e:
                print(f"❌ Ошибка при запуске Django сервера: {e}")
                return False
        
        return run_server()
    else:
        print("✅ Django сервер уже запущен")
        return True

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
    "images/3.png"
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
            
            response = requests.get(XML_URL, headers=headers, timeout=60)
            
            if response.status_code == 200:
                with open(LOCAL_XML_PATH, 'wb') as f:
                    f.write(response.content)
                return True
            elif response.status_code == 429:
                # Если сервер вернул 429, ждем дольше
                wait_time = retry_delay * attempt
                time.sleep(wait_time)
            else:
                if attempt < max_retries:
                    time.sleep(retry_delay)
                
        except Exception:
            if attempt < max_retries:
                time.sleep(retry_delay)
    
    # Проверяем, есть ли локальная копия XML-файла
    if os.path.exists(LOCAL_XML_PATH):
        return True
    
    return False

def create_output_dir():
    """Создание директории для обработанных изображений"""
    # Создаем директорию для обработанных изображений в media
    if not os.path.exists(LOCAL_IMAGES_DIR):
        os.makedirs(LOCAL_IMAGES_DIR)
    
    # Создаем директорию для уникализированных изображений в media
    if not os.path.exists(LOCAL_UNIQUE_IMAGES_DIR):
        os.makedirs(LOCAL_UNIQUE_IMAGES_DIR)
    
    return LOCAL_IMAGES_DIR


def overlay_image(base_image_url, overlay_path, output_path):
    """Наложение одного изображения на другое с сохранением соотношения сторон и загрузкой на Яндекс.Диск"""
    try:
        response = requests.get(base_image_url)
        if response.status_code != 200:
            return None
        base_img = PILImage.open(BytesIO(response.content)).convert("RGBA")
        overlay_img = PILImage.open(overlay_path).convert("RGBA")
        base_width, base_height = base_img.size
        overlay_width, overlay_height = overlay_img.size
        ratio = min(base_width / overlay_width, base_height / overlay_height)
        new_overlay_width = int(overlay_width * ratio)
        new_overlay_height = int(overlay_height * ratio)
        overlay_img = overlay_img.resize((new_overlay_width, new_overlay_height), PILImage.LANCZOS)
        paste_x = (base_width - new_overlay_width) // 2
        bottom_margin = int(base_height * 0.005)
        paste_y = base_height - new_overlay_height - bottom_margin
        if paste_y < 0:
            paste_y = 0
        result = PILImage.new("RGBA", base_img.size, (0, 0, 0, 0))
        result.paste(base_img, (0, 0))
        result.paste(overlay_img, (paste_x, paste_y), overlay_img)
        result = result.convert("RGB")
        result.save(output_path)
        yadisk_url = upload_image_to_yandex_disk(output_path)
        return yadisk_url
    except Exception as e:
        print(f"Ошибка overlay_image: {e}")
        return None

def add_shop_image(base_image_url, shop_image_path, output_path):
    """Добавляет изображение магазина к первому изображению товара в виде коллажа и загружает на Яндекс.Диск"""
    try:
        response = requests.get(base_image_url)
        if response.status_code != 200:
            return None
        base_img = PILImage.open(BytesIO(response.content)).convert("RGB")
        shop_img = PILImage.open(shop_image_path).convert("RGB")
        base_width, base_height = base_img.size
        collage_width = base_width * 2 + 20
        collage_height = base_height
        shop_width, shop_height = shop_img.size
        new_shop_height = base_height
        new_shop_width = int(shop_width * (new_shop_height / shop_height))
        shop_img = shop_img.resize((new_shop_width, new_shop_height), PILImage.LANCZOS)
        collage = PILImage.new("RGB", (collage_width, collage_height), (255, 255, 255))
        collage.paste(base_img, (0, 0))
        collage.paste(shop_img, (base_width + 20, 0))
        collage.save(output_path)
        yadisk_url = upload_image_to_yandex_disk(output_path)
        return yadisk_url
    except Exception as e:
        print(f"Ошибка add_shop_image: {e}")
        return None


def upload_images_to_yandex_disk(local_paths, remote_folder="/avito_images/"):
    """
    Загружает список локальных изображений на Яндекс.Диск и возвращает список публичных ссылок.
    Если файл уже есть на Яндекс.Диске, используется существующая публичная ссылка.
    """
    public_urls = []
    for local_path in local_paths:
        try:
            file_name = os.path.basename(local_path)
            remote_path = f"{remote_folder.rstrip('/')}/{file_name}"
            # Проверяем, существует ли файл на Яндекс.Диске
            file_exists = False
            try:
                meta = disk.get_meta(remote_path)
                file_exists = True
            except Exception:
                file_exists = False
            if file_exists:
                # Если файл есть, получаем публичную ссылку (публикуем, если нужно)
                try:
                    yadisk_url = getattr(meta, 'public_url', None)
                    if not yadisk_url:
                        disk.publish(remote_path)
                        meta = disk.get_meta(remote_path)
                        yadisk_url = getattr(meta, 'public_url', None)
                    public_urls.append(yadisk_url)
                    continue
                except Exception as e:
                    print(f"Ошибка публикации {remote_path}: {e}")
            # Если файла нет, загружаем и публикуем
            disk.upload(local_path, remote_path, overwrite=True)
            disk.publish(remote_path)
            meta = disk.get_meta(remote_path)
            yadisk_url = getattr(meta, 'public_url', None)
            public_urls.append(yadisk_url)
        except Exception as e:
            print(f"Ошибка загрузки {local_path} на Яндекс.Диске: {e}")
    return public_urls

def process_images(ad_element, output_dir, ad_id, shop_image_path=None):
    """Обработка изображений для объявления"""
    
    # Попробуем получить изображения различными способами
    images = ad_element.findall(".//Image")
    
    if not images:
        # Попробуем другой способ поиска изображений
        images_section = ad_element.find("Images")
        if images_section is not None:
            images = images_section.findall("Image")
    
    if not images:
        # Попробуем получить изображения напрямую из атрибутов url
        try:
            # Проверим, есть ли элемент Images и что в нем
            images_section = ad_element.find("Images")
            if images_section is not None:
                for child in images_section:
                    if 'url' in child.attrib:
                        # Создаем список URL из атрибутов
                        original_urls = [child.attrib['url'] for child in images_section if 'url' in child.attrib]
                        
                        # Обработка изображений по найденным URL
                        return process_image_urls(original_urls, output_dir, ad_id, shop_image_path)
        except Exception as e:
            pass  # Убираем детальное логирование ошибок
        
        return []  # Нет изображений для обработки

    os.makedirs(output_dir, exist_ok=True)
    
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
    
    return process_image_urls(original_urls, output_dir, ad_id, shop_image_path)

def process_image_urls(original_urls, output_dir, ad_id, shop_image_path=None):
    """Обработка URL изображений для объявления и загрузка на Яндекс.Диск"""
    if not original_urls:
        return []
    processed_urls = []
    # Обрабатываем все фотографии
    for i, img_url in enumerate(original_urls):
        if not img_url:
            continue
        output_filename = f"{ad_id}_{i+1}.jpg"
        output_path = os.path.join(output_dir, output_filename)
        # Кэширование: если файл уже есть на Яндекс.Диске, используем ссылку
        remote_folder = '/avito_images/'
        file_name = os.path.basename(output_path)
        remote_path = f"{remote_folder.rstrip('/')}/{file_name}"
        try:
            meta = disk.get_meta(remote_path)
            if not meta.is_public:
                disk.publish(remote_path)
                meta = disk.get_meta(remote_path)
            yadisk_url = meta.public_url
            print(f"Использована существующая ссылка на Яндекс.Диск: {yadisk_url}")
        except Exception:
            if i == 0 and shop_image_path and os.path.exists(shop_image_path):
                yadisk_url = add_shop_image(img_url, shop_image_path, output_path)
            elif i < 4:
                overlay_index = i % len(OVERLAY_IMAGES)
                overlay_path = OVERLAY_IMAGES[overlay_index]
                yadisk_url = overlay_image(img_url, overlay_path, output_path)
            else:
                try:
                    response = requests.get(img_url)
                    if response.status_code == 200:
                        with open(output_path, 'wb') as f:
                            f.write(response.content)
                        yadisk_url = upload_image_to_yandex_disk(output_path)
                    else:
                        yadisk_url = None
                except Exception as e:
                    yadisk_url = None
        if yadisk_url:
            processed_urls.append(yadisk_url)
            print(f"Обработано изображение: {output_filename} -> {yadisk_url}")
    return processed_urls

def process_images_for_original_products(ad_element, output_dir, ad_id, shop_image_path=None):
    """Обработка изображений для оригинальных товаров с сохранением в uniqualized_images"""
    
    # Попробуем получить изображения различными способами
    images = ad_element.findall(".//Image")
    
    if not images:
        # Попробуем другой способ поиска изображений
        images_section = ad_element.find("Images")
        if images_section is not None:
            images = images_section.findall("Image")
    
    if not images:
        # Попробуем получить изображения напрямую из атрибутов url
        try:
            # Проверим, есть ли элемент Images и что в нем
            images_section = ad_element.find("Images")
            if images_section is not None:
                for child in images_section:
                    if 'url' in child.attrib:
                        # Создаем список URL из атрибутов
                        original_urls = [child.attrib['url'] for child in images_section if 'url' in child.attrib]
                        
                        # Обработка изображений по найденным URL
                        return process_image_urls_for_original_products(original_urls, output_dir, ad_id, shop_image_path)
        except Exception as e:
            pass  # Убираем детальное логирование ошибок
        
        return []  # Нет изображений для обработки

    os.makedirs(output_dir, exist_ok=True)
    
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
    
    return process_image_urls_for_original_products(original_urls, output_dir, ad_id, shop_image_path)

def process_image_urls_for_original_products(original_urls, output_dir, ad_id, shop_image_path=None):
    """Обработка URL изображений для оригинальных товаров с загрузкой на Яндекс.Диск"""
    if not original_urls:
        return []
    processed_urls = []
    # Получаем shop_image_urls из кэша
    try:
        shop_image_urls = load_shop_images_cache() or []
    except Exception:
        shop_image_urls = []
    for i, img_url in enumerate(original_urls):
        if not img_url:
            continue
        output_filename = f"{ad_id}_original_{i+1}.jpg"
        output_path = os.path.join(output_dir, output_filename)
        remote_folder = '/avito_images/'
        file_name = os.path.basename(output_path)
        remote_path = f"{remote_folder.rstrip('/')}/{file_name}"
        yadisk_url = None
        file_exists = False
        try:
            meta = disk.get_meta(remote_path)
            file_exists = True
            if not hasattr(meta, 'public_url') or not meta.public_url:
                disk.publish(remote_path)
                meta = disk.get_meta(remote_path)
            yadisk_url = getattr(meta, 'public_url', None)
            if yadisk_url:
                print(f"[КЭШ] Использована существующая ссылка на Яндекс.Диск: {yadisk_url}")
            else:
                print(f"[INFO] Файл {remote_path} найден, но не удалось получить публичную ссылку.")
        except Exception as e:
            print(f"[INFO] Файл {remote_path} не найден на Яндекс.Диске — будет загружен.")
            file_exists = False
        if file_exists and yadisk_url:
            # Фильтрация: только Яндекс.Диск или shop_image_urls
            if (str(yadisk_url).startswith('https://disk.yandex.ru') or str(yadisk_url).startswith('https://yadi.sk') or yadisk_url in shop_image_urls):
                processed_urls.append(yadisk_url)
            print(f"[SKIP UPLOAD] Файл уже есть на Яндекс.Диске: {remote_path}")
            continue
        # Если файла нет, только тогда загружаем
        if i == 0 and shop_image_path and os.path.exists(shop_image_path):
            yadisk_url = add_shop_image(img_url, shop_image_path, output_path)
            print(f"[UPLOAD] Загружен коллаж с магазином: {output_filename} -> {yadisk_url}")
        elif i < 4:
            overlay_index = i % len(OVERLAY_IMAGES)
            overlay_path = OVERLAY_IMAGES[overlay_index]
            yadisk_url = overlay_image(img_url, overlay_path, output_path)
            print(f"[UPLOAD] Загружено с наложением: {output_filename} -> {yadisk_url}")
        else:
            try:
                response = requests.get(img_url)
                if response.status_code == 200:
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    yadisk_url = upload_image_to_yandex_disk(output_path)
                    print(f"[UPLOAD] Загружено оригинальное изображение: {output_filename} -> {yadisk_url}")
                else:
                    yadisk_url = None
            except Exception as e:
                print(f"[ERROR] Ошибка загрузки изображения {img_url}: {e}")
                yadisk_url = None
        if yadisk_url:
            # Фильтрация: только Яндекс.Диск или shop_image_urls
            if (str(yadisk_url).startswith('https://disk.yandex.ru') or str(yadisk_url).startswith('https://yadi.sk') or yadisk_url in shop_image_urls):
                processed_urls.append(yadisk_url)
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
                    except Exception:
                        # Альтернативный способ создания заливки
                        try:
                            from openpyxl.styles import Fill
                            yellow_fill = openpyxl.styles.PatternFill(patternType='solid', fgColor='FFFF00')
                        except Exception:
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
                            for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):  # Начинаем с 2, пропуская заголовок
                                cell = row[id_col_index - 1]  # Получаем ячейку с Id
                                try:
                                    if cell.value and "-" not in str(cell.value):  # Если это исходный товар (без суффикса "-")
                                        for cell in row:
                                            try:
                                                cell.fill = yellow_fill
                                            except Exception:
                                                # Альтернативный подход через прямую установку атрибута
                                                try:
                                                    cell._style.fill = yellow_fill
                                                except:
                                                    pass
                                except Exception:
                                    pass
                    
                    # Сохраняем отформатированный файл
                    try:
                        wb.save(output_path)
                    except Exception:
                        # Если не удалось сохранить отформатированный файл, используем оригинальный
                        merged_df.to_excel(output_path, index=False)
                except Exception:
                    # Сохраняем без форматирования
                    merged_df.to_excel(output_path, index=False)
                
                # Удаляем временный файл
                try:
                    os.remove(temp_output)
                except Exception:
                    pass
                
                return output_path, True  # Файл был обновлен
            else:
                return output_path, False  # Файл не был обновлен
        else:
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
                except Exception:
                    # Альтернативный способ создания заливки
                    try:
                        from openpyxl.styles import Fill
                        yellow_fill = openpyxl.styles.PatternFill(patternType='solid', fgColor='FFFF00')
                    except Exception:
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
                        for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
                            cell = row[id_col_index - 1]
                            try:
                                if cell.value and "-" not in str(cell.value):
                                    for cell in row:
                                        try:
                                            cell.fill = yellow_fill
                                        except Exception:
                                            # Альтернативный подход
                                            try:
                                                cell._style.fill = yellow_fill
                                            except:
                                                pass
                            except Exception:
                                pass
                
                # Сохраняем отформатированный файл
                try:
                    wb.save(output_path)
                except Exception:
                    # Если не удалось сохранить отформатированный файл, используем оригинальный
                    merged_df.to_excel(output_path, index=False)
            except Exception:
                # Сохраняем без форматирования
                merged_df.to_excel(output_path, index=False)
            
            # Удаляем временный файл
            try:
                os.remove(temp_output)
            except Exception:
                pass
            
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
            except Exception:
                # Альтернативный способ создания заливки
                try:
                    from openpyxl.styles import Fill
                    yellow_fill = openpyxl.styles.PatternFill(patternType='solid', fgColor='FFFF00')
                except Exception:
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
                    for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
                        cell = row[id_col_index - 1]
                        try:
                            if cell.value and "-" not in str(cell.value):
                                for cell in row:
                                    try:
                                        cell.fill = yellow_fill
                                    except Exception:
                                        # Альтернативный подход
                                        try:
                                            cell._style.fill = yellow_fill
                                        except:
                                            pass
                        except Exception:
                            pass
                
                # Сохраняем отформатированный файл
                try:
                    wb.save(output_path)
                except Exception:
                    # Если не удалось сохранить отформатированный файл, используем оригинальный
                    df.to_excel(output_path, index=False)
            else:
                # Сохраняем без форматирования
                df.to_excel(output_path, index=False)
        except Exception:
            # Сохраняем без форматирования
            df.to_excel(output_path, index=False)
        
        # Удаляем временный файл
        try:
            os.remove(temp_output)
        except Exception:
            pass
        
        print(f"Создан новый Excel-файл: {output_path}")
        return output_path, True  # Файл был создан

def upload_to_yandex_disk(file_path, force_update=True):
    """Загружает Excel файл на Яндекс.Диск"""
    try:
        # Проверяем, что загружаем только Excel файлы
        if not file_path.endswith(('.xlsx', '.xls')):
            print(f"⚠️ Файл {file_path} не является Excel файлом, пропускаем загрузку")
            return None
            
        # Проверяем наличие токена
        if not YANDEX_DISK_TOKEN:
            print("❌ Токен Яндекс.Диска не найден в переменной окружения YANDEX_DISK_TOKEN")
            return None
            
        # Создаем клиент Яндекс.Диска
        try:
            disk = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)
            
            # Проверяем валидность токена
            if not disk.check_token():
                print("❌ Токен Яндекс.Диска недействителен")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка при создании клиента Яндекс.Диска: {e}")
            return None
        
        # Название файла на Яндекс.Диске
        file_name = os.path.basename(file_path)
        remote_path = f"{YANDEX_DISK_FOLDER_PATH.rstrip('/')}/{file_name}"
        
        # Проверяем, существует ли файл на Яндекс.Диске
        file_exists = False
        try:
            disk.get_meta(remote_path)
            file_exists = True
        except yadisk.exceptions.NotFoundError:
            file_exists = False
        except Exception as e:
            print(f"⚠️ Ошибка при проверке существования файла: {e}")
        
        if file_exists and not force_update:
            print(f"📄 Файл '{file_name}' уже существует на Яндекс.Диске")
            return f"https://disk.yandex.ru/client/disk{remote_path}"
        
        # Загружаем файл на Яндекс.Диск
        try:
            if file_exists:
                print(f"🔄 Обновляем файл '{file_name}' на Яндекс.Диске...")
            else:
                print(f"📤 Загружаем новый файл '{file_name}' на Яндекс.Диск...")
            
            disk.upload(file_path, remote_path, overwrite=True)
            print(f"✅ Файл '{file_name}' успешно загружен на Яндекс.Диск")
            
            # Публикуем файл для получения ссылки
            try:
                disk.publish(remote_path)
                meta = disk.get_meta(remote_path)
                public_url = meta.public_url
                print(f"🔗 Файл '{file_name}' доступен по ссылке")
                return public_url
            except Exception as e:
                print(f"⚠️ Файл загружен, но не удалось сделать его публичным: {e}")
                return f"https://disk.yandex.ru/client/disk{remote_path}"
            
        except Exception as e:
            print(f"❌ Ошибка при загрузке файла на Яндекс.Диск: {e}")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла на Яндекс.Диск: {e}")
        return None

def sync_excel_from_yandex_disk():
    """Скачивание актуальной версии Excel-файла с Яндекс.Диска и объединение с локальными изменениями"""
    try:
        # Проверяем наличие токена
        if not YANDEX_DISK_TOKEN:
            print("❌ Токен Яндекс.Диска не найден в переменной окружения YANDEX_DISK_TOKEN")
            return False
            
        # Сохраняем локальные данные перед скачиванием, если файл существует
        local_data = None
        if os.path.exists(OUTPUT_EXCEL_PATH):
            try:
                local_data = pd.read_excel(OUTPUT_EXCEL_PATH)
                print(f"💾 Сохранены локальные данные: {len(local_data)} строк")
            except Exception as e:
                print(f"⚠️ Ошибка при чтении локального файла: {e}")
        
        # Создаем клиент Яндекс.Диска
        try:
            disk = yadisk.YaDisk(token=YANDEX_DISK_TOKEN)
            
            # Проверяем валидность токена
            if not disk.check_token():
                print("❌ Токен Яндекс.Диска недействителен")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка при создании клиента Яндекс.Диска: {e}")
            return False
        
        # Название файла на Яндекс.Диске
        file_name = os.path.basename(OUTPUT_EXCEL_PATH)
        remote_path = f"{YANDEX_DISK_FOLDER_PATH.rstrip('/')}/{file_name}"
        
        # Проверяем, существует ли файл на Яндекс.Диске
        try:
            disk.get_meta(remote_path)
        except yadisk.exceptions.NotFoundError:
            print(f"📁 Файл {file_name} не найден на Яндекс.Диске")
            # Если файла нет на Яндекс.Диске, используем локальные данные
            return local_data is not None
        except Exception as e:
            print(f"❌ Ошибка при проверке файла на Яндекс.Диске: {e}")
            return False
        
        # Создаем резервную копию локального файла перед скачиванием
        if os.path.exists(OUTPUT_EXCEL_PATH):
            backup_path = f"{OUTPUT_EXCEL_PATH}.local_backup"
            try:
                shutil.copy2(OUTPUT_EXCEL_PATH, backup_path)
                print(f"💾 Создана резервная копия локального файла: {backup_path}")
            except Exception as e:
                print(f"⚠️ Не удалось создать резервную копию локального файла: {e}")
        
        # Скачивание файла с Яндекс.Диска во временный файл
        temp_yadisk_path = f"{OUTPUT_EXCEL_PATH}.yadisk_temp"
        
        try:
            print("⬇️ Скачивание файла с Яндекс.Диска...")
            disk.download(remote_path, temp_yadisk_path)
            print(f"✅ Файл {file_name} успешно скачан с Яндекс.Диска")
        except Exception as e:
            print(f"❌ Ошибка при скачивании файла с Яндекс.Диска: {e}")
            return False
        
        # Загружаем данные из скачанного файла
        yadisk_data = None
        try:
            yadisk_data = pd.read_excel(temp_yadisk_path)
            print(f"📊 Загружены данные из Яндекс.Диска: {len(yadisk_data)} строк")
        except Exception as e:
            print(f"❌ Ошибка при чтении файла с Яндекс.Диска: {e}")
            # Удаляем временный файл
            if os.path.exists(temp_yadisk_path):
                os.remove(temp_yadisk_path)
            return False
        
        # Объединяем локальные данные с данными из Яндекс.Диска
        merged_data = merge_with_yadisk_changes(local_data, yadisk_data)
        
        # Сохраняем объединенные данные в основной файл
        if merged_data is not None:
            merged_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
            print(f"💾 Сохранены объединенные данные в {OUTPUT_EXCEL_PATH}: {len(merged_data)} строк")
        
        # Удаляем временный файл
        if os.path.exists(temp_yadisk_path):
            os.remove(temp_yadisk_path)
            
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при синхронизации с Яндекс.Диском: {e}")
        import traceback
        traceback.print_exc()
        return False

def clean_uniqualized_images_folder():
    """Очищает папку uniqualized_images после загрузки всех изображений"""
    unique_images_dir = LOCAL_UNIQUE_IMAGES_DIR  # Используем константу из настроек
    
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
    """Очищает папку processed_images после загрузки всех изображений"""
    processed_images_dir = LOCAL_IMAGES_DIR  # Используем константу из настроек
    
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
    Создает дубли для каждой строки с изменением ID и адреса для каждого города из списка CITY_LIST
    
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
    gdrive_service = create_drive_service()
    if gdrive_service:
        print("Google Drive API инициализирован для загрузки уникализированных изображений.")
    else:
        print("Ошибка при инициализации Google Drive API")
        print("Уникализированные изображения будут сохранены локально.")
    
    # Создаем директорию для уникализированных изображений
    unique_images_dir = LOCAL_UNIQUE_IMAGES_DIR
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
        # Оригинал: product_images -> upload на Яндекс.Диск (original), ссылки + shop_image_urls
        original_row = row.to_dict()
        if original_row.get('Address') == 'Тула, улица Волнянского, 1':
            original_row['Delivery'] = 'ПВЗ'
        else:
            original_row['Delivery'] = ''
        # Получаем product_images
        original_image_urls = []
        if 'ImageUrls' in row and row['ImageUrls'] and pd.notna(row['ImageUrls']):
            original_image_urls = row['ImageUrls'].split('|')
        orig_yadisk_links = []
        for j, img_url in enumerate(original_image_urls):
            orig_filename = f"{original_row['Id']}_original_{j+1}.jpg"
            orig_path = os.path.join(LOCAL_UNIQUE_IMAGES_DIR, orig_filename)
            try:
                response = requests.get(img_url, timeout=30)
                if response.status_code == 200:
                    with open(orig_path, 'wb') as f:
                        f.write(response.content)
                    links = upload_images_to_yandex_disk([orig_path], remote_folder='/avito_images/')
                    if links and links[0]:
                        orig_yadisk_links.append(links[0])
            except Exception as e:
                print(f"[ORIG ERROR] Не удалось скачать/залить {img_url}: {e}")
        orig_yadisk_links.extend(shop_image_urls)
        original_row['ImageUrls'] = "|".join(orig_yadisk_links)
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
        
        # Создаем дубли для каждого города из списка CITY_LIST
        for city_index, city in enumerate(CITY_LIST):
            # Создаем копию строки
            duplicate = row.to_dict()
            
            # Изменяем ID (добавляем -1, -2, и т.д.)
            duplicate['Id'] = f"{original_id}-{city_index + 1}"
            
            # Изменяем адрес на город из списка
            duplicate['Address'] = city
            
            # Получаем индекс города для уникализации изображений (теперь city_index уже доступен)
            # city_index = CITY_LIST.index(city)  # Эта строка больше не нужна
            
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
                unique_image_urls = []
                num_shop_images = len(shop_image_urls)
                if num_shop_images > 0 and len(original_image_urls) >= num_shop_images:
                    product_images = original_image_urls[:-num_shop_images]
                else:
                    product_images = original_image_urls.copy()
                # Для каждого города делаем уникальные derived-фото (ID, город, номер)
                print(f"Для товара {duplicate['Id']}: найдено {len(product_images)} изображений продукта и {num_shop_images} shop-изображений (будут заменены)")
                for j, img_url in enumerate(product_images):
                    derived_filename = f"{original_id}_derived_{city_index+1}_{j+1}.jpg"
                    derived_path = os.path.join(unique_images_dir, derived_filename)
                    try:
                        response = requests.get(img_url, timeout=30)
                        if response.status_code == 200:
                            with open(derived_path, 'wb') as f:
                                f.write(response.content)
                            links = upload_images_to_yandex_disk([derived_path], remote_folder='/avito_images/')
                            if links and links[0]:
                                unique_image_urls.append(links[0])
                            else:
                                unique_image_urls.append(img_url)
                        else:
                            unique_image_urls.append(img_url)
                    except Exception as e:
                        print(f"[DERIVED ERROR] Не удалось скачать/залить {img_url}: {e}")
                        unique_image_urls.append(img_url)
                unique_image_urls.extend(shop_image_urls)
                if len(unique_image_urls) < 10 and shop_image_urls:
                    missing = 10 - len(unique_image_urls)
                    for k in range(missing):
                        unique_image_urls.append(shop_image_urls[k % len(shop_image_urls)])
                if len(unique_image_urls) > 10:
                    unique_image_urls = unique_image_urls[:10]
                duplicate['ImageUrls'] = "|".join(unique_image_urls)
                print(f"Для товара {duplicate['Id']}: обновлены URL изображений (derived+shop, всего {len(unique_image_urls)})")
            
            # Добавляем дубль в список всех строк
            all_rows.append(duplicate)
    
    # Создаем новый DataFrame из всех строк
    result_df = pd.DataFrame(all_rows)
    
    # НЕ очищаем папку с уникализированными изображениями здесь!
    # Очистка будет происходить после сохранения данных в Excel
    # clean_uniqualized_images_folder()
    
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
    Обработка XML-файла и создание Excel-таблицы с сохранением пользовательских изменений из Яндекс.Диска.
    
    Логика работы:
    1. Синхронизация с Яндекс.Диском с сохранением всех пользовательских изменений
    2. Обновление только полей Price, Brand, ImageUrls из XML
    3. Добавление новых позиций из XML
    4. Удаление позиций, отсутствующих в XML
    """
    # Синхронизация с Яндекс.Диском (теперь с сохранением пользовательских изменений)
    sync_excel_from_yandex_disk()
    
    # Создание директории для изображений
    output_dir = create_output_dir()
    
    # Создание директории для оригинальных товаров (в uniqualized_images)
    original_products_dir = LOCAL_UNIQUE_IMAGES_DIR
    os.makedirs(original_products_dir, exist_ok=True)
    
    # Инициализация Google Drive API для изображений (только для изображений, не для Excel)
    # gdrive_service = None
    # if use_gdrive_for_images:
    #     gdrive_service = create_drive_service()
    
    # Сначала пробуем загрузить ссылки на изображения магазина из кэша
    shop_image_urls = load_shop_images_cache()
    
    # Если не удалось загрузить из кэша, загружаем shop-изображения на Яндекс.Диск и кэшируем публичные ссылки
    if shop_image_urls is None and SHOP_IMAGES:
        shop_image_urls = []
        # Загружаем shop-изображения на Яндекс.Диск и получаем публичные ссылки
        try:
            public_links = upload_images_to_yandex_disk(SHOP_IMAGES, remote_folder='/avito_images/')
            for link in public_links:
                if link:
                    shop_image_urls.append(link)
            print(f"Загружено shop-изображений на Яндекс.Диск: {len(shop_image_urls)}")
        except Exception as e:
            print(f"Ошибка загрузки shop-изображений на Яндекс.Диск: {e}")
        # Сохраняем публичные ссылки в кэш
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
            
            # Очищаем "осиротевшие" производные товары
            existing_data, was_cleaned = clean_orphaned_derived_products(existing_data)
            if was_cleaned:
                # Сохраняем очищенные данные
                existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
                
                # Загружаем очищенный файл на Яндекс.Диск
                file_url = upload_to_yandex_disk(OUTPUT_EXCEL_PATH, force_update=True)
            
            if 'Id' in existing_data.columns:
                # Получаем список существующих Id
                existing_ids = set(existing_data['Id'].astype(str).tolist())
                
                # Проверяем наличие изображений в существующих товарах
                if 'ImageUrls' in existing_data.columns:
                    for index, row in existing_data.iterrows():
                        product_id = str(row['Id'])
                        image_urls = str(row['ImageUrls']) if pd.notna(row['ImageUrls']) else ""
                        
                        # Если у товара нет изображений, добавляем его в список для обработки
                        if not image_urls or image_urls == "nan" or image_urls.strip() == "":
                            existing_products_with_missing_images[product_id] = index
        except Exception as e:
            pass  # Убираем детальное логирование ошибок
    
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
            # Собираем все ID, которые нужно удалить (базовые и их производные)
            all_ids_to_remove = set()
            
            for removed_id in removed_base_ids:
                # Добавляем базовый ID
                all_ids_to_remove.add(removed_id)
                
                # Добавляем все производные ID этого базового ID
                if removed_id in derived_excel_ids:
                    all_ids_to_remove.update(derived_excel_ids[removed_id])
            
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
                            all_ids_to_remove.update(similar_ids)
            
            # Удаляем все связанные записи из DataFrame
            existing_data = existing_data[~existing_data['Id'].astype(str).isin(all_ids_to_remove)]
        
        # Сохраняем обновленную таблицу
        existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
        
        # Загружаем обновленную таблицу на Яндекс.Диск
        file_url = upload_to_yandex_disk(OUTPUT_EXCEL_PATH, force_update=True)
    
    # Ищем и удаляем нежелательный текст в описаниях (без логирования)
    unwanted_suffix = "</p><p>__________________________<br />Режим работы : 9.00-19.00<br />Отправляем всеми ТК СДЕК BOXBERRY Яндекс Почта России DPD Авито <br />Максимально упаковываем товар перед отправкой</p>"
    for ad in root.findall("Ad"):
        description = ad.find("Description")
        if description is not None and description.text:
            if description.text.endswith(unwanted_suffix):
                description.text = description.text[:-len(unwanted_suffix)]
            elif "</p><p>__________________________<br />" in description.text:
                # Находим начало нежелательного текста
                start_idx = description.text.find("</p><p>__________________________<br />")
                if start_idx != -1:
                    # Удаляем весь текст с этого места до конца
                    description.text = description.text[:start_idx] + "</p>"

    # ВРЕМЕННО: ограничиваем обработку только 10 объявлений для теста (берём любые первые 10)
    total_ads = 0
    ads_to_process = []
    for ad in root.findall("Ad"):
        ads_to_process.append(ad)
        total_ads += 1
    
    # Обновляем поля Price и Brand для существующих записей (без детального логирования)
    price_brand_updated = False
    if existing_data is not None and len(existing_data) > 0:
        try:
            price_brand_updated = update_existing_records(existing_data, root)
            if price_brand_updated:
                # Сохраняем обновленный DataFrame
                existing_data.to_excel(OUTPUT_EXCEL_PATH, index=False)
                # Загружаем обновленный файл на Яндекс.Диск
                file_url = upload_to_yandex_disk(OUTPUT_EXCEL_PATH, force_update=True)
        except Exception as e:
            pass
    
    # Получаем все возможные параметры из XML
    all_parameters = set()
    for ad in ads_to_process:
        for elem in ad:
            all_parameters.add(elem.tag)
    
    print(f"Найдено {len(all_parameters)} уникальных параметров в XML")

    # Список стандартных параметров, которые всегда должны быть
    standard_parameters = [
        "Id", "AdType", "Category", "Address", "ContactPhone", 
        "GoodsType", "ProductType", "SparePartType", "Title", 
        "Description", "Price", "Availability", "Condition", "Brand", "OEM",
        "TechnicSparePartType", "TransmissionSparePartType", "EngineSparePartType",
        "Delivery", "CompatibleCars"
    ]

    # Добавляем стандартные параметры, которых может не быть в XML
    for param in standard_parameters:
        all_parameters.add(param)

    # Добавляем кастомные столбцы
    all_parameters.update(["InternetCalls", "CallsDevices", "ImageUrls"])
    all_parameters = sorted(list(all_parameters))
    
    # Инициализация переменных для обработки
    data = []
    processed_count = 0
    skipped_count = 0
    processed_images_dict = {}
    
    # Обработка каждого объявления с ограничением
    for ad in ads_to_process:
        ad_id_elem = ad.find("Id")
        
        # Проверяем наличие элемента Id
        if ad_id_elem is None or ad_id_elem.text is None:
            continue
            
        ad_id = ad_id_elem.text
        
        # Проверяем, является ли этот товар существующим товаром без изображений
        if ad_id in existing_products_with_missing_images:
            # Обработка изображений для существующих товаров (используем папку uniqualized_images)
            processed_images = process_images_for_original_products(ad, original_products_dir, ad_id)
            processed_images_dict[ad_id] = processed_images
            if processed_images:
                # Добавляем ссылки на изображения магазина, если они есть
                all_images = list(processed_images)
                if shop_image_urls:
                    # Проверяем, сколько ещё можно добавить изображений (максимум 10)
                    remaining_slots = 10 - len(all_images)
                    if remaining_slots > 0:
                        # Добавляем столько изображений магазина, сколько поместится
                        shop_images_to_add = min(remaining_slots, len(shop_image_urls))
                        all_images.extend(shop_image_urls[:shop_images_to_add])

                # Фильтруем только Яндекс.Диск-ссылки и shop_image_urls
                def is_yadisk_link(img):
                    s = str(img)
                    if s.startswith('https://disk.yandex.ru') or s.startswith('https://yadi.sk'):
                        return True
                    if img in (shop_image_urls or []):
                        return True
                    return False
                all_images = [img for img in all_images if is_yadisk_link(img)]
                # Добавляем shop_image_urls, если их нет среди all_images
                if shop_image_urls:
                    for shop_img in shop_image_urls:
                        if shop_img not in all_images:
                            all_images.append(shop_img)

                # Формируем строку со всеми URL изображений, разделенными |
                image_urls_string = "|".join(all_images)

                # Обновляем запись в существующем DataFrame
                row_index = existing_products_with_missing_images[ad_id]
                existing_data.at[row_index, 'ImageUrls'] = image_urls_string

                # Если есть секция Images, заменяем её в XML
                images_element = ad.find("Images")
                if images_element is not None:
                    # Удаляем существующие изображения
                    for img in images_element.findall("Image"):
                        images_element.remove(img)
                    # Добавляем новые изображения в XML
                    for i, img_path in enumerate(all_images):
                        img_url = img_path if isinstance(img_path, str) else img_path[0]
                        img_elem = ET.SubElement(images_element, "Image")
                        img_elem.text = img_url
                        img_elem.set("url", img_url)
            continue
        
        # Обрабатываем только товары начинающиеся с "bz" и с ограничением на количество
        if MAX_ITEMS is not None and processed_count >= MAX_ITEMS:
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
            continue
        
        # НОВАЯ ЛОГИКА: Пропускаем товары, которые уже существуют в таблице (даже без производных записей)
        # Обрабатываем изображения только для полностью новых товаров
        if is_existing_product:
            skipped_count += 1
            print(f"⏭️  Пропущен существующий товар: {ad_id}")
            continue
        
        processed_count += 1
        
        # МИНИМАЛЬНОЕ ЛОГИРОВАНИЕ - только ID товара и процент
        percentage = (processed_count / (MAX_ITEMS if MAX_ITEMS else total_ads)) * 100
        print(f"🔄 {ad_id} - {percentage:.1f}%")
        
        # Замена описания (без детального логирования)
        description = ad.find("Description")
        if description is not None and description.text:
            # Проверяем, содержит ли текст CDATA
            if "<![CDATA[" in description.text and "]]>" in description.text:
                # Извлекаем содержимое CDATA
                cdata_start = description.text.find("<![CDATA[") + 9
                cdata_end = description.text.rfind("]]>")
                cdata_content = description.text[cdata_start:cdata_end]
                
                # Ищем маркер "Lada;"
                lada_index = cdata_content.find("Lada;")
                if lada_index != -1:
                    # Всегда вставляем описание сразу после "Lada;"
                    new_cdata_content = cdata_content[:lada_index + 5] + NEW_DESCRIPTION + cdata_content[lada_index + 5:]
                    description.text = f"<![CDATA[{new_cdata_content}]]>"
                else:
                    # Если нет "Lada;", ищем последний </p><p>
                    last_p_tag = cdata_content.rfind("</p><p>")
                    if last_p_tag != -1:
                        # Вставляем после последнего тега </p><p>
                        tag_end = last_p_tag + len("</p><p>")
                        new_cdata_content = cdata_content[:tag_end] + NEW_DESCRIPTION + cdata_content[tag_end:]
                        description.text = f"<![CDATA[{new_cdata_content}]]>"
                    else:
                        # Если нет тегов, вставляем в конец
                        description.text = f"<![CDATA[{cdata_content}{NEW_DESCRIPTION}]]>"
            else:
                # Если нет CDATA, просто добавляем описание в конец
                lada_index = description.text.find("Lada;")
                if lada_index != -1:
                    # Вставляем описание сразу после "Lada;"
                    # Уже закодированное в исходном файле описание
                    description.text = description.text[:lada_index + 5] + NEW_DESCRIPTION + description.text[lada_index + 5:]
                else:
                    # Если нет "Lada;", добавляем в конец
                    description.text = description.text + NEW_DESCRIPTION
        
        # Обработка изображений
        processed_images = process_images_for_original_products(ad, original_products_dir, ad_id)
        processed_images_dict[ad_id] = processed_images
        
        # ГАРАНТИЯ: В ImageUrls только Яндекс.Диск-ссылки (disk.yandex.ru, yadi.sk) и shop_image_urls
        def is_yadisk_link(img):
            s = str(img)
            if s.startswith('https://disk.yandex.ru') or s.startswith('https://yadi.sk'):
                return True
            if img in (shop_image_urls or []):
                return True
            return False

        # Оставляем только Яндекс.Диск-ссылки и shop_image_urls
        all_images = [img for img in processed_images if is_yadisk_link(img)]
        # Добавляем shop_image_urls, если их нет среди all_images
        if shop_image_urls:
            for shop_img in shop_image_urls:
                if shop_img not in all_images:
                    all_images.append(shop_img)

        # Если есть секция Images, заменяем её в XML
        if all_images:
            images_element = ad.find("Images")
            if images_element is not None:
                # Удаляем существующие изображения
                for img in images_element.findall("Image"):
                    images_element.remove(img)
                # Добавляем новые изображения в XML
                for i, img_path in enumerate(all_images):
                    img_url = img_path if isinstance(img_path, str) else img_path[0]
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
        
        # Специальная обработка CompatibleCars
        compatible_cars_data = []
        compatible_cars_elem = ad.find("CompatibleCars")
        if compatible_cars_elem is not None:
            for compatible_car in compatible_cars_elem.findall("CompatibleCar"):
                make_elem = compatible_car.find("Make")
                model_elem = compatible_car.find("Model") 
                generation_elem = compatible_car.find("Generation")
                
                make = make_elem.text if make_elem is not None and make_elem.text else ""
                model = model_elem.text if model_elem is not None and model_elem.text else ""
                generation = generation_elem.text if generation_elem is not None and generation_elem.text else ""
                
                if make or model or generation:  # Если есть хотя бы одно значение
                    compatible_cars_data.append(f"{make}|{model}|{generation}")
        
        # Формируем строку для CompatibleCars, разделенную символом |
        row_data["CompatibleCars"] = "|".join(compatible_cars_data) if compatible_cars_data else ""
        
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
        
        # Загружаем файл на Яндекс.Диск
        file_url = upload_to_yandex_disk(OUTPUT_EXCEL_PATH, force_update=True)
        print(f"Обновленная таблица загружена на Яндекс.Диск")
        
        if not data:  # Если нет новых товаров для добавления
            return pd.DataFrame(), file_url
    
    if not data:
        print("Нет новых товаров для добавления")
        
        # Возвращаем существующую ссылку если нет новых товаров
        file_url = None
        if os.path.exists(OUTPUT_EXCEL_PATH):
            # Проверяем, есть ли файл на Яндекс.Диске
            try:
                # Инициализируем Google Drive API с OAuth
                drive_service = create_drive_service()
                if not drive_service:
                    print("Ошибка инициализации Drive сервиса")
                    return pd.DataFrame(), None
                
                # Название файла в Google Drive
                file_name = os.path.basename(OUTPUT_EXCEL_PATH)
                
                # Создаем запрос для поиска файла
                query = f"name='{file_name}' and trashed=false"
                
                # Google Drive больше не используется
                
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
    
    # Загружаем файл на Яндекс.Диск только если он был обновлен
    if was_updated:
        file_url = upload_to_yandex_disk(excel_path, force_update=True)
        print(f"Таблица обновлена и загружена на Яндекс.Диск")
    else:
        # Если файл не был обновлен, получаем существующую ссылку
        file_url = upload_to_yandex_disk(excel_path, force_update=False)
        print(f"Таблица не изменилась, используем существующую ссылку")
    
    # Очищаем папку с обработанными изображениями после завершения всех операций
    clean_processed_images_folder()
    
    # НЕ очищаем папку с уникализированными изображениями - они должны сохраняться!
    # clean_uniqualized_images_folder()
    
    # Выводим итоговую статистику обработки
    print("=" * 50)
    print("📊 ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ:")
    print(f"📄 Всего товаров в XML: {total_ads}")
    print(f"🆕 Новых товаров обработано: {processed_count}")
    print(f"⏭️  Существующих товаров пропущено: {skipped_count}")
    print(f"🖼️  Товаров с добавленными изображениями: {len(existing_products_with_missing_images)}")
    print(f"📊 Итоговый размер таблицы: {len(final_df)} строк")
    print("=" * 50)
    
    return final_df, file_url

def process_xml_with_yandex_disk():
    """
    Обрабатывает XML-файл с синхронизацией с Яндекс.Диском.
    Возвращает: (DataFrame, file_url)
    """
    try:
        # Сначала скачиваем актуальную версию Excel с Яндекс.Диска
        sync_success = sync_excel_from_yandex_disk()
        if not sync_success:
            print("⚠️ Предупреждение: проблемы с синхронизацией с Яндекс.Диском")
        
        # Обрабатываем XML-файл
        df = process_xml()
        
        # Загружаем обновленный файл на Яндекс.Диск
        file_url = upload_to_yandex_disk(OUTPUT_EXCEL_PATH, force_update=True)
        
        return df, file_url
        
    except Exception as e:
        print(f"Ошибка при обработке XML с Яндекс.Диском: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def job():
    """Основная функция для запуска процесса обработки"""
    print(f"🚀 Начало обработки: {datetime.now()}")
    
    # Сначала скачиваем актуальную версию Excel
    print("📥 Синхронизация с Яндекс.Диском...")
    sync_success = sync_excel_from_yandex_disk()
    if sync_success:
        print("✅ Синхронизация с Яндекс.Диском завершена успешно")
    else:
        print("⚠️ Синхронизация с Яндекс.Диском завершена с предупреждениями")
    
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
        df, file_url = process_xml_with_yandex_disk()
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
    """Проверяет доступ к папке на Google Drive"""
    try:
        # Пытаемся получить информацию о папке
        folder = drive_service.files().get(fileId=folder_id).execute()
        return True
    except Exception as e:
        print(f"❌ Ошибка доступа к папке {folder_id}: {e}")
        return False

def main():
    """Основная функция для запуска скрипта (для обратной совместимости)"""
    # Проверяем доступ к папке Google Drive, если указан ID
    # Google Drive больше не используется
    
    # Проверяем консистентность товаров в Excel перед началом работы
    if os.path.exists(OUTPUT_EXCEL_PATH):
        print("Проверка консистентности товаров в Excel перед началом работы...")
        check_excel_consistency()
    
    # Запускаем обработку однократно
    job()

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

def upload_image_to_yandex_disk(local_path, remote_folder='/avito_images/'):
    """
    Загружает одно изображение на Яндекс.Диск и возвращает публичную ссылку.
    Если файл уже есть на Яндекс.Диске, используется существующая публичная ссылка.
    """
    file_name = os.path.basename(local_path)
    remote_path = f"{remote_folder.rstrip('/')}/{file_name}"
    try:
        # Проверяем, существует ли файл на Яндекс.Диске
        file_exists = False
        try:
            meta = disk.get_meta(remote_path)
            file_exists = True
        except Exception:
            file_exists = False
        if file_exists:
            # Если файл есть, получаем публичную ссылку (публикуем, если нужно)
            try:
                if not meta.is_public:
                    disk.publish(remote_path)
                    meta = disk.get_meta(remote_path)
                return meta.public_url
            except Exception as e:
                print(f"Ошибка публикации {remote_path}: {e}")
        # Если файла нет, загружаем и публикуем
        disk.upload(local_path, remote_path, overwrite=True)
        disk.publish(remote_path)
        meta = disk.get_meta(remote_path)
        return meta.public_url
    except Exception as e:
        print(f"Ошибка загрузки изображения на Яндекс.Диск: {e}")
        return None
    
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
            
            # Загружаем на Яндекс.Диск
            file_url = upload_to_yandex_disk(excel_file_path, force_update=True)
            if file_url:
                print(f"Обновленный Excel-файл загружен на Яндекс.Диск: {file_url}")
            
            return True
        else:
            print("В Excel-файле нет 'осиротевших' производных товаров")
            return False
            
    except Exception as e:
        print(f"Ошибка при проверке консистентности Excel-файла: {e}")
        return False


def update_existing_records(existing_data, xml_root):
    """
    Обновляет поля Price, Brand и CompatibleCars для существующих записей на основе данных из XML
    
    existing_data: DataFrame с существующими данными
    xml_root: корневой элемент XML дерева
    
    Возвращает: обновленный DataFrame и флаг изменений
    """
    if existing_data is None or existing_data.empty:
        return existing_data, False
    
    print("Обновление полей Price, Brand и CompatibleCars для существующих товаров...")
    
    
    # Создаем словарь для быстрого поиска данных из XML
    xml_data = {}
    for ad in xml_root.findall("Ad"):
        ad_id_elem = ad.find("Id")
        if ad_id_elem is not None and ad_id_elem.text is not None:
            ad_id = ad_id_elem.text
            
            # Извлекаем Price и Brand из XML
            price_elem = ad.find("Price")
            brand_elem = ad.find("Brand")
            
            # Извлекаем CompatibleCars
            compatible_cars_data = []
            compatible_cars_elem = ad.find("CompatibleCars")
            if compatible_cars_elem is not None:
                for compatible_car in compatible_cars_elem.findall("CompatibleCar"):
                    make_elem = compatible_car.find("Make")
                    model_elem = compatible_car.find("Model")
                    generation_elem = compatible_car.find("Generation")
                    
                    make = make_elem.text if make_elem is not None and make_elem.text else ""
                    model = model_elem.text if model_elem is not None and model_elem.text else ""
                    generation = generation_elem.text if generation_elem is not None and generation_elem.text else ""
                    
                    if make or model or generation:
                        compatible_cars_data.append(f"{make}|{model}|{generation}")
            
            xml_data[ad_id] = {
                'Price': price_elem.text if price_elem is not None and price_elem.text else "",
                'Brand': brand_elem.text if brand_elem is not None and brand_elem.text else "",
                'CompatibleCars': "|".join(compatible_cars_data) if compatible_cars_data else ""
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
            
            # Обновляем CompatibleCars если есть данные
            if 'CompatibleCars' in existing_data.columns:
                current_compatible_cars = str(row['CompatibleCars']) if pd.notna(row['CompatibleCars']) else ""
                if current_compatible_cars != xml_record['CompatibleCars']:
                    existing_data.at[index, 'CompatibleCars'] = xml_record['CompatibleCars']
                    changes_made = True
                    row_updated = True
                    print(f"Обновлены совместимые автомобили для товара {row_id}: {xml_record['CompatibleCars']}")
            
            # Увеличиваем счетчик, если запись была обновлена
            if row_updated:
                updated_count += 1
    
    if changes_made:
        print(f"Обновлено {updated_count} записей с новыми данными Price, Brand и/или CompatibleCars")
    else:
        print("Все поля Price, Brand и CompatibleCars актуальны, обновлений не требуется")
    
    return existing_data, changes_made

def merge_with_yadisk_changes(local_data, yadisk_data):
    """
    Объединяет локальные данные с изменениями из Яндекс.Диска.
    Сохраняет все ручные изменения пользователя, обновляя только определенные поля из XML.
    
    local_data: DataFrame с локальными данными (может быть None)
    yadisk_data: DataFrame с данными из Яндекс.Диска
    
    Возвращает: объединенный DataFrame с сохраненными пользовательскими изменениями
    """
    if yadisk_data is None or yadisk_data.empty:
        print("📄 Данные из Яндекс.Диска пусты, используются локальные данные")
        return local_data
    
    if local_data is None or local_data.empty:
        print("📄 Локальные данные отсутствуют, используются данные из Яндекс.Диска")
        return yadisk_data
    
    print("🔄 Объединение локальных данных с изменениями из Яндекс.Диска...")
    
    # Поля, которые могут обновляться из XML (остальные сохраняются из Яндекс.Диска)
    xml_updatable_fields = {'Price', 'Brand', 'ImageUrls', 'CompatibleCars'}
    
    # Поля, которые всегда сохраняются из Яндекс.Диска (пользовательские изменения)
    user_editable_fields = set(yadisk_data.columns) - xml_updatable_fields - {'Id'}
    
    print(f"📊 Поля, обновляемые из XML: {xml_updatable_fields}")
    print(f"🔒 Поля, сохраняемые из Яндекс.Диска: {user_editable_fields}")
    
    # Создаем копию данных из Яндекс.Диска как основу
    merged_data = yadisk_data.copy()
    
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
    
    # Добавляем новые записи из локальных данных, которых нет в Яндекс.Диске
    yadisk_ids = set(str(id_val) for id_val in yadisk_data['Id'].astype(str).tolist()) if 'Id' in yadisk_data.columns else set()
    local_ids = set(str(id_val) for id_val in local_data['Id'].astype(str).tolist()) if 'Id' in local_data.columns else set()
    
    new_ids = local_ids - yadisk_ids
    
    if new_ids:
        print(f"📥 Добавление {len(new_ids)} новых записей из локальных данных")
        new_rows = []
        for new_id in new_ids:
            if new_id in local_dict:
                new_rows.append(local_dict[new_id])
        
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            merged_data = pd.concat([merged_data, new_df], ignore_index=True)
    
    print(f"✅ Объединение завершено. Обновлено записей: {updated_count}, добавлено новых: {len(new_ids) if new_ids else 0}")
    
    return merged_data

def authenticate_google_service_account():
    """
    Заглушка для совместимости - Google Drive больше не используется
    """
    print("❌ Ошибка при аутентификации: Google Drive больше не используется, используйте Яндекс.Диск")
    return None

def create_drive_service():
    """
    Заглушка для совместимости - Google Drive больше не используется
    """
    print("❌ Ошибка при инициализации Google Drive API")
    print("Уникализированные изображения будут сохранены локально.")
    return None

def get_service_account_email():
    """
    Заглушка для совместимости - Google Drive больше не используется
    """
    return 'не используется'

def uniqualize_image(input_image_path_or_url, output_path, city_index):
    """
    Создает уникализированную версию изображения, изменяя метаданные и визуальные параметры
    
    input_image_path_or_url: путь к исходному изображению или URL
    output_path: путь для сохранения уникализированного изображения
    city_index: индекс города, используется для вариации параметров
    
    Возвращает: путь к уникализированному изображению
    """
    import os
    try:
        is_url = input_image_path_or_url.startswith('http')
        if is_url:
            response = requests.get(input_image_path_or_url)
            if response.status_code != 200:
                return None
            img = PILImage.open(BytesIO(response.content))
        else:
            img = PILImage.open(input_image_path_or_url)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        contrast_factor = 1.0 + (city_index % 3 + 1) * 0.05
        brightness_factor = 1.0 + (city_index % 5 - 2) * 0.02
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(contrast_factor)
        enhancer = ImageEnhance.Brightness(img)
        img = enhancer.enhance(brightness_factor)
        img_array = np.array(img)
        noise_level = (city_index % 4 + 2) * 2
        noise = np.random.normal(0, noise_level, img_array.shape)
        noisy_img_array = np.clip(img_array + noise, 0, 255).astype(np.uint8)
        img = PILImage.fromarray(noisy_img_array)
        if city_index % 3 == 0:
            blur_radius = (city_index % 2) * 0.3 + 0.1
            img = img.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        if city_index % 4 == 0:
            rotation_angle = (city_index % 3 - 1) * 0.5
            img = img.rotate(rotation_angle, resample=PILImage.BICUBIC, expand=False)
        # Сохраняем только как JPEG
        img.save(output_path, format='JPEG', quality=95)
        # Проверка валидности после сохранения
        try:
            with PILImage.open(output_path) as test_img:
                test_img.verify()
        except Exception as e:
            if os.path.exists(output_path):
                os.remove(output_path)
            return None
        # Вставляем EXIF только если файл валидный
        try:
            exif_dict = {'0th': {}, 'Exif': {}, 'GPS': {}, '1st': {}}
            days_shift = city_index % 3 + 1
            creation_date = (datetime.now() - timedelta(days=days_shift)).strftime("%Y:%m:%d %H:%M:%S")
            exif_dict['0th'][piexif.ImageIFD.DateTime] = creation_date
            exif_dict['Exif'][piexif.ExifIFD.DateTimeOriginal] = creation_date
            exif_dict['Exif'][piexif.ExifIFD.DateTimeDigitized] = creation_date
            camera_models = [
                "iPhone 13", "Samsung Galaxy S21", "Google Pixel 6",
                "Xiaomi Mi 11", "Sony Alpha", "Canon EOS R5"
            ]
            camera_model = camera_models[city_index % len(camera_models)]
            exif_dict['0th'][piexif.ImageIFD.Model] = camera_model
            manufacturers = ["Apple", "Samsung", "Google", "Xiaomi", "Sony", "Canon"]
            manufacturer = manufacturers[city_index % len(manufacturers)]
            exif_dict['0th'][piexif.ImageIFD.Make] = manufacturer
            if city_index % 3 == 0:
                city_coords = [
                    (55.7558, 37.6173),
                    (59.9343, 30.3351),
                    (56.8431, 60.6454),
                    (55.0415, 82.9346),
                    (56.3287, 44.0020),
                    (53.1950, 50.1982),
                    (51.5406, 46.0086),
                    (45.0448, 38.9760)
                ]
                base_lat, base_lon = city_coords[city_index % len(city_coords)]
                lat = base_lat + (random.random() - 0.5) * 0.01
                lon = base_lon + (random.random() - 0.5) * 0.01
                def to_deg(value, loc):
                    if value < 0:
                        loc_value = -value
                    else:
                        loc_value = value
                    deg = int(loc_value)
                    d = loc_value - deg
                    min = int(d * 60)
                    sec = int((d - min / 60) * 3600 * 100)
                    return ((deg, 1), (min, 1), (sec, 100))
                try:
                    exif_dict['GPS'][piexif.GPSIFD.GPSVersionID] = (2, 2, 0, 0)
                    exif_dict['GPS'][piexif.GPSIFD.GPSLatitudeRef] = 'N' if lat >= 0 else 'S'
                    exif_dict['GPS'][piexif.GPSIFD.GPSLongitudeRef] = 'E' if lon >= 0 else 'W'
                    exif_dict['GPS'][piexif.GPSIFD.GPSLatitude] = to_deg(abs(lat), 'lat')
                    exif_dict['GPS'][piexif.GPSIFD.GPSLongitude] = to_deg(abs(lon), 'lon')
                except Exception as e:
                    exif_dict['GPS'] = {}
            try:
                exif_bytes = piexif.dump(exif_dict)
                piexif.insert(exif_bytes, output_path)
            except Exception as e:
                try:
                    simple_exif = {'0th': {}, 'Exif': {}, '1st': {}}
                    simple_exif['0th'][piexif.ImageIFD.DateTime] = creation_date
                    simple_exif['Exif'][piexif.ExifIFD.DateTimeOriginal] = creation_date
                    exif_bytes = piexif.dump(simple_exif)
                    piexif.insert(exif_bytes, output_path)
                except Exception as e2:
                    pass
        except Exception as e:
            pass
        # Проверка валидности после вставки EXIF
        try:
            with PILImage.open(output_path) as test_img:
                test_img.verify()
        except Exception as e:
            if os.path.exists(output_path):
                os.remove(output_path)
            return None
        return output_path
    except Exception as e:
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

# Django Management Command
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = 'Обновляет данные из XML, обрабатывает изображения и создает Excel файл'

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-items',
            type=int,
            help='Максимальное количество товаров для обработки',
        )
        parser.add_argument(
            '--skip-sync',
            action='store_true',
            help='Пропустить синхронизацию с Яндекс.Диском',
        )
        parser.add_argument(
            '--check-consistency',
            action='store_true',
            help='Только проверить консистентность Excel файла',
        )

    def handle(self, *args, **options):
        """Основной метод выполнения команды"""
        global MAX_ITEMS
        
        # Проверяем и запускаем Django сервер если нужно
        ensure_django_server_running()
        
        # Устанавливаем максимальное количество товаров если указано
        if options.get('max_items'):
            MAX_ITEMS = options['max_items']
            self.stdout.write(f"Установлено ограничение: {MAX_ITEMS} товаров")
        
        # Если нужно только проверить консистентность
        if options.get('check_consistency'):
            self.stdout.write("🔍 Проверка консистентности товаров в Excel...")
            if os.path.exists(OUTPUT_EXCEL_PATH):
                consistency_changes = check_excel_consistency()
                if consistency_changes:
                    self.stdout.write(self.style.SUCCESS("✅ Консистентность восстановлена, изменения сохранены"))
                else:
                    self.stdout.write(self.style.SUCCESS("✅ Данные консистентны, изменений не требуется"))
            else:
                self.stdout.write(self.style.WARNING("⚠️ Excel файл не найден"))
            return
        
        # Проверяем консистентность товаров в Excel перед началом работы
        if os.path.exists(OUTPUT_EXCEL_PATH):
            self.stdout.write("🔍 Проверка консистентности товаров в Excel перед началом работы...")
            check_excel_consistency()
        
        # Запускаем основную логику обработки
        self.stdout.write(f"🚀 Начало обработки: {datetime.now()}")
        
        # Синхронизация с Яндекс.Диском (если не пропускаем)
        if not options.get('skip_sync'):
            self.stdout.write("📥 Синхронизация с Яндекс.Диском...")
            sync_success = sync_excel_from_yandex_disk()
            if sync_success:
                self.stdout.write(self.style.SUCCESS("✅ Синхронизация с Яндекс.Диском завершена успешно"))
            else:
                self.stdout.write(self.style.WARNING("⚠️ Синхронизация с Яндекс.Диском завершена с предупреждениями"))
        
        # Проверяем консистентность товаров в Excel
        if os.path.exists(OUTPUT_EXCEL_PATH):
            self.stdout.write("🔍 Проверка консистентности товаров в Excel...")
            consistency_changes = check_excel_consistency()
            if consistency_changes:
                self.stdout.write(self.style.SUCCESS("✅ Консистентность восстановлена, изменения сохранены"))
            else:
                self.stdout.write(self.style.SUCCESS("✅ Данные консистентны, изменений не требуется"))
        
        # Загрузка и обработка XML
        self.stdout.write("📄 Загрузка и обработка XML...")
        if download_xml():
            self.stdout.write(self.style.SUCCESS("✅ XML-файл успешно загружен"))
            df, file_url = process_xml_with_yandex_disk()
            self.stdout.write(self.style.SUCCESS("✅ Обработка XML завершена"))
            if file_url:
                self.stdout.write(f"🔗 Ссылка на обработанный документ: {file_url}")
            
            # Дополнительная очистка папки processed_images
            self.stdout.write("🧹 Очистка временных файлов...")
            clean_processed_images_folder()
            self.stdout.write(self.style.SUCCESS("✅ Очистка завершена"))
        else:
            self.stdout.write(self.style.ERROR("❌ Ошибка при загрузке XML-файла"))
            
        self.stdout.write(self.style.SUCCESS(f"🏁 Обработка завершена: {datetime.now()}"))
        self.stdout.write("=" * 50)

# Для обратной совместимости, если кто-то запустит файл напрямую
if __name__ == "__main__":
    # Удаляем бесконечный цикл и schedule, просто запускаем job() один раз
    job()