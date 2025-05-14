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

# Конфигурация
XML_URL = "https://baz-on.ru/export/c4447/32a54/avito-ipkuznetsov.xml"
LOCAL_XML_PATH = "avito-ipkuznetsov.xml"
OUTPUT_EXCEL_PATH = "avito_processed.xlsx"
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

def process_xml(use_gdrive_for_images=True):
    """Обработка XML-файла и создание Excel-таблицы"""
    # Синхронизация с Google Drive
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
                # Формируем строку со всеми URL изображений, разделенными |
                image_urls_string = "|".join(processed_images)
                
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
                    for i, img_path in enumerate(processed_images):
                        # Получаем соответствующий URL
                        img_url = img_path if isinstance(img_path, str) else img_path[0]
                        
                        # Создаём элемент для XML
                        img_elem = ET.SubElement(images_element, "Image")
                        img_elem.text = img_url
                        img_elem.set("url", img_url)
            continue
        
        # Обрабатываем только товары начинающиеся с "bz" и с ограничением на количество
        if not ad_id.startswith("bz") or processed_count >= MAX_ITEMS:
            continue
        
        # Пропускаем уже существующие товары (кроме тех, которые нуждаются в добавлении изображений)
        if ad_id in existing_ids and ad_id not in existing_products_with_missing_images:
            skipped_count += 1
            print(f"Пропуск объявления {ad_id} (уже существует в таблице)")
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
        
        # Обработка изображений
        processed_images = process_images(ad, output_dir, ad_id, gdrive_service)
        processed_images_dict[ad_id] = processed_images
        
        # Если есть секция Images, заменяем её в XML
        if processed_images:
            images_element = ad.find("Images")
            if images_element is not None:
                # Удаляем существующие изображения
                for img in images_element.findall("Image"):
                    images_element.remove(img)
                    
                # Добавляем новые изображения в XML
                for i, img_path in enumerate(processed_images):
                    # Получаем соответствующий URL
                    img_url = img_path if isinstance(img_path, str) else img_path[0]
                    
                    # Создаём элемент для XML
                    img_elem = ET.SubElement(images_element, "Image")
                    img_elem.text = img_url
                    img_elem.set("url", img_url)
        
        # Формируем строку со всеми URL изображений, разделенными |
        image_urls_string = "|".join(processed_images)
        
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
    
    # Создаем Excel-файл без вставки изображений, только ссылки
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

def job():
    """Основная функция для запуска процесса обработки"""
    print(f"Начало обработки: {datetime.now()}")
    
    # Сначала скачиваем актуальную версию Excel
    sync_excel_from_gdrive()
    
    if download_xml():
        df, file_url = process_xml_with_gdrive()
        print(f"Ссылка на обработанный документ: {file_url}")
    print(f"Обработка завершена: {datetime.now()}")

def main():
    """Основная функция для запуска скрипта"""
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
