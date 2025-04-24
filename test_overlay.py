import os
from PIL import Image as PILImage
import requests
from io import BytesIO

def overlay_test():
    """Тест функции наложения изображений с максимальным смещением вниз"""
    
    # Тестовый URL изображения
    test_image_url = "http://export-content.baz-on.ru/pub/c4447/productphoto/0000/00/0000_00_718.jpg"
    
    # Пути к изображениям для наложения
    overlay_path = "images/1.png"
    
    # Путь для сохранения результата
    output_path = "test_overlay_result.jpg"
    
    try:
        # Загрузка базового изображения
        response = requests.get(test_image_url)
        if response.status_code != 200:
            print(f"Ошибка загрузки изображения {test_image_url}, код: {response.status_code}")
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
        
        # Минимальный отступ от нижнего края - всего 2% высоты
        bottom_margin = int(base_height * 0.02)  # 2% от высоты для минимального отступа снизу
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
        print(f"Результат сохранен: {output_path}")
        return True
    except Exception as e:
        print(f"Ошибка при наложении изображения: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    overlay_test() 