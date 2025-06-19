from django.db import models

class Organization(models.Model):
    name = models.CharField(max_length=255)
    xml_link = models.URLField()
    local_xml_file = models.FileField(upload_to='xml_files/')
    output_excel_file = models.FileField(upload_to='excel_files/')
    google_cred_file = models.FileField(upload_to='google_cred_files/')
    max_items = models.IntegerField()
    images_folder_name = models.CharField(max_length=255)
    new_description = models.TextField()
    overlay_images = models.JSONField()
    watermark_path = models.FileField(upload_to='watermark_files/')
    shop_images = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name
    
    class Meta:
        verbose_name = 'Организация'
        verbose_name_plural = 'Организации'
        ordering = ['-created_at']

# Динамическое определение пути для файлов
def content_file_name(instance, filename):
    return '/'.join(['content', instance.account_id, filename])


class Account(models.Model):
    '''
        Таблица базон аккаунтов
    '''
    account_id = models.IntegerField(verbose_name='ID аккаунта', primary_key=True, editable=False)
    entry = models.CharField(verbose_name='Ссылка на xml', max_length=128)
    google_dir = models.CharField(verbose_name='ID google папки', max_length=64)
    description = models.TextField(verbose_name='Описание')
    marker = models.CharField(verbose_name='Маркер для описания', max_length=64)
    marker_padding = models.PositiveSmallIntegerField(verbose_name='Отступ после маркера', help_text='Если маркер не нужно учитывать, укажите длину маркера', default=0)
    png_banner = models.IntegerField(blank=True, verbose_name='Фото png плашки')
    is_cities = models.BooleanField(verbose_name='Умножение по городам', default=False)
    cities = models.TextField(verbose_name='Список городов', help_text='Указывать через |', default=None, null=True, blank=True)
    is_active = models.BooleanField(verbose_name='Активен', default=True)

    class Meta:
        verbose_name = "Модель аккаунта"
    
    def __str__(self):
        return f'Аккаунт {self.account_id}'


