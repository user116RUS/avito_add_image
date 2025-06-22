import os

from django.db import models
from django.core.files.storage import FileSystemStorage

from AVITO.settings import BASE_DIR


fs = FileSystemStorage(location=f"{BASE_DIR}/accounts")


# Динамическое определение пути для фото магазина
def store_file_name(instance, filename):
    return os.path.join(f'account_{instance.account.account_ident}/images', 'shop', filename)

# Динамическое определение пути для файлов для фото баннера
def banner_file_name(instance, filename):
    return os.path.join(f'account_{instance.account.account_ident}/images', 'banner', filename)

# Динамическое определение пути для файлов для google_cred
def google_cred_file_name(instance, filename):
    return os.path.join(f'account_{instance.account_ident}', 'google_cred.json')

class Account(models.Model):
    '''
        Таблица базон аккаунтов
    '''
    account_id = models.AutoField(verbose_name='ID аккаунта', primary_key=True, editable=False, default=0, auto_created=True)
    account_ident = models.CharField(verbose_name='Идентификатор аккаунта', max_length=64, help_text='Название или имя')
    entry = models.CharField(verbose_name='Ссылка на xml', max_length=128)
    google_dir = models.CharField(verbose_name='ID google папки', max_length=64)
    description = models.TextField(verbose_name='Описание')
    marker = models.CharField(verbose_name='Маркер для описания', max_length=64)
    marker_padding = models.PositiveSmallIntegerField(verbose_name='Отступ после маркера', help_text='Если маркер не нужно учитывать, укажите длину маркера', default=0)
    google_cred = models.FileField(storage=fs, verbose_name='Файл google_cred.json', upload_to=google_cred_file_name, default='')
    is_cities = models.BooleanField(verbose_name='Умножение по городам', default=False)
    cities = models.TextField(verbose_name='Список городов', help_text='Указывать через |', default=None, null=True, blank=True)
    is_active = models.BooleanField(verbose_name='Активен', default=True)

    class Meta:
        verbose_name = "Модель аккаунта"
    
    def __str__(self):
        return f'Аккаунт {self.account_id}'


class StoreImage(models.Model):
    '''
        Класс для работы с несколькими изображениями сразу
    '''
    account = models.ForeignKey(Account, verbose_name='Ссылка на пользователя', on_delete=models.CASCADE)
    image = models.FileField(verbose_name='Изображение', storage=fs, upload_to=store_file_name)

    class Meta:
        verbose_name = "Изображения магазина"
        verbose_name_plural = "Изображения магазина"


class BannerImage(models.Model):
    '''
        Класс для работы с несколькими изображениями сразу
    '''
    account = models.ForeignKey(Account, verbose_name='Ссылка на пользователя', on_delete=models.CASCADE)
    image = models.FileField(verbose_name='Изображение', storage=fs, upload_to=banner_file_name)

    class Meta:
        verbose_name = "Изображения плашек"
        verbose_name_plural = "Изображения плашек"

    # Керчь|Нижний Тагил|Екатеринбург|Пятигорск|Киров|Орск|Пенза