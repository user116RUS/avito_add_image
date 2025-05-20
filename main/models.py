from django.db import models

class Organization(models.Model):
    name = models.CharField(max_length=255, verbose_name='Название организации')
    xml_link = models.URLField(verbose_name='Ссылка на XML-файл')
    local_xml_file = models.FileField(
        upload_to='xml_files/',
        null=True,
        blank=True,
        verbose_name='Локальный XML-файл',
    )
    output_excel_file = models.FileField(
        upload_to='excel_files/',
        null=True,
        blank=True,
        verbose_name='Выходной Excel-файл',
    )
    max_items = models.IntegerField(
        default=99999,
        verbose_name='Максимальное количество товаров',
    )
    text_description = models.TextField(
        null=True,
        blank=True,
        verbose_name='Какой текст добавим в описание?',
    )
    where_add_text = models.TextField(
        verbose_name='Где добавим текст?',
        help_text='Укажите маркер, после которого будет вставлен текст (например, "Lada;")',
    )
    offset_after_marker = models.IntegerField(
        default=0,
        verbose_name='Смещение после маркера',
        help_text='Через сколько символов после маркера вставлять текст (0 - сразу после маркера)',
    )
    
    new_description = models.TextField(
        verbose_name='Новое описание',
        blank=True,
        null=True,
        )
    gdrive_images_folder_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='ID папки на Google Drive для изображений',
        help_text='ID папки на Google Drive, куда будут сохраняться обработанные изображения',
    )
    gdrive_output_file_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='ID файла Excel на Google Drive',
        help_text='ID Excel-файла на Google Drive с обработанными данными',
    )
    gdrive_output_file_url = models.URLField(
        null=True,
        blank=True,
        verbose_name='URL к файлу Excel на Google Drive',
        help_text='Полная ссылка на документ Excel на Google Drive',
    )
    google_credentials_file = models.FileField(
        upload_to='google_creds/',
        null=True,
        blank=True,
        verbose_name='Файл авторизации Google',
        help_text='Файл JSON с учетными данными сервисного аккаунта Google',
    )
    is_active = models.BooleanField(
        default=True,
        verbose_name='Активна',
        help_text='Включить/выключить обработку данной организации',
    )
    processing_interval = models.IntegerField(
        default=5,
        verbose_name='Интервал обработки (минуты)',
        help_text='Как часто обрабатывать данные этой организации',
    )
    last_processed = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Последняя обработка',
    )
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Дата создания')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='Дата обновления')

    def __str__(self):
        return self.name
    
    class Meta:
        verbose_name = 'Организация'
        verbose_name_plural = 'Организации'
        ordering = ['-created_at']


class OverlayImage(models.Model):
    """Изображения для наложения (водяные знаки)"""
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        related_name='overlay_images',
        verbose_name='Организация'
    )
    image = models.ImageField(
        upload_to='overlay_images/',
        verbose_name='Изображение для наложения'
    )
    name = models.CharField(
        max_length=100, 
        verbose_name='Название'
    )
    position = models.IntegerField(
        default=0,
        verbose_name='Позиция',
        help_text='Порядок применения (0 - первое изображение товара, 1 - второе и т.д.)'
    )
    is_active = models.BooleanField(
        default=True,
        verbose_name='Активно'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.name} ({self.organization.name})"
    
    class Meta:
        verbose_name = 'Изображение для наложения'
        verbose_name_plural = 'Изображения для наложения'
        ordering = ['organization', 'position']


class ShopImage(models.Model):
    """Изображения магазина для добавления в конец товара"""
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        related_name='shop_images',
        verbose_name='Организация'
    )
    image = models.ImageField(
        upload_to='shop_images/',
        verbose_name='Изображение магазина'
    )
    name = models.CharField(
        max_length=100, 
        verbose_name='Название'
    )
    position = models.IntegerField(
        default=0,
        verbose_name='Позиция',
        help_text='Порядок добавления (меньшее значение - раньше в списке)'
    )
    use_for_collage = models.BooleanField(
        default=False,
        verbose_name='Использовать для коллажа',
        help_text='Если отмечено, это изображение будет использоваться для создания коллажа с первым фото товара'
    )
    is_active = models.BooleanField(
        default=True,
        verbose_name='Активно'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.name} ({self.organization.name})"
    
    class Meta:
        verbose_name = 'Изображение магазина'
        verbose_name_plural = 'Изображения магазина'
        ordering = ['organization', 'position']


class ProcessingLog(models.Model):
    """Журнал обработки организаций"""
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        verbose_name='Организация',
        related_name='logs',
    )
    start_time = models.DateTimeField(
        auto_now_add=True,
        verbose_name='Время начала',
    )
    end_time = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Время окончания',
    )
    status = models.CharField(
        max_length=20,
        choices=[
            ('in_progress', 'В процессе'),
            ('completed', 'Завершено'),
            ('failed', 'Ошибка'),
        ],
        default='in_progress',
        verbose_name='Статус',
    )
    items_processed = models.IntegerField(
        default=0,
        verbose_name='Обработано товаров',
    )
    items_added = models.IntegerField(
        default=0,
        verbose_name='Добавлено новых товаров',
    )
    items_updated = models.IntegerField(
        default=0,
        verbose_name='Обновлено товаров',
    )
    items_with_errors = models.IntegerField(
        default=0,
        verbose_name='Товары с ошибками',
    )
    error_message = models.TextField(
        null=True,
        blank=True,
        verbose_name='Сообщение об ошибке',
    )
    
    def __str__(self):
        return f"{self.organization.name} - {self.start_time}"
    
    class Meta:
        verbose_name = 'Журнал обработки'
        verbose_name_plural = 'Журналы обработки'
        ordering = ['-start_time']


class ProductItem(models.Model):

    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        verbose_name='Организация',
        related_name='products',
    )
    external_id = models.CharField(
        max_length=255,
        verbose_name='Внешний ID',
        help_text='ID товара из XML-файла',
    )
    title = models.CharField(
        max_length=255,
        verbose_name='Название',
    )
    description = models.TextField(
        null=True,
        blank=True,
        verbose_name='Описание',
    )
    price = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name='Цена',
    )
    category = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='Категория',
    )
    image_urls = models.JSONField(
        null=True,
        blank=True,
        verbose_name='URL изображений',
        help_text='Список URL обработанных изображений',
    )
    additional_data = models.JSONField(
        null=True,
        blank=True,
        verbose_name='Дополнительные данные',
        help_text='Дополнительные поля товара из XML',
    )
    is_processed = models.BooleanField(
        default=True,
        verbose_name='Обработан',
    )
    is_active = models.BooleanField(
        default=True,
        verbose_name='Активен',
    )
    processing_log = models.ForeignKey(
        ProcessingLog,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        verbose_name='Журнал обработки',
        related_name='products',
    )
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Дата создания')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='Дата обновления')
    
    class Meta:
        verbose_name = 'Товар'
        verbose_name_plural = 'Товары'
        ordering = ['-updated_at']
        unique_together = ['organization', 'external_id']
        indexes = [
            models.Index(fields=['organization', 'external_id']),
            models.Index(fields=['is_active']),
        ]
    
    def __str__(self):
        return f"{self.title} ({self.external_id})"
