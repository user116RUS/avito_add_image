from django.contrib import admin
from .models import Organization, ProcessingLog, ProductItem, OverlayImage, ShopImage


@admin.register(Organization)
class OrganizationAdmin(admin.ModelAdmin):
    list_display = ('name', 'is_active', 'last_processed', 'processing_interval', 'created_at')
    list_filter = ('is_active',)
    search_fields = ('name',)
    readonly_fields = ('created_at', 'updated_at', 'last_processed')
    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'is_active', 'processing_interval')
        }),
        ('XML настройки', {
            'fields': ('xml_link', 'local_xml_file', 'output_excel_file', 'max_items')
        }),
        ('Настройки описания', {
            'fields': ('text_description', 'where_add_text', 'new_description')
        }),
        ('Google Drive интеграция', {
            'fields': ('gdrive_images_folder_id', 'gdrive_output_file_id', 
                       'gdrive_output_file_url', 'google_credentials_file')
        }),
        ('Системная информация', {
            'fields': ('created_at', 'updated_at', 'last_processed')
        }),
    )


@admin.register(OverlayImage)
class OverlayImageAdmin(admin.ModelAdmin):
    list_display = ('name', 'organization', 'position', 'is_active')
    list_filter = ('is_active', 'organization')
    search_fields = ('name', 'organization__name')
    readonly_fields = ('created_at',)
    list_editable = ('position', 'is_active')
    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'organization', 'image')
        }),
        ('Настройки', {
            'fields': ('position', 'is_active')
        }),
        ('Системная информация', {
            'fields': ('created_at',)
        }),
    )


@admin.register(ShopImage)
class ShopImageAdmin(admin.ModelAdmin):
    list_display = ('name', 'organization', 'position', 'use_for_collage', 'is_active')
    list_filter = ('is_active', 'use_for_collage', 'organization')
    search_fields = ('name', 'organization__name')
    readonly_fields = ('created_at',)
    list_editable = ('position', 'use_for_collage', 'is_active')
    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'organization', 'image')
        }),
        ('Настройки', {
            'fields': ('position', 'use_for_collage', 'is_active')
        }),
        ('Системная информация', {
            'fields': ('created_at',)
        }),
    )


@admin.register(ProcessingLog)
class ProcessingLogAdmin(admin.ModelAdmin):
    list_display = ('organization', 'start_time', 'end_time', 'status', 
                    'items_processed', 'items_added', 'items_updated', 'items_with_errors')
    list_filter = ('status', 'organization')
    search_fields = ('organization__name',)
    readonly_fields = ('start_time', 'end_time', 'items_processed', 
                       'items_added', 'items_updated', 'items_with_errors')
    fieldsets = (
        ('Общая информация', {
            'fields': ('organization', 'status')
        }),
        ('Время обработки', {
            'fields': ('start_time', 'end_time')
        }),
        ('Статистика', {
            'fields': ('items_processed', 'items_added', 'items_updated', 'items_with_errors')
        }),
        ('Детали ошибок', {
            'fields': ('error_message',)
        }),
    )


@admin.register(ProductItem)
class ProductItemAdmin(admin.ModelAdmin):
    list_display = ('title', 'organization', 'external_id', 'price', 
                    'is_active', 'is_processed', 'updated_at')
    list_filter = ('is_active', 'is_processed', 'organization')
    search_fields = ('title', 'external_id')
    readonly_fields = ('created_at', 'updated_at', 'processing_log')
    fieldsets = (
        ('Основная информация', {
            'fields': ('title', 'organization', 'external_id', 'price', 'category')
        }),
        ('Содержимое', {
            'fields': ('description', 'image_urls')
        }),
        ('Статус', {
            'fields': ('is_active', 'is_processed', 'processing_log')
        }),
        ('Дополнительно', {
            'fields': ('additional_data', 'created_at', 'updated_at')
        }),
    )
    list_per_page = 50
