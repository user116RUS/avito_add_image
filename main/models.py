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
