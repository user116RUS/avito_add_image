from django.core.files.storage import FileSystemStorage

from AVITO.settings import BASE_DIR

class AccountStorage(FileSystemStorage):
    def __init__(self, location = ..., base_url = ..., file_permissions_mode = ..., directory_permissions_mode = ...):
        self.location = BASE_DIR / 'accounts'
        self.base_url = '/accounts/'
        super().__init__(location, base_url, file_permissions_mode, directory_permissions_mode)