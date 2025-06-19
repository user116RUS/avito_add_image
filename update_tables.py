# import os
# from datetime import datetime
from main_fixed import set_attributes_for_account
import os, sys, subprocess
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "AVITO.settings")
django.setup()

from main.models import Account

def update():
    if not os.path.exists('accounts'):
        os.mkdir('accounts')

    for account in Account.objects.filter(is_active=True):
        # Запускаем процесс
        print("Запуск основного скрипта...")
        if not os.path.exists(f'accounts/account_{account.account_id}'):
            os.mkdir(f'accounts/account_{account.account_id}')
        
        local_xml = f'accounts/account_{account.account_id}/local_xml_{account.account_id}.xml'
        output_excel = f'accounts/account_{account.account_id}/output_excel_{account.account_id}.xlsx'
        googl_cred = "google_asdcred.json"
        images_folder = f'accounts/account_{account.account_id}/images'
        shop_images_cache = f'accounts/account_{account.account_id}/shop_images_cache.json'
        
        set_attributes_for_account(
            xml_url=account.entry,
            local_xml=local_xml,
            output_excel=output_excel,
            google_cred=googl_cred,
            images_folder=images_folder,
            gdrive_folder_id=account.google_dir,
            shop_images_cache=shop_images_cache,
            city_list=account.cities.split('|'),
            description=account.description,
            is_city=account.is_cities,
            max_items=5,
        )
        

if __name__ == "__main__":
    update()



# Set the DJANGO_SETTINGS_MODULE environment variable

# Initialize Django