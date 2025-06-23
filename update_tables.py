# from datetime import datetime
from main_fixed import set_attributes_for_account
import os, shutil
import django
import schedule, time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "AVITO.settings")
django.setup()

from main.models import Account

def clear_account_folders():
    print('Очистка прошлых директорий')
    try:
        shutil.rmtree('media')
        shutil.rmtree('accounts')
        os.mkdir('media')
        os.mkdir('accounts')
        return True
    
    except Exception as e:
        print('Возникла ошибка', e)
        return False


def update():
    '''
        Запуск скрипта для всех аккаунтов
    '''
    print("Запуск основного скрипта...")
    print('=' * 50)
    for account in Account.objects.filter(is_active=True):
        print(f'Аккаунт: {account.account_ident}')
        # Запускаем процесс
        if not os.path.exists(f'accounts/account_{account.account_ident}'):
            os.mkdir(f'accounts/account_{account.account_ident}')
        
        local_xml = f'accounts/account_{account.account_ident}/local_xml_{account.account_ident}.xml'
        output_excel = f'accounts/account_{account.account_ident}/output_excel_{account.account_ident}.xlsx'
        googl_cred = f'accounts/account_{account.account_ident}/google_cred.json'
        images_folder = f'accounts/account_{account.account_ident}/images'
        shop_images_cache = f'accounts/account_{account.account_ident}/shop_images_cache.json'
        
        set_attributes_for_account(
            xml_url=account.entry,
            local_xml=local_xml,
            output_excel=output_excel,
            google_cred=googl_cred,
            images_folder=images_folder,
            gdrive_folder_id=account.google_dir,
            shop_images_cache=shop_images_cache,
            city_list=list(map(lambda elem: elem.strip(), account.cities.split('|'))),
            description=account.description,
            is_city=account.is_cities,
            max_items=5,
        )

        

def update_with_schedule():
    # Настраиваем регулярное выполнение задачи (каждые 5 минут)
    update()
    schedule.every(5).minutes.do(update)
    
    # Бесконечный цикл для выполнения запланированных задач
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверка каждую минуту

if __name__ == "__main__":
    update_with_schedule()



# Set the DJANGO_SETTINGS_MODULE environment variable

# Initialize Django