from main import download_xml, process_xml

def run_manual_process():
    """Запуск процесса обработки вручную"""
    print("Начало ручной обработки XML-файла")
    if download_xml():
        df, file_url = process_xml()
        print(f"Ссылка на обработанный документ: {file_url}")
    print("Обработка завершена")

if __name__ == "__main__":
    run_manual_process() 