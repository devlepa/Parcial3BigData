# src/lambda_functions/download_headlines.py
import os
import boto3
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import time # Importar time para el sleep

# AWS S3 client
s3_client = boto3.client('s3')

def download_and_upload_html_with_selenium(event, context):
    print("Starting download_and_upload_html_with_selenium Lambda function.")

    s3_data_bucket = os.environ.get('S3_DATA_BUCKET')
    if not s3_data_bucket:
        print("ERROR: S3_DATA_BUCKET environment variable not set.")
        return {
            'statusCode': 500,
            'body': 'S3_DATA_BUCKET environment variable not configured.'
        }

    # URL base para El Espectador Archivo.
    # Para raspar múltiples páginas, tendrías que invocar esta Lambda varias veces
    # o implementar una lógica de paginación más avanzada (ej. usando Step Functions).
    # Por ahora, solo la primera página para simplificar y ajustarse a los límites de Lambda.
    scrape_url = "https://www.elespectador.com/archivo/" # La primera página del archivo

    # --- Selenium Setup for Lambda Layer ---
    chrome_bin_path = "/opt/bin/chrome"
    chromedriver_path = "/opt/bin/chromedriver"

    print(f"Chromium binary path: {chrome_bin_path}")
    print(f"ChromeDriver path: {chromedriver_path}")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280x800")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--remote-debugging-port=9222") # Útil para depuración si fuera necesario

    service = Service(executable_path=chromedriver_path)

    driver = None
    try:
        print("Initializing Chrome WebDriver...")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print(f"WebDriver initialized. Navigating to: {scrape_url}")

        driver.get(scrape_url)
        time.sleep(5) # Esperar a que la página cargue completamente los elementos dinámicos
        print(f"Successfully navigated to {scrape_url}. Page title: {driver.title}")

        html_content = driver.page_source
        print(f"Successfully obtained HTML content. Length: {len(html_content)} bytes.")

        # Generate a unique filename for S3
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        # El nombre del archivo puede incluir la página para identificarla mejor si raspas varias
        file_name = f"elespectador_archive_page_1_{timestamp}.html"
        s3_key = f"headlines/raw/{file_name}"

        s3_client.put_object(
            Bucket=s3_data_bucket,
            Key=s3_key,
            Body=html_content,
            ContentType='text/html'
        )
        print(f"Successfully uploaded HTML to s3://{s3_data_bucket}/{s3_key}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'HTML content from {scrape_url} saved to S3 at {s3_key}')
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error scraping {scrape_url}: {str(e)}')
        }
    finally:
        if driver:
            print("Quitting WebDriver.")
            driver.quit()
        print("Lambda function execution finished.")