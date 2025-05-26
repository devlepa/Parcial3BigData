from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time
import os

# --- Configuración Básica ---
base_url = "https://www.eltiempo.com/ultimas-noticias" # URL de la página principal a raspar
all_data = [] # Lista para almacenar todos los datos extraídos

# --- Configuración del WebDriver ---
options = webdriver.ChromeOptions()
options.add_argument("--headless") # Ejecutar el navegador en modo sin cabeza
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

driver = None

try:
    # Inicializar el WebDriver de Chrome una sola vez
    print("Iniciando WebDriver de Chrome...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    print("WebDriver iniciado.")

    # --- Navegar y raspar la ÚNICA página ---
    print(f"Navegando a {base_url} (página única)...")
    driver.get(base_url)
    time.sleep(7) # Esperar 7 segundos para que la página cargue el contenido dinámico (ajustado para fiabilidad)

    # Obtener el HTML completo de la página después de la carga dinámica
    html_content = driver.page_source

    # --- Guarda el HTML descargado en eltiempo.html ---
    # Se usará un nombre fijo ya que es una sola página.
    with open("eltiempo.html", "w", encoding="utf-8") as f: # <-- ¡Nombre del archivo corregido aquí!
        f.write(html_content)
    print("HTML de la página principal de El Tiempo guardado en 'eltiempo.html'.")
    print("*** Puedes abrir 'eltiempo.html' en tu navegador y usar F12 para inspeccionar su estructura. ***")

    # Parsear el contenido HTML con BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # --- Extracción de datos de noticias ---
    articles = soup.find_all("article", class_=lambda x: x and "c-article" in x)

    if not articles:
        print(f"Advertencia: No se encontraron artículos con la clase 'c-article' en la página. Los selectores pueden haber cambiado o el contenido no cargó.")
    else:
        print(f"Procesando {len(articles)} artículos encontrados en la página.")

    for article in articles:
        # 1. Extraer Categoría
        category = article.get("data-seccion", "No category").strip()

        # La descripción se extrae pero NO se incluye en `all_data`
        # desc_tag = article.find("p", class_="c-article__epigraph")
        # description = desc_tag.get_text(strip=True) if desc_tag else "No description"

        # 2. Extraer Título y Link
        title_tag = article.find("h3", class_="c-article__title")

        title = "No title"
        link = "No link"

        if title_tag:
            a_tag = title_tag.find("a")
            if a_tag:
                title = a_tag.get_text(strip=True)
                link = a_tag['href'].strip() if a_tag.has_attr('href') else "No link"
                # Opcional: Si el link es relativo, hacerlo absoluto
                if link and not link.startswith("http") and link != "No link":
                    link = "https://www.eltiempo.com" + link

        # 3. Añadir los datos a la lista (sin descripción)
        if title != "No title":
            all_data.append({
                "category": category,
                "title": title,
                "link": link,
            })
            print(f"Extraído: Título='{title}', Categoría='{category}', Enlace='{link}'") # Agregado enlace al print
        else:
            print(f"Advertencia: Artículo sin título válido encontrado. Omitiendo.")

except Exception as e:
    print(f"Ocurrió un error inesperado durante el proceso de scraping: {e}")
    print("Asegúrate de tener Google Chrome/Chromium instalado y que 'webdriver_manager' pueda descargar el driver compatible.")

finally:
    if driver:
        driver.quit()
        print("Navegador cerrado.")

# --- Guarda todos los datos extraídos en un único archivo JSON ---
if all_data:
    with open("eltiempo.json", "w", encoding="utf-8") as json_file:
        json.dump(all_data, json_file, ensure_ascii=False, indent=4)
    print(f"\n¡Proceso completado! Datos de {len(all_data)} noticias guardados en 'eltiempo.json'.")
else:
    print("\nNo se extrajeron datos de noticias. El archivo 'eltiempo.json' no se creará o estará vacío.")