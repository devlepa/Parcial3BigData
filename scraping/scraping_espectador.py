import json
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# --- Configuración Básica ---
base_url = "https://www.elespectador.com/archivo/" # URL de la página principal a raspar
all_data = [] # Lista para almacenar todos los datos extraídos

# --- Configuración del WebDriver ---
options = webdriver.ChromeOptions()
options.add_argument("--headless") # Ejecutar el navegador en modo sin cabeza (sin interfaz gráfica)
options.add_argument("--no-sandbox") # Recomendado para entornos de ejecución sin interfaz gráfica
options.add_argument("--disable-dev-shm-usage") # Previene problemas de memoria en ciertos sistemas
options.add_argument("--disable-gpu") # Desactiva la aceleración por hardware de la GPU (útil en algunos sistemas)
options.add_argument("--window-size=1920,1080") # Define un tamaño de ventana para un renderizado consistente
# Simula un user-agent real para evitar posibles bloqueos o contenido diferente
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

driver = None # Inicializar driver a None para un manejo seguro en el bloque try-finally

try:
    print("Iniciando WebDriver de Chrome...")
    # Instala o carga el ChromeDriver compatible con tu versión de Chrome
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    print("WebDriver iniciado.")

    print(f"Navegando a {base_url} (página única)...")
    driver.get(base_url)

    # --- Espera EXPLICITA y Robusta para que el contenido de noticias cargue ---
    # Esto es crucial para páginas con contenido dinámico.
    # Buscamos la presencia de un elemento clave que indica que las noticias han cargado.
    # 'div.BlockContainer-Content h2.Card-Title' es un selector común para titulares de El Espectador.
    print("Esperando hasta 20 segundos para que los elementos principales de noticias aparezcan...")
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.BlockContainer-Content h2.Card-Title"))
        )
        print("Títulos de noticias detectados. El contenido dinámico de la página parece haber cargado.")
    except Exception as e:
        print(f"ERROR: No se encontraron los títulos de noticia esperados después de 20 segundos.")
        print(f"Esto puede indicar que la página no cargó completamente, la estructura HTML ha cambiado, o hay un problema de conexión.")
        print(f"Detalle del error: {e}")
        # Guardar el HTML en caso de fallo para ayudar en la depuración
        html_content_fail = driver.page_source
        with open("elespectador_load_fail.html", "w", encoding="utf-8") as f:
            f.write(html_content_fail)
        print(f"HTML (después de fallo de carga) guardado en 'elespectador_load_fail.html' para revisión.")
        # Re-lanzar la excepción para detener el script si la página no carga correctamente.
        raise # Re-lanzar la excepción para que el script termine si no puede cargar la página

    # Obtener el HTML completo de la página DESPUÉS de que el contenido dinámico ha cargado
    html_content = driver.page_source

    # --- Guarda el HTML descargado en un archivo (elespectador.html) ---
    with open("elespectador.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("HTML de la página guardado exitosamente en 'elespectador.html'.")
    print("*** Puedes abrir 'elespectador.html' en tu navegador y usar F12 para inspeccionar su estructura. ***")

    # Parsear el contenido HTML con BeautifulSoup para extraer la información
    soup = BeautifulSoup(html_content, "html.parser")

    # --- Extracción de datos de noticias ---
    # Buscar todos los contenedores principales de noticias.
    # Basado en la estructura de El Espectador, 'BlockContainer-Content' es un buen punto de partida.
    cards = soup.find_all("div", class_="BlockContainer-Content")

    if not cards:
        print(f"Advertencia: No se encontraron elementos 'div.BlockContainer-Content' en la página. No se procesarán noticias.")
    else:
        print(f"Se encontraron {len(cards)} posibles contenedores de noticias para procesar.")

    # Iterar sobre cada contenedor de noticia para extraer la información
    for card in cards:
        category = "No category"
        title = "No title"
        link = "No link"

        # 1. Extraer Categoría
        # La categoría suele estar en un div con la clase "Card-SectionContainer".
        # Puede ser un hijo directo de 'card' o un hermano de otro sub-elemento dentro de 'card'.
        category_tag = card.find("div", class_="Card-SectionContainer")
        if category_tag:
            category = category_tag.get_text(strip=True)

        # 2. Extraer Título
        # El título de la noticia se encuentra típicamente en un h2 con la clase "Card-Title".
        title_tag = card.find("h2", class_="Card-Title")
        if title_tag:
            title = title_tag.get_text(strip=True)

            # 3. Extraer Enlace (Link)
            # El enlace ('href') se encuentra en una etiqueta '<a>' que es el elemento padre del 'h2.Card-Title'.
            a_tag = title_tag.find_parent("a", href=True) # Busca el <a> padre que tenga un atributo href
            if a_tag:
                link = a_tag.get('href', "No link").strip()
                # A veces, el enlace puede ser relativo (ej. /noticia/abc). Es buena práctica hacerlos absolutos.
                if not link.startswith("http") and link != "No link":
                    # Asume que si es relativo, es relativo a la base del sitio web.
                    link = "https://www.elespectador.com" + link


        # La descripción NO se extrae ni se incluye en el JSON final, tal como solicitaste.

        # Añadir los datos extraídos a la lista `all_data` solo si se encontró un título válido.
        if title != "No title":
            all_data.append({
                "category": category,
                "title": title,
                "link": link, # <-- Enlace incluido en el JSON final
            })
            # Imprimir en consola la información extraída para seguimiento.
            print(f"Extraído: Título='{title}', Categoría='{category}', Enlace='{link}'")
        else:
            print(f"Advertencia: Un contenedor 'BlockContainer-Content' no contenía un título válido (h2.Card-Title). Omitiendo este item.")

except Exception as e:
    print(f"\n¡Ocurrió un error crítico durante la ejecución del script: {e}")
    print("Por favor, revisa la conexión a internet, la instalación de Google Chrome/Chromium y el ChromeDriver compatible.")

finally:
    # Asegurarse de cerrar el navegador al finalizar o si ocurre un error.
    if driver:
        driver.quit()
        print("Navegador cerrado.")

# --- Guarda todos los datos extraídos en un único archivo JSON ---
# Se guardarán todos los datos recopilados en un archivo llamado 'espectador.json'.
if all_data:
    with open("espectador.json", "w", encoding="utf-8") as json_file:
        json.dump(all_data, json_file, ensure_ascii=False, indent=4) # `indent=4` para formato legible.
    print(f"\n¡Proceso completado con éxito! Se extrajeron {len(all_data)} noticias.")
    print(f"Los datos han sido guardados en 'espectador.json'.")
else:
    print("\nNo se extrajeron datos de noticias. El archivo 'espectador.json' no se creará o estará vacío.")
    print("Revisa los mensajes de advertencia/error anteriores para más detalles.")