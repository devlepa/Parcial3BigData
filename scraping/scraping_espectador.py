from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time

base_url = "https://www.elespectador.com/archivo"
all_data = []

for i in range(1, 10):
    url = f"{base_url}/{i}/"
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    time.sleep(5)
    html_content = driver.page_source
    driver.quit()
    soup = BeautifulSoup(html_content, "html.parser")
    cards = soup.find_all("div", class_="BlockContainer-Content")
    for card in cards:
        category = card.find("div", class_="Card-SectionContainer")
        category = category.get_text(strip=True) if category else "No category"
        title = card.find("h2", class_="Card-Title")
        title = title.get_text(strip=True) if title else "No title"
        description = card.find("div", class_="Card-Hook")
        description = description.get_text(strip=True) if description else "No description"
        all_data.append({
            "category": category,
            "title": title,
            "description": description
        })

with open("espectador.json", "w", encoding="utf-8") as json_file:
    json.dump(all_data, json_file, ensure_ascii=False, indent=4)