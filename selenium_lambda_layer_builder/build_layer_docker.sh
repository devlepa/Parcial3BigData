#!/bin/bash

set -e # Salir si algún comando falla

echo "🔄 Actualizando el sistema y preparando entorno Docker..."
yum update -y

# <-- AÑADIR 'zip' AQUÍ -->
echo "📦 Instalando unzip, python3-pip, tar, gzip y zip (si no están ya)..."
yum install -y unzip python3-pip tar gzip zip # <-- ¡Añadido 'zip'!

echo "📁 Creando estructura de la Lambda Layer en el contenedor..."
LAYER_ROOT="/lambda_layer"
PYTHON_SITE_PACKAGES="${LAYER_ROOT}/python/lib/python3.9/site-packages"
BIN_DIR="${LAYER_ROOT}/bin"

mkdir -p "$PYTHON_SITE_PACKAGES"
mkdir -p "$BIN_DIR"

echo "📦 Instalando Selenium en la capa..."
pip3 install selenium --upgrade -t "$PYTHON_SITE_PACKAGES"

echo "🌐 Descargando Headless Chromium y ChromeDriver compatibles (versión 137.0.7151.55)..."
CHROMIUM_VERSION="137.0.7151.55"

# --- SECCIÓN DE DESCARGA DE CHROMIUM MEJORADA ---
CHROMIUM_ZIP_NAME="chrome-headless-shell-linux64.zip"
CHROMIUM_DOWNLOAD_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROMIUM_VERSION}/linux64/${CHROMIUM_ZIP_NAME}"

echo "Descargando ${CHROMIUM_ZIP_NAME} desde ${CHROMIUM_DOWNLOAD_URL}..."
curl -fL -O "$CHROMIUM_DOWNLOAD_URL"

if [ ! -s "$CHROMIUM_ZIP_NAME" ]; then
    echo "❌ ERROR: La descarga de Headless Chromium falló o el archivo está vacío. Archivo: $CHROMIUM_ZIP_NAME"
    echo "Por favor, verifica la URL: $CHROMIUM_DOWNLOAD_URL"
    echo "Y la conexión a internet del contenedor."
    exit 1
fi

echo "Descomprimiendo Headless Chromium..."
unzip "$CHROMIUM_ZIP_NAME" -d "$BIN_DIR"/

echo "Moviendo binario de Chrome a la ruta final en la capa..."
CHROMIUM_EXTRACTED_DIR="${BIN_DIR}/chrome-headless-shell-linux64"
CHROMIUM_BIN="${CHROMIUM_EXTRACTED_DIR}/chrome-headless-shell"
FINAL_CHROMIUM_PATH="${BIN_DIR}/chrome"

if [ -f "$CHROMIUM_BIN" ]; then
    mv "$CHROMIUM_BIN" "$FINAL_CHROMIUM_PATH"
    echo "Binario 'chrome-headless-shell' movido y renombrado a ${FINAL_CHROMIUM_PATH}"
    rm -rf "$CHROMIUM_EXTRACTED_DIR"
else
    echo "❌ ERROR: No se encontró el binario 'chrome-headless-shell' en la ruta esperada después de la descompresión."
    echo "Esperaba: ${CHROMIUM_BIN}"
    echo "Por favor, verifica la estructura del zip descargado."
    exit 1
fi
chmod +x "$FINAL_CHROMIUM_PATH"

# --- SECCIÓN DE DESCARGA DE CHROMEDRIVER MEJORADA ---
CHROMEDRIVER_ZIP="chromedriver-linux64.zip"
CHROMEDRIVER_DOWNLOAD_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROMIUM_VERSION}/linux64/${CHROMEDRIVER_ZIP}"

echo "Descargando ${CHROMEDRIVER_ZIP} desde ${CHROMEDRIVER_DOWNLOAD_URL}..."
curl -fL -O "$CHROMEDRIVER_DOWNLOAD_URL"

if [ ! -s "$CHROMEDRIVER_ZIP" ]; then
    echo "❌ ERROR: La descarga de ChromeDriver falló o el archivo está vacío. Archivo: $CHROMEDRIVER_ZIP"
    echo "Por favor, verifica la URL: $CHROMEDRIVER_DOWNLOAD_URL"
    echo "Y la conexión a internet del contenedor."
    exit 1
fi

echo "Descomprimiendo ChromeDriver..."
unzip "$CHROMEDRIVER_ZIP" -d "$BIN_DIR"/

echo "Moviendo binario de ChromeDriver a la ruta final en la capa..."
CHROMEDRIVER_EXTRACTED_DIR="${BIN_DIR}/chromedriver-linux64"
FINAL_CHROMEDRIVER_PATH="${BIN_DIR}/chromedriver"

if [ -f "${CHROMEDRIVER_EXTRACTED_DIR}/chromedriver" ]; then
    mv "${CHROMEDRIVER_EXTRACTED_DIR}/chromedriver" "$FINAL_CHROMEDRIVER_PATH"
    echo "Binario 'chromedriver' movido a ${FINAL_CHROMEDRIVER_PATH}"
    rm -rf "$CHROMEDRIVER_EXTRACTED_DIR"
else
    echo "❌ ERROR: No se encontró el binario 'chromedriver' en la ruta esperada después de la descompresión."
    echo "Esperaba: ${CHROMEDRIVER_EXTRACTED_DIR}/chromedriver"
    echo "Por favor, verifica la estructura del zip descargado."
    exit 1
fi
chmod +x "$FINAL_CHROMEDRIVER_PATH"

echo "🧹 Limpiando archivos temporales..."
rm -f "$CHROMIUM_ZIP_NAME" "$CHROMEDRIVER_ZIP"

echo "📦 Empaquetando capa en ZIP..."
cd "$LAYER_ROOT"
zip -r /host_volume/selenium_chromium_layer.zip .

echo "✅ ¡Listo! El archivo /host_volume/selenium_chromium_layer.zip (en tu máquina local) está preparado para subir como Lambda Layer."