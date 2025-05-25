import pytest
import os
import requests_mock
from moto import mock_aws
from src.lambda_functions.download_headlines import download_and_upload_to_s3, s3

# Mock para S3 (usando moto) y requests (para HTTP)
@mock_aws
def test_download_and_upload_success():
    """
    Testea que la función descarga HTML y lo sube correctamente a S3.
    """
    # Configurar un bucket S3 de prueba para moto
    test_bucket = "test-noticias-data-bucket"
    os.environ['S3_DATA_BUCKET'] = test_bucket # Establecer la variable de entorno para que la Lambda la lea
    s3.create_bucket(Bucket=test_bucket)

    # Simular respuestas HTTP de los sitios de noticias
    with requests_mock.Mocker() as m:
        m.get("https://www.eltiempo.com/", text="<html><body><h1>El Tiempo News</h1></body></html>")
        m.get("https://www.elespectador.com/", text="<html><body><h2>El Espectador News</h2></body></html>")

        # Ejecutar la función Lambda
        response = download_and_upload_to_s3(None, None)

        # Verificar el resultado
        assert response['statusCode'] == 200
        assert "Successfully uploaded eltiempo" in response['body']
        assert "Successfully uploaded elespectador" in response['body']

        # Verificar que los archivos fueron subidos a S3
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        eltiempo_key = f"headlines/raw/eltiempo/{current_date}/eltiempo_{current_date}.html"
        elespectador_key = f"headlines/raw/elespectador/{current_date}/elespectador_{current_date}.html"

        eltiempo_obj = s3.get_object(Bucket=test_bucket, Key=eltiempo_key)
        elespectador_obj = s3.get_object(Bucket=test_bucket, Key=elespectador_key)

        assert eltiempo_obj['Body'].read().decode('utf-8') == "<html><body><h1>El Tiempo News</h1></body></html>"
        assert elespectador_obj['Body'].read().decode('utf-8') == "<html><body><h2>El Espectador News</h2></body></html>"

@mock_aws
def test_download_and_upload_http_error():
    """
    Testea que la función maneja errores HTTP al descargar.
    """
    test_bucket = "test-noticias-data-bucket-error"
    os.environ['S3_DATA_BUCKET'] = test_bucket
    s3.create_bucket(Bucket=test_bucket)

    with requests_mock.Mocker() as m:
        m.get("https://www.eltiempo.com/", status_code=500) # Simular error 500
        m.get("https://www.elespectador.com/", text="<html><body><h2>El Espectador News</h2></body></html>")

        response = download_and_upload_to_s3(None, None)

        assert response['statusCode'] == 200 # La Lambda no falla si una fuente falla, solo registra el error
        assert "Error al descargar de eltiempo" in response['body']
        assert "Successfully uploaded elespectador" in response['body']

        # Verificar que el archivo de El Tiempo no se subió (o se subió incorrectamente)
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        eltiempo_key = f"headlines/raw/eltiempo/{current_date}/eltiempo_{current_date}.html"
        with pytest.raises(s3.exceptions.NoSuchKey):
            s3.get_object(Bucket=test_bucket, Key=eltiempo_key) # Debería lanzar error porque no existe

# Limpia la variable de entorno después de cada test (opcional pero buena práctica)
@pytest.fixture(autouse=True)
def run_around_tests():
    original_s3_bucket = os.environ.get('S3_DATA_BUCKET')
    yield
    if original_s3_bucket:
        os.environ['S3_DATA_BUCKET'] = original_s3_bucket
    else:
        del os.environ['S3_DATA_BUCKET']