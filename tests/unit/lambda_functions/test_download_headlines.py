import pytest
import boto3
from moto import mock_aws
import requests_mock
import os
import datetime # Importar datetime para la fecha actual

# Mockear la variable de entorno para el nombre del bucket
@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ['S3_BUCKET'] = 'test-bucket-for-headlines'
    yield
    del os.environ['S3_BUCKET']

@mock_aws
@requests_mock.Mocker()
def test_download_and_upload_to_s3_success(requests_mocker, capsys):
    # Importar la función *dentro* del test mockeado para que boto3 sea mockeado correctamente
    from src.lambda_functions.download_headlines import download_and_upload_to_s3

    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=os.environ['S3_BUCKET'])

    eltiempo_content = "<html><body><h1>El Tiempo Headline</h1></body></html>"
    elespectador_content = "<html><body><h2>El Espectador News</h2></body></html>"

    requests_mocker.get("[https://www.eltiempo.com/](https://www.eltiempo.com/)", text=eltiempo_content, status_code=200)
    requests_mocker.get("[https://www.elespectador.com/](https://www.elespectador.com/)", text=elespectador_content, status_code=200)

    response = download_and_upload_to_s3({}, {})

    assert response['statusCode'] == 200
    assert 'Download and upload process completed.' in response['body']

    today_str = datetime.date.today().strftime('%Y-%m-%d')

    obj_el_tiempo = s3_client.get_object(Bucket=os.environ['S3_BUCKET'], Key=f'headlines/raw/el_tiempo-contenido-{today_str}.html')
    assert obj_el_tiempo['Body'].read().decode('utf-8') == eltiempo_content

    obj_el_espectador = s3_client.get_object(Bucket=os.environ['S3_BUCKET'], Key=f'headlines/raw/el_espectador-contenido-{today_str}.html')
    assert obj_el_espectador['Body'].read().decode('utf-8') == elespectador_content

    captured = capsys.readouterr()
    assert "Successfully uploaded el_tiempo content." in captured.out
    assert "Successfully uploaded el_espectador content." in captured.out

@mock_aws
@requests_mock.Mocker()
def test_download_and_upload_to_s3_partial_failure(requests_mocker, capsys):
    from src.lambda_functions.download_headlines import download_and_upload_to_s3

    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=os.environ['S3_BUCKET'])

    # Mock one success, one failure
    requests_mocker.get("[https://www.eltiempo.com/](https://www.eltiempo.com/)", status_code=500)
    requests_mocker.get("[https://www.elespectador.com/](https://www.elespectador.com/)", text="<html><body><h2>El Espectador News</h2></body></html>", status_code=200)

    response = download_and_upload_to_s3({}, {})

    assert response['statusCode'] == 200 # Lambda returns 200 if it handles partial failures gracefully
    assert 'Download and upload process completed.' in response['body']

    captured = capsys.readouterr()
    assert "Error downloading [https://www.eltiempo.com/](https://www.eltiempo.com/)" in captured.out
    assert "Successfully uploaded el_espectador content." in captured.out

    today_str = datetime.date.today().strftime('%Y-%m-%d')
    with pytest.raises(s3_client.exceptions.NoSuchKey):
        s3_client.get_object(Bucket=os.environ['S3_BUCKET'], Key=f'headlines/raw/el_tiempo-contenido-{today_str}.html')

    obj_el_espectador = s3_client.get_object(Bucket=os.environ['S3_BUCKET'], Key=f'headlines/raw/el_espectador-contenido-{today_str}.html')
    assert obj_el_espectador['Body'].read().decode('utf-8') == "<html><body><h2>El Espectador News</h2></body></html>"