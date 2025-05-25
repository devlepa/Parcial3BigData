import pytest
import boto3
from moto import mock_aws
import requests_mock # For mocking HTTP requests
from unittest.mock import patch
import os

# Assume download_headlines.py has a function like this:
# def download_and_upload_to_s3(event, context): ...

# Mock the environment variable for the bucket name
@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ['S3_BUCKET'] = 'test-bucket-for-headlines'
    yield
    del os.environ['S3_BUCKET']

@mock_aws
@requests_mock.Mocker()
def test_download_and_upload_to_s3(requests_mocker):
    from src.lambda_functions.download_headlines import download_and_upload_to_s3 # Adjust import path

    # 1. Mock S3 bucket creation (moto handles this implicitly if bucket doesn't exist)
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='test-bucket-for-headlines')

    # 2. Mock external HTTP requests
    eltiempo_content = "<html><body><h1>El Tiempo Headline</h1></body></html>"
    elespectador_content = "<html><body><h2>El Espectador News</h2></body></html>"
    requests_mocker.get("https://www.eltiempo.com/", text=eltiempo_content, status_code=200)
    requests_mocker.get("https://www.elespectador.com/", text=elespectador_content, status_code=200)

    # 3. Execute the Lambda function
    response = download_and_upload_to_s3({}, {})

    # 4. Assertions
    assert response['statusCode'] == 200
    assert 'Download and upload process completed.' in response['body']

    # Verify files were uploaded to S3
    obj_el_tiempo = s3_client.get_object(Bucket='test-bucket-for-headlines', Key=f'headlines/raw/el_tiempo-contenido-{datetime.date.today().strftime("%Y-%m-%d")}.html')
    assert obj_el_tiempo['Body'].read().decode('utf-8') == eltiempo_content

    obj_el_espectador = s3_client.get_object(Bucket='test-bucket-for-headlines', Key=f'headlines/raw/el_espectador-contenido-{datetime.date.today().strftime("%Y-%m-%d")}.html')
    assert obj_el_espectador['Body'].read().decode('utf-8') == elespectador_content

@mock_aws
@requests_mock.Mocker()
def test_download_and_upload_to_s3_failure(requests_mocker):
    from src.lambda_functions.download_headlines import download_and_upload_to_s3

    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket='test-bucket-for-headlines')

    # Mock a failed request
    requests_mocker.get("https://www.eltiempo.com/", status_code=500)
    requests_mocker.get("https://www.elespectador.com/", text="<html></html>", status_code=200) # One success

    response = download_and_upload_to_s3({}, {})

    assert response['statusCode'] == 200 # Lambda itself might still return 200 if it handles errors gracefully
    # Verify that only the successful file was uploaded, and the other wasn't or logs indicate failure.
    # You'd typically check logs for failure indication or assert specific error handling.
    assert 'Error downloading https://www.eltiempo.com/' in capsys.readouterr().out # If using capsys to capture print
    assert 'Successfully uploaded el_espectador content.' in capsys.readouterr().out
    # More robust tests would check for the absence of the failed file in S3.