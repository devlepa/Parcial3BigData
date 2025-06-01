# glue_jobs/run_ml_pipeline.py
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import io
import boto3

# Inicializar SparkContext y SparkSession
sc = SparkContext()
spark = SparkSession(sc)

# Obtener argumentos pasados al script
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_bucket', 'output_path'])
job_name = args['JOB_NAME']
input_bucket = args['input_bucket']
output_path = args['output_path']

print(f"--- Spark Job '{job_name}' started ---")
print(f"Input Bucket: {input_bucket}")
print(f"Output Path: {output_path}")

try:
    # 1. Cargar datos desde S3 (CSVs procesados por la Lambda)
    input_path = f"s3://{input_bucket}/headlines/processed/"
    print(f"Reading data from: {input_path}")
    spark_df = spark.read.csv(input_path, header=True, inferSchema=True)
    spark_df.show(5)
    print(f"Number of rows loaded: {spark_df.count()}")

    # Convertir a Pandas DataFrame
    pandas_df = spark_df.toPandas()
    print("Converted to Pandas DataFrame.")

    # Asegurarse de que la columna 'title' existe y no es nula
    if 'title' not in pandas_df.columns:
        raise ValueError("Column 'title' not found in the input CSV data. Check process_headlines.py.")
    pandas_df['title'] = pandas_df['title'].fillna('').astype(str).str.lower().replace(r'\W', ' ', regex=True).str.strip()

    # 2. Simulación de Etiquetas (PARA PROBAR, DEBES REEMPLAZAR CON ETIQUETAS REALES)
    print("Simulating labels for ML model based on title content (REPLACE WITH REAL LABELS).")
    pandas_df['label'] = 0 # Default label: Other
    pandas_df.loc[pandas_df['title'].str.contains('fútbol|deporte|gol|equipo'), 'label'] = 1 # Label: Sports
    pandas_df.loc[pandas_df['title'].str.contains('política|gobierno|elecciones|congreso'), 'label'] = 2 # Label: Politics
    pandas_df.loc[pandas_df['title'].str.contains('salud|pandemia|medicina|enfermedad'), 'label'] = 3 # Label: Health
    pandas_df.loc[pandas_df['title'].str.contains('cultura|cine|música|arte'), 'label'] = 4 # Label: Culture

    # Eliminar filas con titulares vacíos después del preprocesamiento
    pandas_df = pandas_df[pandas_df['title'].str.len() > 0]

    if pandas_df.empty:
        print("No valid titles to process after filtering. Exiting.")
        sys.exit(0)

    # 3. Dividir datos en conjuntos de entrenamiento y prueba
    X = pandas_df['title'] # Usamos la columna 'title' para el ML
    y = pandas_df['label']

    # Solo estratificar si hay más de una clase
    stratify_param = y if len(y.unique()) > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=stratify_param)
    print(f"Train/Test split: {len(X_train)} training samples, {len(X_test)} testing samples.")

    # 4. Vectorización TF-IDF
    tfidf_vectorizer = TfidfVectorizer(max_features=5000)
    X_train_tfidf = tfidf_vectorizer.fit_transform(X_train)
    X_test_tfidf = tfidf_vectorizer.transform(X_test)
    print("TF-IDF vectorization complete.")

    # 5. Entrenamiento del Modelo (Regresión Logística)
    print("Training Logistic Regression model...")
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train_tfidf, y_train)
    print("Model training complete.")

    # 6. Evaluación del Modelo
    y_pred = model.predict(X_test_tfidf)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, zero_division=0)

    print(f"Model Accuracy: {accuracy}")
    print("Classification Report:\n", report)

    # 7. Guardar reporte de clasificación en S3
    s3_client_raw = boto3.client('s3')
    output_report_s3_key = f"{output_path.split(input_bucket + '/')[1]}classification_report_{job_name}.txt"
    with io.StringIO() as buf:
        buf.write(f"Model Accuracy: {accuracy}\n")
        buf.write("Classification Report:\n")
        buf.write(report)
        s3_client_raw.put_object(Bucket=input_bucket, Key=output_report_s3_key, Body=buf.getvalue().encode('utf-8'))
    print(f"Classification report saved to s3://{input_bucket}/{output_report_s3_key}")

except Exception as e:
    print(f"--- Spark Job failed with error: {e} ---")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("--- Spark Job finished successfully ---")