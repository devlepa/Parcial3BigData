from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys
import os
from dotenv import load_dotenv
from awsglue.utils import getResolvedOptions # Para obtener argumentos de EMR/Spark Submit

# Carga variables de entorno del archivo .env si existe (solo para desarrollo local)
load_dotenv()

# Inicializar SparkSession
# En EMR, SparkSession ya viene preconfigurada por el JobFlow.
spark = SparkSession.builder.appName("NewsMLPipeline").getOrCreate()

# Try to get S3_DATA_BUCKET from command-line arguments (passed by EMR orchestrator)
# Fallback to environment variables if not found (for local testing or other env setup)
try:
    # EMR Orchestrator pasa --s3_data_bucket, así se lee
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_data_bucket'])
    S3_BUCKET_NAME = args['s3_data_bucket']
except Exception as e:
    print(f"Advertencia: No se pudo obtener s3_data_bucket de los argumentos. Intentando leer de variables de entorno. Error: {e}")
    # Esto se usará para pruebas locales o si EMR inyecta la variable de otra forma
    S3_BUCKET_NAME = os.environ.get('S3_DATA_BUCKET')

if not S3_BUCKET_NAME:
    raise ValueError("S3_DATA_BUCKET no está configurado. Por favor, proporiónalo como argumento de Spark-Submit o en las variables de entorno.")

S3_INPUT_PATH = f"s3://{S3_BUCKET_NAME}/headlines/final/"
S3_OUTPUT_PATH = f"s3://{S3_BUCKET_NAME}/ml_results/news_classification/"

print(f"Leyendo datos de: {S3_INPUT_PATH}")
print(f"Escribiendo resultados en: {S3_OUTPUT_PATH}")

# Cargar datos
try:
    df = spark.read.csv(S3_INPUT_PATH, header=True, sep=';', inferSchema=True)
    print("Esquema de los datos cargados:")
    df.printSchema()
    df.show(5, truncate=False)
except Exception as e:
    print(f"Error al cargar datos desde {S3_INPUT_PATH}: {e}")
    # En un entorno de producción, aquí podrías querer enviar una notificación.
    spark.stop()
    sys.exit(1) # Salir con un código de error

# Preprocesamiento de texto
tokenizer = Tokenizer(inputCol="titular", outputCol="words")
stopwords_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
hashing_tf = HashingTF(inputCol=stopwords_remover.getOutputCol(), outputCol="raw_features")
idf = IDF(inputCol=hashing_tf.getOutputCol(), outputCol="features")

# Preparar datos para el modelo (usaremos una columna numérica para la categoría)
# Crear un mapeo simple de categorías a números
# Si tienes muchas categorías, podrías usar StringIndexer y OneHotEncoder
df_prep = df.withColumn("label", when(col("categoria") == "colombia", 0.0)
                               .when(col("categoria") == "mundo", 1.0)
                               .otherwise(2.0)) # Ejemplo: 0 para Colombia, 1 para Mundo, 2 para Otros

# Dividir datos en conjuntos de entrenamiento y prueba
(trainingData, testData) = df_prep.randomSplit([0.7, 0.3], seed=42)

# Modelo de clasificación (Regresión Logística como ejemplo simple)
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

# Crear un pipeline
pipeline = Pipeline(stages=[tokenizer, stopwords_remover, hashing_tf, idf, lr])

# Entrenar el modelo
print("Entrenando el modelo ML...")
model = pipeline.fit(trainingData)
print("Modelo ML entrenado.")

# Realizar predicciones en los datos de prueba
predictions = model.transform(testData)
print("Predicciones realizadas:")
predictions.select("titular", "categoria", "label", "prediction").show(5, truncate=False)

# Evaluar el modelo
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Precisión del modelo: {accuracy}")

# Guardar los resultados del análisis ML en S3
# Añadir la fecha actual a los resultados para facilitar el particionamiento si se desea
results_df = predictions.select("titular", "categoria", "enlace", "prediction") \
                        .withColumn("fecha_ejecucion_ml", current_date())

print(f"Guardando resultados del ML en: {S3_OUTPUT_PATH}")
# Guardar en formato Parquet, que es optimizado para Spark y consultas futuras
results_df.write.mode("overwrite").parquet(S3_OUTPUT_PATH)
print("Resultados del ML guardados exitosamente.")

# Finaliza la sesión de Spark
spark.stop()
print("Job Spark finalizado.")