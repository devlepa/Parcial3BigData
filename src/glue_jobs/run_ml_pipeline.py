from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys
import os

# --- Inicializar Spark Session ---
# Esta SparkSession será creada cuando el script se ejecute via spark-submit en EMR
spark = SparkSession.builder.appName("NewsHeadlinesMLPipeline") \
    .getOrCreate()

# --- Configuración (se recomienda usar variables de entorno o argumentos para EMR) ---
# En un cluster EMR, puedes pasar estos como --conf spark.driver.extraJavaOptions=-DYOUR_VAR=value
# O, en el caso de spark-submit, puedes pasarlos como argumentos del script (sys.argv)
# Para este ejemplo, los definimos aquí asumiendo que EMR tiene acceso a S3_BUCKET_NAME
# O los obtendrás del entorno de EMR.
S3_BUCKET_NAME = os.environ.get('S3_BUCKET', 'your-unique-bucket-name-here') # Reemplaza con tu bucket real
S3_INPUT_PATH = f"s3://{S3_BUCKET_NAME}/headlines/final/"
S3_OUTPUT_PATH = f"s3://{S3_BUCKET_NAME}/ml_results/news_classification/"

print(f"Ejecutando Pipeline ML de PySpark. Entrada: {S3_INPUT_PATH}, Salida: {S3_OUTPUT_PATH}")

# --- 1. Cargar Datos desde S3 ---
print("Cargando datos desde S3...")
# Se asume que los CSV tienen cabecera y el esquema puede inferirse.
# Si quieres usar el catálogo de Glue (recomendado), usa:
# df = spark.read.table("news_headlines_db.headlines_final")
df = spark.read.csv(S3_INPUT_PATH, header=True, inferSchema=True)
df.show(5, truncate=False)
df.printSchema()

# --- 2. Preparación de Datos e Ingeniería de Características ---

# Crear una columna 'label' sintética para la clasificación (ejemplo).
# En un proyecto real, necesitarías un dataset previamente etiquetado para el entrenamiento supervisado.
# Asignando etiquetas numéricas arbitrarias a las categorías para clasificación multiclase
df = df.withColumn("label",
    when(col("category").isin(["General", "Noticias"]), lit(0))
    .when(col("category") == "Deportes", lit(1))
    .when(col("category") == "Politica", lit(2))
    .when(col("category") == "Economia", lit(3))
    .when(col("category") == "Cultura", lit(4))
    .when(col("category") == "Internacional", lit(5))
    .when(col("category") == "Opinion", lit(6))
    .otherwise(lit(7)) # Etiqueta por defecto para categorías desconocidas
)

# Combinar columnas de texto relevantes en una única columna 'text'
df = df.withColumn("text", col("headline"))
df.show(5, truncate=False)

# --- 3. Etapas del Pipeline PySpark ML ---

# Etapa 1: Tokenización (dividir texto en palabras)
tokenizer = Tokenizer(inputCol="text", outputCol="words")

# Etapa 2: Eliminar Stop Words (palabras comunes como "un", "la", "es")
# Para palabras en español, puedes cargar un diccionario de stop words específico.
# Ej: StopWordsRemover.loadDefaultStopWords("spanish") si tienes los datos de NLTK
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")

# Etapa 3: TF (Term Frequency) - Conteo de palabras
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="rawFeatures", numFeatures=20000)

# Etapa 4: IDF (Inverse Document Frequency) - Ponderación de palabras por rareza
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features") # 'features' es nuestro vector final

# Etapa 5: Modelo de Clasificación
# Regresión Logística como ejemplo. Otras opciones: NaiveBayes, RandomForestClassifier, GBTClassifier
# Asegúrate de que la columna 'label' exista y sea numérica.
classifier = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

# --- 4. Construir el Pipeline ML ---
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, classifier])

# --- 5. Dividir Datos en Entrenamiento y Prueba ---
(trainingData, testData) = df.randomSplit([0.8, 0.2], seed=1234)

# --- 6. Entrenar el Modelo ---
print("Entrenando el pipeline ML...")
model = pipeline.fit(trainingData)
print("Entrenamiento del modelo completado.")

# --- 7. Realizar Predicciones en Datos de Prueba ---
print("Realizando predicciones en datos de prueba...")
predictions = model.transform(testData)
predictions.select("headline", "category", "label", "prediction", "probability").show(10, truncate=False)

# --- 8. Evaluar el Modelo ---
print("Evaluando el modelo...")
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Precisión (Accuracy) en Test = {accuracy}")

# Puedes evaluar otras métricas como F1-score si las clases están desbalanceadas
# f1_score = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
# print(f"F1 Score = {f1_score}")

# --- 9. Guardar Resultados en S3 ---
print(f"Guardando predicciones en S3: {S3_OUTPUT_PATH}")
# Seleccionar columnas relevantes para la salida
output_df = predictions.select(
    "headline",
    "category",
    col("label").alias("original_label"), # Renombrar para claridad
    col("prediction").alias("predicted_label"),
    col("probability").alias("prediction_probability")
)

# Añadir fecha de procesamiento para particionamiento
output_df = output_df.withColumn("processing_date", current_date())

# Guardar en formato Parquet, particionado por processing_date
output_df.write.mode("overwrite").parquet(S3_OUTPUT_PATH)
print("Resultados guardados en S3.")

# --- Detener SparkSession ---
spark.stop()