# run_ml_pipeline.py
# This script will be uploaded to S3 and executed on EMR via spark-submit

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, current_date, year, month, dayofmonth, when, rand
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys

# --- Initialize Spark Session ---
# SparkSession will be created here when run via spark-submit
spark = SparkSession.builder.appName("NewsHeadlinesMLPipeline") \
    .getOrCreate()

# --- Get Arguments (e.g., S3 Bucket Name) ---
# For a script run by spark-submit, you might pass arguments
# For simplicity, we'll hardcode or use environment variables set in the Lambda,
# or read from a configuration file.
# For now, let's assume direct access or pass as a placeholder
# In a real scenario, you'd use sys.argv or a config file for parameters.
# Example if using arguments:
# S3_BUCKET_NAME = sys.argv[1] if len(sys.argv) > 1 else "your-unique-bucket-name-here"

# --- Configuration ---
S3_BUCKET_NAME = "your-unique-bucket-name-here" # Your data S3 bucket
S3_INPUT_PATH = f"s3://{S3_BUCKET_NAME}/headlines/final/"
S3_OUTPUT_PATH = f"s3://{S3_BUCKET_NAME}/ml_results/news_classification/"

print(f"Running PySpark ML Pipeline from script. Input: {S3_INPUT_PATH}, Output: {S3_OUTPUT_PATH}")

# --- 1. Load Data from S3 ---
# Ensure your Glue Data Catalog table is accessible if using that instead of direct CSV read.
# If reading from Glue Catalog:
# df = spark.read.table("news_headlines_db.headlines_final")
# For direct CSV read:
df = spark.read.csv(S3_INPUT_PATH, header=True, inferSchema=True)
df.show(5, truncate=False)
df.printSchema()

# --- 2. Data Preparation and Feature Engineering ---
# (Same as in notebook)
df = df.withColumn("label",
    when(col("category").isin(["General", "Noticias"]), lit(0))
    .when(col("category") == "Deportes", lit(1))
    .when(col("category") == "Politica", lit(2))
    .when(col("category") == "Economia", lit(3))
    .when(col("category") == "Cultura", lit(4))
    .when(col("category") == "Internacional", lit(5))
    .when(col("category") == "Opinion", lit(6))
    .otherwise(lit(7))
)
df = df.withColumn("text", col("headline"))

# --- 3. PySpark ML Pipeline Stages ---
# (Same as in notebook)
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="rawFeatures", numFeatures=20000)
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
classifier = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

# --- 4. Build the ML Pipeline ---
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, classifier])

# --- 5. Train-Test Split ---
(trainingData, testData) = df.randomSplit([0.8, 0.2], seed=1234)

# --- 6. Train the Model ---
print("Training the ML pipeline...")
model = pipeline.fit(trainingData)
print("Model training complete.")

# --- 7. Make Predictions on Test Data ---
print("Making predictions on test data...")
predictions = model.transform(testData)
predictions.select("headline", "category", "label", "prediction", "probability").show(10, truncate=False)

# --- 8. Evaluate the Model ---
print("Evaluating the model...")
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Test Accuracy = {accuracy}")

# --- 9. Save Results to S3 ---
print(f"Saving predictions to S3: {S3_OUTPUT_PATH}")
output_df = predictions.select(
    "headline", "category", col("label").alias("original_label"),
    col("prediction").alias("predicted_label"), col("probability").alias("prediction_probability")
)
output_df = output_df.withColumn("processing_date", current_date())
output_df.write.mode("overwrite").parquet(S3_OUTPUT_PATH)
print("Results saved to S3.")

# --- Stop Spark Session ---
spark.stop()