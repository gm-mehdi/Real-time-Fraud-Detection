from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline

# Initialize Spark
sc = SparkContext()
spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Load data and train model (only needs to be done once)
data = spark.read.csv("transactions.csv", header=True, inferSchema=True)
indexer = StringIndexer(inputCol="type", outputCol="typeIndex")
assembler = VectorAssembler(inputCols=["amount", "typeIndex"], outputCol="features")
dt = DecisionTreeClassifier(labelCol="isFlaggedFraud", featuresCol="features")

# Create a pipeline
pipeline = Pipeline(stages=[indexer, assembler, dt])

# Train the model
model = pipeline.fit(data)

# Save the trained pipeline
model.save("trained_pipeline")

# Load the trained pipeline
model_trained = PipelineModel.load("trained_pipeline")

def is_fraud(transactions):
    # Convert incoming transaction to DataFrame
    transactions['isFlaggedFraud'] = int(transactions['isFlaggedFraud'])
    df = spark.createDataFrame([transactions])
    
    # Use the trained pipeline to make predictions
    prediction = model_trained.transform(df)
    
    # Retrieve the prediction
    is_fraudulent = prediction.collect()[0]['isFlaggedFraud'] == 1
    return is_fraudulent
