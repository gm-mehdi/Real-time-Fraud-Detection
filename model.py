from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
# from kafka import KafkaProducer, KafkaConsumer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
# import json

# Initialiser Spark
spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Charger les données
data = spark.read.csv("transactions.csv", header=True, inferSchema=True)

# Changer le type de la colonne "type"
indexer = StringIndexer(inputCol="type", outputCol="typeIndex")
data = indexer.fit(data).transform(data)

# Assembler les features
assembler = VectorAssembler(
    inputCols=["amount", "typeIndex"],  # Ajoutez d'autres colonnes de features si nécessaire
    outputCol="features"
)

# Appliquer l'assemblage
data = assembler.transform(data)

# Séparer les données en données d'entraînement et de test
(trainingData, testData) = data.randomSplit([0.8, 0.2])

# Créer le modèle 
dt = DecisionTreeClassifier(labelCol="isFlaggedFraud", featuresCol="features")

# Créer un pipeline
pipeline = Pipeline(stages=[dt])

# Entraîner le modèle
model = pipeline.fit(trainingData)

# Sauvegarder le modèle
model.save("trained.csv")

model_trained = PipelineModel.load("trained.csv")
# # Kafka configuration
# input_topic = 'transactions'
# output_topic = 'fraud_transactions'

# producer = KafkaProducer(bootstrap_servers='localhost:9092',
#                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# consumer = KafkaConsumer(input_topic,
#                          bootstrap_servers='localhost:9092',
#                          value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def is_fraud(transactions):
    # Convertir le dictionnaire en INT
    transactions['isFlaggedFraud'] = int(transactions['isFlaggedFraud'])
    # Convertir le dictionnaire en DataFrame Spark
    df = spark.createDataFrame([transactions])
    # Prédire la fraude
    prediction = model.transform(df)
    # Récupérer la prédiction
    is_fraudulent = prediction.collect()[0]['isFlaggedFraud'] == 1
    return is_fraudulent

# try:
#     for message in consumer:
#         transaction = message.value
#         if is_fraud(transaction):
#             producer.send(output_topic, value=transaction)
#             print("Fraud detected and sent:", transaction)
# except KeyboardInterrupt:
#     consumer.close()
#     producer.close()