from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col

# Crear una sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate() 

# Define la ruta del archivo .csv en HDFS 
file_path = 'hdfs://localhost:9000/Tarea3/cancer-probabilities.csv' 

# Cargar el archivo CSV
df = spark.read.csv("cancer-probabilities.csv", header=True, inferSchema=True)

# Mostrar las primeras filas del DataFrame
print("Primeras filas del DataFrame:")
df.show()

# Mostrar el esquema del DataFrame
print("Esquema del DataFrame:")
df.printSchema()

# Contar el número de registros
num_registros = df.count()
print(f"Número total de registros: {num_registros}")

# Consultar la distribución de hábitos de fumar
print("Distribución de hábitos de fumar:")
df.groupBy("Smoking Habit").count().show()

# 1. Relación entre el hábito de fumar y la probabilidad de cáncer
print("Probabilidad media de cáncer según el hábito de fumar:")
df.groupBy("Smoking Habit").agg(F.mean("Probability of Cancer").alias("Mean Cancer Probability")).show()

# 2. Efecto del hábito de ciclismo en la probabilidad de cáncer
print("Probabilidad media de cáncer según el hábito de ciclismo:")
df.groupBy("Biking Habit").agg(F.mean("Probability of Cancer").alias("Mean Cancer Probability")).show()

# 3. Relación entre el hábito de caminar y la probabilidad de cáncer
print("Probabilidad media de cáncer según el hábito de caminar:")
df.groupBy("Walking Habit").agg(F.mean("Probability of Cancer").alias("Mean Cancer Probability")).show()

# 4. Efecto del hábito de trotar en la probabilidad de cáncer
print("Probabilidad media de cáncer según el hábito de trotar:")
df.groupBy("Jogging Habit").agg(F.mean("Probability of Cancer").alias("Mean Cancer Probability")).show()


# 6. Probabilidad de cáncer para fumadores intensos que consumen alcohol con frecuencia
print("Probabilidad de cáncer para fumadores intensos y consumidores frecuentes de alcohol:")
df.filter((df["Smoking Habit"] == "Heavy") & (df["Drinking Habit"] == "Frequent")) \
  .agg(F.mean("Probability of Cancer").alias("Mean Cancer Probability")).show()

# 7. Probabilidad de cáncer para quienes no fuman ni consumen alcohol
print("Probabilidad de cáncer para quienes no fuman ni consumen alcohol:")
df.filter((df["Smoking Habit"] == "None") & (df["Drinking Habit"] == "None")) \
  .agg(F.mean("Probability of Cancer").alias("Mean Cancer Probability")).show()
