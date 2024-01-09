from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize Spark Session
spark = SparkSession.builder.appName("WriteToHadoop").getOrCreate()

# Sample data
data = [Row(name="John Doe", age=30),
        Row(name="Jane Doe", age=25),
        Row(name="Mike Johnson", age=35)]

# Create DataFrame
df = spark.createDataFrame(data)

# Write DataFrame to HDFS
df.write.csv("hdfs://192.168.1.76:8020/user/your-username/data.csv")

# Stop Spark Session
spark.stop()
