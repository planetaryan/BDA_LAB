
# Import
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').master("local[5]").getOrCreate()

# Create DataFrame
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.show(truncate=False)

df.write.option("header",True) \
    .option("delimiter",";") \
    .mode("overwrite") \
    .csv("employees.csv")