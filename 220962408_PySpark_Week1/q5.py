from pyspark.sql import SparkSession

spark=SparkSession.builder\
.appName("Print CSV")\
.getOrCreate()

file_path="/home/lplab/Desktop/220962408_PySpark_Week1/stats.csv"

df=spark.read.csv(file_path,header=True,inferSchema=True)
df.show(5)
df.printSchema()


# +----+--------------------+--------------------+------------+--------------------+-----+-----------------+
# |year|industry_code_ANZSIC|industry_name_ANZSIC|rme_size_grp|            variable|value|             unit|
# +----+--------------------+--------------------+------------+--------------------+-----+-----------------+
# |2011|                   A|Agriculture, Fore...|         a_0|       Activity unit|46134|            COUNT|
# |2011|                   A|Agriculture, Fore...|         a_0|Rolling mean empl...|    0|            COUNT|
# |2011|                   A|Agriculture, Fore...|         a_0|Salaries and wage...|  279|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|Sales, government...| 8187|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|        Total income| 8866|DOLLARS(millions)|
# +----+--------------------+--------------------+------------+--------------------+-----+-----------------+
# only showing top 5 rows

# root
#  |-- year: integer (nullable = true)
#  |-- industry_code_ANZSIC: string (nullable = true)
#  |-- industry_name_ANZSIC: string (nullable = true)
#  |-- rme_size_grp: string (nullable = true)
#  |-- variable: string (nullable = true)
#  |-- value: string (nullable = true)
#  |-- unit: string (nullable = true)