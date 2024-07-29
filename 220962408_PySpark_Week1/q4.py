from pyspark.sql import SparkSession

spark=SparkSession.builder\
.appName("Read CSV")\
.getOrCreate()

file_path="/home/lplab/Desktop/220962408_PySpark_Week1/stats.csv"

df=spark.read.csv(file_path,header=True,inferSchema=True)
df.show()


# +----+--------------------+--------------------+------------+--------------------+-----+-----------------+
# |year|industry_code_ANZSIC|industry_name_ANZSIC|rme_size_grp|            variable|value|             unit|
# +----+--------------------+--------------------+------------+--------------------+-----+-----------------+
# |2011|                   A|Agriculture, Fore...|         a_0|       Activity unit|46134|            COUNT|
# |2011|                   A|Agriculture, Fore...|         a_0|Rolling mean empl...|    0|            COUNT|
# |2011|                   A|Agriculture, Fore...|         a_0|Salaries and wage...|  279|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|Sales, government...| 8187|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|        Total income| 8866|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|   Total expenditure| 7618|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|Operating profit ...|  770|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|        Total assets|55700|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|         a_0|Fixed tangible as...|32155|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       b_1-5|       Activity unit|21777|            COUNT|
# |2011|                   A|Agriculture, Fore...|       b_1-5|Rolling mean empl...|38136|            COUNT|
# |2011|                   A|Agriculture, Fore...|       b_1-5|Salaries and wage...| 1435|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       b_1-5|Sales, government...|13359|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       b_1-5|        Total income|13771|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       b_1-5|   Total expenditure|12316|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       b_1-5|Operating profit ...| 1247|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       b_1-5|        Total assets|52666|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       b_1-5|Fixed tangible as...|31235|DOLLARS(millions)|
# |2011|                   A|Agriculture, Fore...|       c_6-9|       Activity unit| 1965|            COUNT|
# |2011|                   A|Agriculture, Fore...|       c_6-9|Rolling mean empl...|13848|            COUNT|
# +----+--------------------+--------------------+------------+--------------------+-----+-----------------+
# only showing top 20 rows