import time
from spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce
from datetime import datetime, timedelta
from pyspark.sql.types import *

spark = create_spark_session()

# Q1 - DATA QUEUE
data = [
  (4, "headphones", "accessories"),
  (5, "smartwatch", "accessories"),
  (6, "keyboard", "accessories"),
  (7, "mouse", "accessories"),
  (8, "monitor", "accessories"),
  (1, "laptop", "electronics"),
  (2, "smartphone", "electronics"),
  (3, "tablet", "electronics"),
  (9, "printer", "electronics"),
]
schema = ["product_id", "product_name", "category"]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

w = Window.partitionBy(F.col("category")).orderBy(F.col("product_id").desc())
w1 = Window.partitionBy(F.col("category")).orderBy(F.col("product_id"))

df = df.withColumn("rn_asc", F.row_number().over(w1))
df = df.withColumn("rn_dsc", F.row_number().over(w))

sdf1 = df.select("product_id", "product_name", "category", "rn_asc")
sdf2 = df.select("product_id", "product_name", "category", "rn_dsc")


sdf = sdf1.alias("sdf1").join(
  sdf2.alias("sdf2"), 
  on=[
    (sdf1.category == sdf2.category) & (sdf1.rn_asc == sdf2.rn_dsc)
  ],
  how="inner"
).select("sdf2.product_id", "sdf1.product_name", "sdf1.category")

sdf.orderBy(F.col("category"), F.col("product_id")).show()


#Q2 - STRING SPLIT
data = [
  (1, "arizona456789"),
  (2, "mumbai456709"),
  (3, "mundur459789"),
  (1, "alsaka400987"),
]
schema = ["id", "city_code"]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

df = df.withColumn("col_length", F.length("city_code") - 6)

df = df.withColumn("city", F.expr("substring(city_code,1, length(city_code)-6)"))
df = df.withColumn("code", F.substring(F.col("city_code"),-6, 6))


#Q3 - CREATE COMBINATIONS
data = [
  ("MI",),
  ("CSK",),
  ("RCB",),
  ("KKR",),
  ("RR",),
  ("DC",),
]
schema = ["team_name"]

df = spark.createDataFrame(data=data, schema=schema)
df = df.repartition(1)

sdf_cj = df.alias("df1").crossJoin(df.alias("df2")).select(
  F.col("df1.team_name").alias("team_1"),
  F.col("df2.team_name").alias("team_2"),
)

sdf_cj.cache().count()
sdf_cj.show()

sdf = sdf_cj.filter(F.col("team_1") != F.col("team_2"))
sdf = sdf.withColumn("team_1_2", F.concat(F.col("team_1"), F.lit("-"), F.col("team_2")))

sdf = sdf.withColumn("sorted_team_1_2", F.concat_ws('-', F.array_sort(F.split("team_1_2", "-"))))
w = Window.partitionBy("sorted_team_1_2").orderBy("team_1")
sdf = sdf.withColumn("rn", F.row_number().over(w))

sdf = sdf.filter(F.col("rn") == 1)

sdf = sdf.select("team_1", "team_2")
sdf = sdf.withColumn("fixture", F.concat(F.col("team_1"), F.lit(" vs "), F.col("team_2")))

sdf.show()
