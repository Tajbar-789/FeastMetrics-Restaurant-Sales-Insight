# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, DateType
from pyspark.sql.functions import collect_list, concat_ws, sum, round, desc, max, quarter, month, explode, split, col, \
    count, row_number, avg
from pyspark.sql.window import Window
from config import load_config
from create_database import create_db

# COMMAND ----------

config = load_config()
create_db() ## to create a db at the server if it does not exists

spark = SparkSession.builder.master("local[*]")\
    .config("spark.jars","postgresql-42.7.3.jar")\
    .appName("New").getOrCreate()


# COMMAND ----------


sales_schema = StructType([StructField("Order Id", IntegerType(), True), \
                           StructField("Customer Id", IntegerType(), True), \
                           StructField("Restaurant Id", IntegerType(), True), \
                           StructField("Date Of Order", DateType(), True), \
                           StructField("Order Details", StringType(), True), \
                           StructField("Total Price", DoubleType(), True), \
                           StructField("Delivery Address", StringType(), True), \
                           StructField("Mode Of Payment", StringType(), True), \
                           StructField("Delivery Person Id", IntegerType(), True), \
                           StructField("Rating", DoubleType(), True)])

sales_df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").schema(sales_schema).load(
    "sales_data_file.csv")

# COMMAND ----------

customer_schema = StructType([StructField("Customer Id", IntegerType(), True), \
                              StructField("Customer Name", StringType(), True), \
                              StructField("Customer Phone Number", StringType(), True), \
                              StructField("Address", StringType(), True)])

customer_df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").schema(
    customer_schema).load("customer_data_file.csv")

# COMMAND ----------

restaurant_schema = StructType([StructField("Restaurant Id", IntegerType(), True), \
                                StructField("Restaurant Name", StringType(), True), \
                                StructField("Restaurant Address", StringType(), True)])

restaurant_df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").schema(
    restaurant_schema).load("restaurant_data_file.csv")

# COMMAND ----------

delivery_schema = StructType([StructField("Delivery Person Id", IntegerType(), True), \
                              StructField("Delivery Agent Name", StringType(), True), \
                              StructField("Delivery Agent Phone Num", StringType(), True), \
                              StructField("Delivery Agent Rating", DoubleType(), True)])

delivery_df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").schema(
    delivery_schema).load("delivery_data_file.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Top 100 Customers

# COMMAND ----------

sales_customer_df = sales_df.join(customer_df, ["Customer Id"], "inner")
df1 = sales_customer_df.groupBy(["Customer Id", "Customer Name"]).agg(
    round(sum("Total Price"), 3).alias("Total amount spent")).orderBy(desc("Total amount spent"), "Customer Name")
df1.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","top_customers")\
    .save()

df1.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Restaurants

# COMMAND ----------

sales_restaurant_df = sales_df.join(restaurant_df, ["Restaurant Id"], "inner")
df2 = sales_restaurant_df.groupBy(["Restaurant Id", "Restaurant Name"]).agg(
    max("Restaurant Address").alias("Restaurant Address"), round(sum("Total Price"), 3).alias("Total sales")).orderBy(
    desc("Total sales"), "Restaurant Name")

df2.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","top_restaurants")\
    .save()

df2.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Quaterly Sales for each restaurant

# COMMAND ----------

sales_restaurant_df = sales_restaurant_df.withColumn("Quarter", quarter(sales_restaurant_df["Date Of Order"])) \
    .withColumn("Month", month(sales_restaurant_df["Date Of Order"]))

# COMMAND ----------

df3 = sales_restaurant_df.groupBy(["Restaurant Id", "Restaurant Name", "Quarter"]).agg(
    round(sum("Total Price"), 3).alias("Quaterly Sales")).orderBy(["Restaurant Id", "Quarter"])
df3.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","quarterly_sales")\
    .save()
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Monthly Sales

# COMMAND ----------

df4 = sales_restaurant_df.groupBy(["Restaurant Id", "Restaurant Name", "Month"]).agg(
    round(sum("Total Price"), 3).alias("Monthly Sales")).orderBy(["Restaurant Id", "Month"])
df4.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","monthly_sales")\
    .save()
df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Top 5 most ordered dish

# COMMAND ----------

df5 = sales_df.withColumn("Dish Name", explode(split(col("Order Details"), ",")))
df5 = df5.groupBy("Dish Name").agg(count("*").alias("Num of Time ordered")).orderBy(desc("Num of Time ordered"))
df5.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","top_most_ordered_dish")\
    .save()
df5.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Top Dish Ordered from Each Restaurant

# COMMAND ----------

window1 = Window.partitionBy(["Restaurant Id", "Restaurant Name"]).orderBy("Num of Time ordered")
df6 = sales_restaurant_df.withColumn("Dish Name", explode(split(col("Order Details"), ",")))
df6 = df6.groupBy(["Restaurant Id", "Restaurant Name", "Dish Name"]).agg(count("*").alias("Num of Time ordered"),
                                                                         round(sum("Total Price"), 3).alias(
                                                                             "Total sales")).orderBy(
    desc("Num of Time ordered"))

# COMMAND ----------

df6 = df6.withColumn("rank", row_number().over(window1))
df6=df6.filter(df6["rank"] == 1).select(["Restaurant Id", "Restaurant Name", "Dish Name", "Num of Time ordered", "Total sales"])
df6.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","top_dish_ordered_from_each_restaurant")\
    .save()
df6.show()
# COMMAND ----------

# MAGIC %md
# MAGIC Payment methods used by customers

# COMMAND ----------

df7 = sales_df.groupBy("Mode Of Payment").agg(count("*").alias("Num of Times Used"))
df7.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","payment_methods_used")\
    .save()
df7.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Top rated 10 Delivery Partner

# COMMAND ----------

sales_delivery_df = sales_df.join(delivery_df, ["Delivery Person Id"], "inner")

# COMMAND ----------

df8 = sales_delivery_df.groupBy(["Delivery Person Id", "Delivery Agent Name"]).agg(
    count("*").alias("Number of Delivery made"), round(avg("Rating"), 2).alias("Rating")).orderBy(desc("Rating"))
df8.write.format("jdbc").mode("overwrite")\
    .option("driver","org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://"+config["host"]+":5432/"+config["database"]) \
    .option("user", config["user"]) \
    .option("password", config["password"])\
    .option("dbtable","top_delivery_partner")\
    .save()
df8.show(10)


