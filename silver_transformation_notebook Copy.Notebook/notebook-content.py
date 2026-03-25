# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2deb5d47-3f64-40d5-b918-d2ce1738cd26",
# META       "default_lakehouse_name": "ecommerce_lakehouse",
# META       "default_lakehouse_workspace_id": "522afad0-ed22-4e5f-8e56-257e0ffac738",
# META       "known_lakehouses": [
# META         {
# META           "id": "2deb5d47-3f64-40d5-b918-d2ce1738cd26"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Read Orders Dataset 
df_orders = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/olist_orders_dataset.csv")

# Read Customers Dataset 
df_customers = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/olist_customers_dataset.csv")

# Read GeoLocaion Table
df_geolocation = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/olist_geolocation_dataset.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# handle bad data

df_reviews = spark.read.option("header", True) \
    .option("multiLine", True) \
    .option("escape", '"') \
    .csv("Files/bronze/olist_order_reviews_dataset.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#explore data 
df_orders.show(5)
df_orders.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#remove nulls
df_orders = df_orders.dropna()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix Date Columns

from pyspark.sql.functions import to_timestamp

df_orders = df_orders.withColumn(
    "order_purchase_timestamp",
    to_timestamp("order_purchase_timestamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove Duplicates 
df_orders = df_orders.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to Delta Table 
df_orders.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
