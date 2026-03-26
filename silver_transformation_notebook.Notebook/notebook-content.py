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


# Read Order items Table
df_order_items = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/olist_order_items_dataset.csv")

# read olist_order_payments Table
df_order_payments = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/olist_order_payments_dataset.csv")


df_order_reviews = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .option("multiLine", True) \
    .option("escape", '"') \
    .csv("Files/bronze/olist_order_reviews_dataset.csv")


# Read Products
df_products = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/olist_products_dataset.csv")


# Read Sellers
df_sellers = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/olist_sellers_dataset.csv")


# Read Product Category Translation
df_category_translation = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/bronze/product_category_name_translation.csv")

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

df_customers.show(5)
df_customers.printSchema()

df_geolocation.show(5)
df_geolocation.printSchema()


df_order_items.show(5)
df_order_items.printSchema()

# Order Payments
df_order_payments.show(5)
df_order_payments.printSchema()

# Order Reviews
df_order_reviews.show(5)
df_order_reviews.printSchema()

# Products
df_products.show(5)
df_products.printSchema()

# Sellers
df_sellers.show(5)
df_sellers.printSchema()

# Category Translation
df_category_translation.show(5)
df_category_translation.printSchema()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#remove nulls
df_orders = df_orders.dropna()
df_customers = df_customers.dropna()
df_geolocation = df_geolocation.dropna()
df_order_items = df_order_items.dropna()
df_order_payments = df_order_payments.dropna()
df_order_reviews = df_order_reviews.dropna()
df_products = df_products.dropna()
df_sellers = df_sellers.dropna()
df_category_translation = df_category_translation.dropna()


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
df_customers = df_customers.dropDuplicates()
df_geolocation = df_geolocation.dropDuplicates()
df_order_items = df_order_items.dropDuplicates()
df_order_payments = df_order_payments.dropDuplicates()
df_order_reviews = df_order_reviews.dropDuplicates()
df_products = df_products.dropDuplicates()
df_sellers = df_sellers.dropDuplicates()
df_category_translation = df_category_translation.dropDuplicates()

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
    
df_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")

df_geolocation.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_geolocation")

    
df_order_items.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_order_items")


# Order Payments
df_order_payments.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_payments")


# Order Reviews
df_order_reviews.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_reviews")


# Products
df_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_products")


# Sellers
df_sellers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_sellers")


# Category Translation
df_category_translation.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_category_translation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
