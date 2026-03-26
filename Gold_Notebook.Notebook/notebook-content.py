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

# LOAD SILVER TABLES

df_orders = spark.table("silver_orders")
df_customers = spark.table("silver_customers")
df_order_items = spark.table("silver_order_items")
df_products = spark.table("silver_products")
df_sellers = spark.table("silver_sellers")
df_payments = spark.table("silver_payments")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CREATE DIMENsION TABLES 

#dim_customers
df_dim_customers = df_customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state"
).dropDuplicates()

#dim_products
df_dim_products = df_products.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
).dropDuplicates()

#dim_sellers
df_dim_sellers = df_sellers.select(
    "seller_id",
    "seller_city",
    "seller_state"
).dropDuplicates()

#dim_date (Very Important)
from pyspark.sql.functions import to_date, year, month, dayofmonth

df_dim_date = df_orders.select(
    to_date("order_purchase_timestamp").alias("order_date")
).dropDuplicates()

df_dim_date = df_dim_date.withColumn("year", year("order_date")) \
    .withColumn("month", month("order_date")) \
    .withColumn("day", dayofmonth("order_date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CREATE FACT TABLES 

df_fact_orders = df_order_items \
    .join(df_orders, "order_id") \
    .join(df_products, "product_id") \
    .join(df_customers, "customer_id") \
    .join(df_sellers, "seller_id") \
    .join(df_payments, "order_id")

# Select Required Columns
df_fact_orders = df_fact_orders.select(
    "order_id",
    "customer_id",
    "product_id",
    "seller_id",
    "order_purchase_timestamp",
    "price",
    "freight_value",
    "payment_value"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write Gold Tables

# FACT TABLE 
df_fact_orders.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("fact_orders")

# DIMENSION TABLE
df_dim_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_customers")

df_dim_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_products")

df_dim_sellers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_sellers")

df_dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate Tables
spark.sql("SHOW TABLES").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sample Business Queries

# Total Revenue
spark.sql("""
SELECT SUM(payment_value) AS total_revenue
FROM fact_orders
""").show()




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Top Products
spark.sql("""
SELECT product_id, SUM(price) AS revenue
FROM fact_orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Monthly Sales
spark.sql("""
SELECT month(order_purchase_timestamp) AS month,
       SUM(payment_value) AS revenue
FROM fact_orders
GROUP BY month
ORDER BY month
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
