# Databricks notebook source
customers_df = spark.table("customers")

# COMMAND ----------

display(customers_df)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

orders_df=spark.table("orders")

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

display(orders_df)

# COMMAND ----------

order_items_df = spark.table("orders_items")

# COMMAND ----------

order_items_df.printSchema()

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

products_df = spark.table("products")

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

display(products_df)

# COMMAND ----------

from pyspark.sql.functions import col

customers_df = customers_df.dropna()
orders_df = orders_df.dropna()
order_items_df = order_items_df.dropna()
products_df = products_df.dropna()

# COMMAND ----------

customers_df = customers_df.dropDuplicates()
orders_df = orders_df.dropDuplicates()
order_items_df = order_items_df.dropDuplicates()
products_df = products_df.dropDuplicates()
display(customers_df)
display(orders_df)
display(order_items_df)
display(products_df)

# COMMAND ----------

order_details_df = order_items_df.join(products_df,"product_id","inner").join(orders_df,"order_id","inner")

# COMMAND ----------

display(order_details_df)

# COMMAND ----------

from pyspark.sql.functions import col
order_details_df = order_details_df.withColumn("total_amount",col("price")*col("quantity"))
display(order_details_df)

# COMMAND ----------

order_details_df.write.mode("overwrite") .saveAsTable("processed_orders")


# COMMAND ----------

spark.sql(" SELECT * FROM processed_orders ").show()

# COMMAND ----------

from pyspark.sql.functions import col
final_df = order_details_df.select("order_id","customer_id","product_id","quantity","price","total_amount","order_date")

final_df.write.mode("overwrite").saveAsTable("fact_orders")


# COMMAND ----------

spark.sql("SELECT * FROM fact_orders").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creation of Dimension Table

# COMMAND ----------

customers_df.write.mode("overwrite").saveAsTable("dim_customers")
products_df.write.mode("overwrite").saveAsTable("dim_products")

# COMMAND ----------

