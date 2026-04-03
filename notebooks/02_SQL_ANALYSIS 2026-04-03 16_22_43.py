# Databricks notebook source
# MAGIC %md
# MAGIC #### TOP CUSTOMERS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id , SUM(total_amount) AS total_spent FROM fact_orders Group BY customer_id ORDER BY total_spent DESC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### REVENUE DATE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_date , SUM(total_amount) AS Revenue FROM fact_orders  GROUP BY order_date ORDER BY order_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### BEST SELLING PRODUCTS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id , SUM(quantity) AS total_quantity FROM fact_orders GROUP BY product_id ORDER BY total_quantity DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_orders;

# COMMAND ----------

