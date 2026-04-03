# 🛒 E-Commerce ETL Pipeline using PySpark & Databricks

## 📌 Project Overview
This project demonstrates an end-to-end ETL pipeline built using PySpark on Databricks. The pipeline processes e-commerce data and transforms it into a structured format suitable for analytics.

---

## ⚙️ Tech Stack
- PySpark
- Databricks
- SQL

---

## 🔄 ETL Pipeline Steps

### 1. Data Ingestion
- Loaded CSV datasets (customers, products, orders, order_items)

### 2. Data Transformation
- Joined multiple datasets
- Created new column: `total_amount`
- Cleaned and structured data

### 3. Data Modeling
- Designed Star Schema:
  - Fact Table: `fact_orders`
  - Dimension Tables: `dim_customers`, `dim_products`

---

## 📊 SQL Analysis
- Top customers by spending
- Revenue trends over time
- Best-selling products

---

## 🧱 Project Structure

ecommerce-etl-pipeline/
│
├── notebooks/
│ ├── 01_data_processing.py
│ └── 02_sql_analysis.sql


---

## 🚀 Future Improvements
- Integrate AWS S3 for data lake storage
- Use Apache Airflow for orchestration
- Deploy pipeline in production environment


----


## 🧠 Key Learnings
- Built scalable ETL pipeline using PySpark
- Designed data warehouse schema
- Performed analytical queries using SQL


----


## ☁️ Architecture (Planned Cloud Integration)

The pipeline is designed to be extended to a cloud-based architecture:

Databricks (ETL Processing) → AWS S3 (Data Lake) → Amazon Redshift (Data Warehouse)

- Databricks: Used for data ingestion and transformation using PySpark  
- AWS S3: Stores raw and processed data  
- Amazon Redshift: Used for analytics and reporting  
---
