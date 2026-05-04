# Airflow GCS to BigQuery ETL Pipeline

## 📌 Project Overview
This project demonstrates a real-world data pipeline using Apache Airflow, Google Cloud Storage (GCS), and BigQuery.

The pipeline loads employee and department CSV files from a GCS bucket into BigQuery raw tables and performs a transformation to create a final enriched table.

---

## 🔄 Pipeline Flow

GCS Bucket (CSV Files)  
→ Airflow DAG  
→ BigQuery Raw Tables  
→ SQL Transformation  
→ Final BigQuery Table  

---

## ⚙️ Technologies Used

- Apache Airflow  
- Google Cloud Storage (GCS)  
- BigQuery  
- Python  
- SQL  

---

## 📊 Workflow Steps

1. Load employee.csv into BigQuery raw table (`emp_raw`)
2. Load departments.csv into BigQuery raw table (`dep_raw`)
3. Perform SQL transformation to join both tables
4. Create final enriched table (`empDep_in`)

---

## 🧠 Key Concepts Covered

- DAG (workflow definition)
- Tasks & Operators
- Task Dependencies
- Scheduling
- Retry Handling
- ETL Pipeline Design

---

## 🎯  Explanation

This DAG loads data from GCS into BigQuery raw tables using `GCSToBigQueryOperator`.  
Once both datasets are loaded, a transformation is performed using `BigQueryInsertJobOperator` to join the data and create a final table.

Airflow handles orchestration, scheduling, dependencies, retries, and monitoring.

---

## 🚀 Future Improvements

- Add data validation checks
- Use Airflow Sensors for file availability
- Parameterize environment configs
