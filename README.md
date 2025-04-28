# Big Data Engineering - Air Quality Analysis 2025
<div style="display: flex; justify-content: center; align-items: center;">
  
  <img src="https://www.splitbrain.org/_media/blog/2024-09/docker.svg?w=200&h=200&tok=e8bc2b" alt="docker" width="40" height="40">

  <img src="https://cdn.iconscout.com/icon/free/png-256/free-mongodb-logo-icon-download-in-svg-png-gif-file-formats--wordmark-programming-langugae-freebies-pack-logos-icons-1175138.png" alt="mongo" width="45" height="45">       

  <img src="https://images.icon-icons.com/112/PNG/512/python_18894.png" alt="python" width="40" height="40">  

  <img src="https://www.svgrepo.com/show/354063/metabase.svg" alt="metabase" width="40" height="40">  

  <img src="https://static-00.iconduck.com/assets.00/hadoop-icon-2048x1535-hnz0inkl.png" alt="hdfs" width="60" height="40"> 

  <img src="https://gitlab.com/uploads/-/system/project/avatar/61199454/apache-airflow.png" alt="airflow" width="45" height="45"> 

  <img src="https://blog.mayadata.io/hs-fs/hubfs/123%20-%20Use%20OpenEBS%20Underneath%20your%20Kafka%20Brokers.png?width=300&name=123%20-%20Use%20OpenEBS%20Underneath%20your%20Kafka%20Brokers.png" alt="kafka" width="35" height="35">  

  <img src="https://images.icon-icons.com/2699/PNG/512/apache_nifi_logo_icon_167863.png" alt="nifi" width="70" height="40">

  <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png" alt="spark" width="70" height="40">    

</div>
<br>

## About the Project

**Course:** Architectures and Systems for Big Data  
**Student:** Matija Maksimović  
**Project Title:** Air Quality Analysis  

The goal of this project is to analyze air quality data using two processing modes:
- **Batch Processing:** Historical air quality data from USA is used. (A total of **10 queries** are performed)
  [USA Air Pollution Dataset](https://www.kaggle.com/datasets/mexwell/us-air-pollution)
- **Streaming Processing:**  Real-time sensor data from USA is used. (A total of **5 queries** are performed)
  [AirVisual API](https://api-docs.iqair.com/#get-started)

## Cleaned Data Structure

### Batch Data:

- **Pollutants:** `NO2 [ppb]`, `O3 [ppm]`, `SO2 [ppb]`, `CO [ppm]`  
  Each pollutant has the following columns:
  - `Max Value (Day)`, `Max Hour (Day)`, `Mean`, `AQI`

- **Location and Time Fields:**
  - `State`, `County`, `City`, `Monitoring Site`
  - `Date`

### Streaming Data:

- **TODO:**
  - `1`
  - `2`

## Technologies

- **Docker**: Containerization platform used for deploying and running the application.  
- **MongoDB**: NoSQL database used for storing transformed data.  
- **Python**: The programming language used to develop the application.  
- **HDFS**: Hadoop Distributed File System for scalable and reliable data storage.  
- **Apache Airflow**: Workflow orchestration tool used for data pipelines.  
- **Apache Kafka**: Distributed streaming platform used for real-time data.  
- **Apache NiFi**: Data integration tool used for automating data flow between systems.  
- **Apache Spark**: Unified analytics engine for large-scale data processing.
- **Metabase**: Open-source business intelligence tool used for visualizing and analyzing data.

## System Architecture

## Queries

**BATCH:**

1. Get the number of unique monitoring locations by state.
2. Get the global NO2 mean value.
3. List the top 5 cities with the highest AQI for O3.
4. What is the average AQI for pollutants (NO2, O3, SO2, CO) in each state?
5. Identify cities with highest combined AQI in winter.
6. Show how highest measured daily pollutant values changed over time.
7. Find the most common hour where the maximum measured values occurred in each county.
8. //TODO
9. //TODO
10. //TODO

**STREAM:**

1. //TODO
2. //TODO
3. //TODO
4. //TODO
5. //TODO




