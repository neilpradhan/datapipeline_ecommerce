# Data Pipeline Ecommerce

This repository contains a data pipeline for an eCommerce platform using Apache Airflow, Docker, and Docker Compose. The pipeline ingests orders and inventory data, validates and processes it, and loads the results into an SQLite database.


## Prerequisites

- Docker
- Docker Compose

## Setup Instructions

1. **Clone the repository**:

   ```sh
   git clone https://github.com/yourusername/datapipeline_ecommerce.git
   cd datapipeline_ecommerce


2. **Build the Docker image**:

   ```sh
   docker build -t ecommerce-pipeline .

3. **Start the services using Docker Compose**:

   ```sh
   docker-compose up -d


Wait for Airflow to start:

Wait for about 1.5 to 2 minutes for Airflow to fully start.


4. **Access the Airflow web UI and trigger the DAG**:

Open your browser and go to http://localhost:8080/. 

Log in with the following credentials:

Username: admin\
Password: admin 

Trigger the DAG:

In the Airflow UI, find the ecommerce_pipeline DAG.
Click on the DAG and press the "Trigger DAG" button to start the pipeline.

a new folder "processed_data" will be created within your directory with 4 folders namely  good_data, bad_data, combined_data, and reports for the results
