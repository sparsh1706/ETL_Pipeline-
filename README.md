# UCLA MSBA Data Management Project - Impact of Wildfires on Air Pollution Levels

This project processes data using PySpark and DuckDB to generate aggregated CSV files for visualization in Tableau. It involves the following main steps:

1. Setting up a Google Cloud instance.

2. Installing required software and dependencies.

3. Cloning the repository containing the input data and pipeline scripts.

4. Running the data processing pipeline to generate output CSVs that can be used in Tableau or other visualization tools.

## 1. Activating Google Cloud and Creating an Instance

Create a Google Cloud account if you don't already have one

Activate billing for your account to access compute resources.

Launch your VM instance by navigating to Compute Engine -> VM Instance -> Create Instance and selecting your preferences

## 2. Installing all pre-requisites required

### Install Java (required for PySpark)

sudo apt install openjdk-11-jdk -y

### Verify Java Installation

java -version

### Install Python and pip

sudo apt install python3 python3-pip python3-venv -y

### Verify Python installation

python3 --version 

pip3 --version

### Install Hadoop (required for PySpark)

wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

sudo tar -xzvf hadoop-3.3.6.tar.gz -C /opt/

sudo mv /opt/hadoop-3.3.6 /opt/hadoop

### Set Hadoop environment variables (add these lines to your .bashrc):

echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc

echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc

source ~/.bashrc

### Verify Hadoop installation:

hadoop version

### Dowload the files from google sheets and upload to the GCP

Pollution data: https://drive.google.com/file/d/1vdUKFzU0-SG7IaZFACPymmEy5WO5ad0M/view?usp=sharing

Fire data: https://drive.google.com/file/d/1U9cHiZIfkBjSviUwvjrpoRDxiPA9VTKE/view?usp=sharing

### Install DuckDB
pip install duckdb

### Install PySpark
pip install pyspark

## 3. Cloning this Repository to Get Input Files and Pipeline Scripts

git clone https://github.com/Prof-Rosario-UCLA/team16.git

### Navigate into the directory 

Make sure the following files are present after cloning:

1. 2 Input CSV files (used by PySpark script)

2. spark_job.py: PySpark script for initial data processing and joining CSV files into Parquet format. (update the location CSV files)

3. queries_v2.sql: SQL queries for DuckDB aggregations. (use the output file of spark_job as base)

4. pipeline_test_2.sh: Python script orchestrating the entire pipeline execution. (update the location spark_job, queries_v2, output CSV location)

## 4. Running the Data Processing Pipeline

Run the complete pipeline script (pipeline_test_2.sh) which will execute:

PySpark processing (spark_job.py) to create Parquet files.

DuckDB SQL queries (queries_v2.sql) to generate aggregated CSV outputs.

Execute the pipeline with:

bash pipeline_test_2.sh

After successful execution, aggregated output CSV files will be available in the output directory

## Next Steps: Visualization in Tableau

Once you have generated your aggregated CSV outputs, you can import these files directly into Tableau Desktop or Tableau Cloud for visualization purposes.

Tableau public dashboard: https://public.tableau.com/app/profile/sparsh.sharma1162/viz/ImpactofWildfiresonAirQualityDashboard/Dashboard?publish=yes
