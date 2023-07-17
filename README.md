# Flights-pipeline

## Description
The goal of this project is to create an ETL pipeline for processing and storing data from the Flightradar24 API to a HDFS, by using PySpark framework.
The data source represents air flights, airports and airlines worldwide. 
The Extract step includes retriving data from the API, with the necessary details for each row. A check for anomalies and for data sparcity is made too.
The Transform step includes the type conversion of the data, deletion of null/duplicate rows and anomalies.
The Load step includes storing the data in folders with a time-stamped nomenclature.

This pipeline provides the following indicators: 
1)The airline with the most active flights
2)For each continent, the airline with the most active regional flights
3)The current flight with the longest route
4)For each continent, the average flight length

## Install
python -m venv .env
source .env/bin/activate

pip install -r requirements.txt

## Comments
