# Flights-pipeline

## Description
This project aims to develop a robust ETL (Extract, Transform, Load) pipeline using the PySpark framework to process and store data obtained from the Flightradar24 API into Hadoop Distributed File System (HDFS). The dataset encompasses comprehensive information about air flights, airports, and airlines across the globe.

The extraction phase involves retrieving data from the Flightradar24 API while ensuring that each row contains all the necessary details. Additionally, the pipeline includes thorough checks for anomalies and data sparsity, guaranteeing data integrity.

During the transformation stage, various operations are performed on the dataset. These operations encompass type conversion, removal of null and duplicate rows, and detection of anomalies, resulting in a refined and consistent dataset.

Finally, the data is loaded into folders with a time-stamped naming convention, ensuring easy access and traceability.

This pipeline offers valuable insights through the following key indicators:

1. The airline with the highest number of active flights.
2. For each continent, the airline with the most active regional flights.
3. The current flight with the longest route.
4. For each continent, the average flight length.
   
By leveraging the power of PySpark and HDFS, this project enables efficient and accurate data processing, facilitating in-depth analysis and decision-making based on the extracted insights.

## Install
python -m venv .env
source .env/bin/activate

pip install -r requirements.txt
