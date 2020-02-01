# Viaplay-Technical-Test

## A quick explanation over the code

An ETL project which analyzes customers' behavior and content using the whatson and stream dataset provided by Viaplay.

The project is done with Maven and Spark using the Java API. 

There is only one class called Logic which handles all the ETL logic.

The code does the following procedures:

1. Loads the necessary libraries.

2. Instantiates a *SparkSession* class.

3. Declares Schemas for the data that are expected from the CSVs using the class *StructType*.  

4. Creates DataFrames from the CSV files using the class *Dataset<Row>* and applies the schemas from the previous step.

5. Registers the DataFrames from the previous step as temporary tables.

6. For each task given, executes a Spark SQL query and saves the resulting tables in a DataFrame.

7. Prints the schemas for the DataFrames created in the previous step (for tesing purposes).

8. Exports the DataFrames in CSV format and saves them on disc for further analysis.

9. The Spark session ends

## Dataset Documentation

For the first task the resulting CSV file has the following schema:
'''
root
 |-- dt: date (nullable = true)
 |-- time: string (nullable = true)
 |-- device_name: string (nullable = true)
 |-- house_number: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- country_code: string (nullable = true)
 |-- program_title: string (nullable = true)
 |-- season: string (nullable = true)
 |-- season_episode: string (nullable = true)
 |-- genre: string (nullable = true)
 |-- product_type: string (nullable = true)
 |-- broadcast_right_start_date: date (nullable = true)
 |-- broadcast_right_end_date: date (nullable = true)
'''
For the second task the schema is the following:

root
 |-- dt: date (nullable = true)
 |-- program_title: string (nullable = true)
 |-- device_name: string (nullable = true)
 |-- country_code: string (nullable = true)
 |-- product_type: string (nullable = true)
 |-- unique_users: long (nullable = false)
 |-- content_count: long (nullable = false)
 
 And finally for the last task the generated schema is:

root
 |-- watched_time: integer (nullable = true)
 |-- genre: string (nullable = true)
 |-- unique_users: long (nullable = true)
