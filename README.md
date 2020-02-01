# Viaplay-Technical-Test
An ETL project which analyzes customers' behavior and content using the whatson and stream dataset provided by Viaplay.

The project is done with Spark using the Java API and Maven. 

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
