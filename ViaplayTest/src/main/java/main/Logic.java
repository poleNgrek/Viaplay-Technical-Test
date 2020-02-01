package main;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import static org.apache.spark.sql.functions.*;


public class Logic{
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().appName("Viaplay Test").getOrCreate();		

		StructType schemaSsn = new StructType()
				.add("dt", "date")
				.add("time", "string")
				.add("device_name", "string")
				.add("house_number", "string")
				.add("user_id", "string")
				.add("country_code", "string")
				.add("program_title", "string")
				.add("season", "string")
				.add("season_episode", "string")
				.add("genre", "string")
				.add("product_type", "string");
		
		StructType schemaWhatson = new StructType()
				.add("dt", "date")
				.add("house_number", "string")
				.add("title", "string")
				.add("product_category", "string")
				.add("broadcast_right_region", "string")
				.add("broadcast_right_vod_type", "string")
				.add("broadcast_right_end_date", "date")
				.add("broadcast_right_start_date", "date");
		
		Dataset<Row> dfSsn = spark.read()
				.option("header", "true")
				//.option("timestampFormat","hh:mm:ss")
				.schema(schemaSsn)
				.csv("/home/ben/Interview/Viaplay/DataEngineerTechTestv1.1/started_streams_new.csv");/**/
		
		Dataset<Row> dfWhatson = spark.read()
				.option("header", "true")
				.schema(schemaWhatson)
				.csv("/home/ben/Interview/Viaplay/DataEngineerTechTestv1.1/whatson.csv");		
		
		//register any DataFrame as a temporary table and query it using pure SQL
		dfSsn.createOrReplaceTempView("stream"); 
		dfWhatson.createOrReplaceTempView("whatson");
		
		Dataset<Row> task1 = spark.sql("SELECT b.dt, a.time, a.device_name, b.house_number, a.user_id, "
				+ "a.country_code, a.program_title, a.season, a.season_episode, a.genre, a.product_type, "
				+ "b.broadcast_right_start_date, b.broadcast_right_end_date "
				+ "FROM ("
					+ "SELECT t.dt, t.house_number, t.title, "
					+ "t.broadcast_right_region, t.broadcast_right_start_date , t.broadcast_right_end_date "
					+ "FROM ("
						+ "SELECT house_number, title, broadcast_right_region, MAX(dt) AS date "
						+ "FROM whatson "
						+ "GROUP BY house_number, title, broadcast_right_region"
						+ ") x "
					+ "JOIN whatson t "
					+ "ON x.house_number == t.house_number "
					+ "AND x.date == t.dt AND x.title == t.title AND x.broadcast_right_region == t.broadcast_right_region "
					+ "GROUP BY t.house_number, t.dt, t.title, t.broadcast_right_region, t.broadcast_right_start_date , t.broadcast_right_end_date) b "
				+ "JOIN stream a "
				+ "ON a.house_number == b.house_number "
				+ "WHERE a.product_type = 'tvod' "
				+ "OR a.product_type = 'est'");
		
		
		Dataset<Row> task2 = spark.sql("SELECT dt, program_title, device_name, country_code, product_type, "
				+ "COUNT(DISTINCT user_id) AS unique_users, COUNT(house_number) AS content_count "
				+ "FROM stream "
				+ "GROUP BY dt, program_title, device_name, country_code, product_type "
				+ "ORDER BY unique_users DESC");
		
		
		Dataset<Row> task3 = spark.sql("SELECT b.time AS watched_time, b.genre, b.unique_users AS unique_users "
				+ "FROM ("
					+ "SELECT a.time as time, a.genre as genre, SUM(a.unique_users) AS unique_users "
					+ "FROM ("
						+ "SELECT hour(time) as time, genre, COUNT(DISTINCT user_id) AS unique_users "
						+ "FROM stream "
						+ "GROUP BY time, genre"
						+ ") a "
				+ "GROUP BY a.time, a.genre"
				+ ") b "
				+ "JOIN ("
				+ "SELECT b.time, MAX(b.unique_users) as max "
				+ "FROM ( "
					+ "SELECT a.time as time, a.genre as genre, SUM(a.unique_users) AS unique_users "
					+ "FROM ("
						+ "SELECT hour(time) as time, genre, COUNT(DISTINCT user_id) AS unique_users "
						+ "FROM stream "
						+ "GROUP BY time, genre"
						+ ") a "
					+ "GROUP BY a.time, a.genre"
					+ ") b "
				+ "GROUP BY b.time"
				+ ") c "
				+ "ON b.time = c.time AND b.unique_users = c.max "
				+ "ORDER BY watched_time");

		
		task3.show(300);//testing
		task3.printSchema();

		task1.repartition(1).write().format("com.databricks.spark.csv")
		.option("charset", "UTF-8")
		.option("header", "true")
		.option("mode", "OVERWRITE")
		.option("path", "/home/ben/Interview/Viaplay/DataEngineerTechTestv1.1/task1.csv")
		.save();
		
		task2.repartition(1).write().format("com.databricks.spark.csv")
		.option("charset", "UTF-8")
		.option("header", "true")
		.option("mode", "OVERWRITE")
		.option("path", "/home/ben/Interview/Viaplay/DataEngineerTechTestv1.1/task2.csv")
		.save();
		
		task3.repartition(1).write().format("com.databricks.spark.csv")
		.option("charset", "UTF-8")
		.option("header", "true")
		.option("mode", "OVERWRITE")
		.option("path", "/home/ben/Interview/Viaplay/DataEngineerTechTestv1.1/task3.csv")
		.save();
		
		spark.stop();
	}
}