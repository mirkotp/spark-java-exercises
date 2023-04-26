package mirko.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ElectricVehiclesSQL {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];
		final int N = Integer.parseInt(args[2]);

		SparkSession spark = SparkSession.builder()
			.appName("Spark EV DF")
			.getOrCreate();

		Dataset<Row> df = spark
			.read()
			.option("delimiter", ",")
			.option("header", true)
			.csv(inputPath);
		
		df.createOrReplaceTempView("ev");

		spark.sql(
				"(SELECT Make, Model, Count(*) AS count " +
					"FROM ev " +
					"GROUP BY Make, Model " +
					"ORDER BY count " +
					"LIMIT " + N + ") " +
					
				"UNION " +
				
				"(SELECT Make, Model, Count(*) AS count " +
					"FROM ev " +
					"GROUP BY Make, Model " +
					"ORDER BY count DESC " +
					"LIMIT " + N + ") " +
				"ORDER BY count")
			.write()
			.json(outputPath);
		
		spark.close();
	}
}
