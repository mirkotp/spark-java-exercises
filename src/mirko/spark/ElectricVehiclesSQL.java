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
				"(SELECT Make, Model, Count(*) AS count \n" +
				"	FROM ev 							\n" +
				"	GROUP BY Make, Model 				\n" +
				"	ORDER BY count 						\n" +
				"	LIMIT " + N + " 					\n" +
				")										\n" +
				"										\n" +
				"UNION 									\n" +
				"										\n" +
				"(SELECT Make, Model, Count(*) AS count	\n" +
				"	FROM ev 							\n" +
				"	GROUP BY Make, Model				\n" +
				"	ORDER BY count DESC 				\n" +
				"	LIMIT " + N + " 					\n" +
				")										\n" +
				"										\n" +
				"ORDER BY count						  	  ")
			.write()
			.json(outputPath);
		
		spark.close();
	}
}
