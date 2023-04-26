package mirko.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ElectricVehiclesDF {
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
			.csv(inputPath)
			.groupBy("Make", "Model")
			.count();
			
		df.sort("count")
			.limit(N)
			.write()
			.csv(outputPath + "-bottom");

		df.sort(df.col("count").desc())
			.limit(N)
			.write()
			.csv(outputPath + "-top");
		
		spark.close();
	}
}
