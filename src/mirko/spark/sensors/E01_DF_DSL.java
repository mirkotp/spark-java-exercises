package mirko.spark.sensors;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class E01_DF_DSL {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkSession spark = SparkSession.builder()
			.appName("Spark Exercise")
			.getOrCreate();

		spark
			.read()
			.option("delimiter", ",")
			.option("inferSchema", "true")
			.csv(inputPath)
			.agg(
				functions.avg("_c2"),
				functions.min("_c2"),
				functions.max("_c2"))
			.show();
				
		spark.close();
	}
}
