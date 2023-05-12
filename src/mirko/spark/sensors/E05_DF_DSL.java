package mirko.spark.sensors;

import org.apache.spark.sql.SparkSession;

public class E05_DF_DSL {
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
			.filter("_c2 > 20")
			.groupBy("_c0")
			.count()
			.filter("count >= 3")
			.show();
			
		spark.close();
	}
}
