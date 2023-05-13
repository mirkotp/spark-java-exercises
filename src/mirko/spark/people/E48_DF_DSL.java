package mirko.spark.people;

import org.apache.spark.sql.SparkSession;

public class E48_DF_DSL {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkSession spark = SparkSession.builder()
			.appName("Spark Exercise")
			.getOrCreate();

		spark
			.read()
			.option("delimiter", ",")
			.option("header", "true")
			.option("inferSchema", "true")
			.csv(inputPath)
			.groupBy("name")
			.avg("age")
			.filter("count(*) >= 2")
			.show();
				
		spark.close();
	}
}
