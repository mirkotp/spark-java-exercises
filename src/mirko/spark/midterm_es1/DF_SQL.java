package mirko.spark.midterm_es1;

import org.apache.spark.sql.SparkSession;

public class DF_SQL {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkSession spark = SparkSession.builder()
			.appName("Spark Exercise")
			.getOrCreate();

		spark.read()
			.option("delimiter", ",")
			.option("header", true)
			.csv(inputPath)
			.createOrReplaceTempView("restaurants");

		spark
			.sql(
				"SELECT city, count(*) " +
				"FROM restaurants " +
				"WHERE main_cuisine_type = \"Italian\" " +
				"GROUP BY city")
			.show();

		spark.close();
	}
}
