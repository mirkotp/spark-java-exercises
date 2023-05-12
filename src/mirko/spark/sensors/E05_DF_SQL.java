package mirko.spark.sensors;

import org.apache.spark.sql.SparkSession;

public class E05_DF_SQL {
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
			.createOrReplaceTempView("samples");
			
		spark.sql(
			"SELECT _c0, count(*) " +
			"FROM samples " +
			"WHERE _c2 > 20 " +
			"GROUP BY _c0 " +
			"HAVING count(*) >= 3"
		)
		.show();
				
		spark.close();
	}
}
