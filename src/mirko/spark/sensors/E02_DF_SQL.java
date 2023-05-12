package mirko.spark.sensors;

import org.apache.spark.sql.SparkSession;

public class E02_DF_SQL {
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
			"SELECT _c1, VARIANCE(_c2) as variance " +
			"FROM samples " +
			"GROUP BY _c1"
		).createOrReplaceTempView("vbd");

		spark.sql(
			"SELECT _c1, variance " +
			"FROM vbd " +
			"WHERE variance = (SELECT MAX(variance) FROM vbd)"
		).show();
				
		spark.close();
	}
}
