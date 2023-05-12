package mirko.spark.sensors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class E01_DF_SQL {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkSession spark = SparkSession.builder()
			.appName("Spark Exercise")
			.getOrCreate();

		Dataset<Row> samples = 
			spark
				.read()
				.option("delimiter", ",")
				.option("inferSchema", "true")
				.csv(inputPath);

		samples.createOrReplaceTempView("samples");
		
		spark.sql("SELECT AVG(_c2), MIN(_c2), MAX(_c2) FROM samples").show();
	
		spark.close();
	}
}
