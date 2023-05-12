package mirko.spark.sensors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class E02_DF_DSL {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkSession spark = SparkSession.builder()
			.appName("Spark Exercise")
			.getOrCreate();

		Dataset<Row> variances = spark
			.read()
			.option("delimiter", ",")
			.option("inferSchema", "true")
			.csv(inputPath)
			.groupBy("_c1")
			.agg(functions.variance("_c2").as("var"))
			.cache();

		double maxVar = variances
			.groupBy()
			.max()
			.first()
			.getDouble(0);

		variances
			.filter("var = " + maxVar)
			.show();
	}
}
