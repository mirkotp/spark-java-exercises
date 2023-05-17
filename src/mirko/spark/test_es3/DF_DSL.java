package mirko.spark.test_es3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DF_DSL {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkSession spark = SparkSession.builder()
			.appName("Spark Exercise")
			.getOrCreate();

		Dataset<Row> df = spark.read()
			.option("inferSchema", "true")
			.option("delimiter", ",")
			.option("header", true)
			.csv(inputPath)
			.filter("date = '2023-05-15'")
			.groupBy("location")
			.agg(functions.sum("consumption").as("total_cons"))
			.orderBy(functions.col("total_cons").desc())
			.cache();
		
		double max_cons = df.as(Encoders.bean(Result.class)).first().getTotal_cons();

		df
			.filter("total_cons = " + max_cons)
			.select("location")
			.show();

		spark.close();
	}
}