package mirko.spark.people;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple2;

public class E47_DF_SQL {
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
			.filter("gender = 'male'")
			.createOrReplaceTempView("people");

			spark
				.sql("SELECT name, age + 1 " +
					"FROM people " +
					"WHERE gender = 'male'")
				.show();
				
		spark.close();
	}
}
