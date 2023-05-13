package mirko.spark.sensors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ByKey_RDD_Max {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc
			.textFile(inputPath)
			.mapToPair(s -> {
				String[] fields = s.split(",");
				return new Tuple2<>(fields[0], Double.parseDouble(fields[2]));
			})
			.reduceByKey((v1, v2) -> Math.max(v1, v2))
			.saveAsTextFile(outputPath);

		sc.close();
	}
}
