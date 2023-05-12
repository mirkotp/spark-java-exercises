package mirko.spark.sensors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class E03_RDD {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, Double> temps = 
			sc
				.textFile(inputPath)
				.mapToPair(s -> {
					String[] fields = s.split(",");

					return new Tuple2<>(
						fields[1],
						Double.parseDouble(s.split(",")[2])
					);
				})
				.cache();

		double min = temps
			.values()
			.mapToDouble(s -> s)
			.min();

		temps
			.filter(r -> r._2()
			.equals(min))
			.keys()
			.saveAsTextFile(outputPath);
	
		sc.close();
	}
}
