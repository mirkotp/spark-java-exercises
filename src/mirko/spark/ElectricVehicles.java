package mirko.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ElectricVehicles {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];
		final int N = Integer.parseInt(args[2]);

		SparkConf conf=new SparkConf().setAppName("Electric Vehicles").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<Integer, String> rdd = sc
			.textFile(inputPath)
			.mapToPair((line) -> {
				String[] fields = line.split(",");
				return new Tuple2<>(fields[6] + " " + fields[7], 1);
			})
			.reduceByKey((a, b) -> a + b)
			.mapToPair((x) -> x.swap());

		sc.parallelize(rdd.sortByKey().take(N)).saveAsTextFile(outputPath + "-bottom");
		sc.parallelize(rdd.sortByKey(false).take(N)).saveAsTextFile(outputPath + "-top");
	
		sc.close();
	}
}
