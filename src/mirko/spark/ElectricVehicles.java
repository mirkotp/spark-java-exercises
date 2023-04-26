package mirko.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ElectricVehicles {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];
		final int N = Integer.parseInt(args[2]);

		SparkConf conf=new SparkConf().setAppName("Electric Vehicles").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> csvLines = sc.textFile(inputPath);
		JavaPairRDD<String,Integer> interm = csvLines.mapToPair((line) -> {
			String[] fields = line.split(",");
			return new Tuple2<>(fields[6] + " " + fields[7], 1);
		});
		JavaPairRDD<String, Integer> counts = interm.reduceByKey((a, b) -> a + b);
		JavaPairRDD<Integer, String> swapped = counts.mapToPair((x) -> x.swap());

		sc.parallelize(swapped.sortByKey().take(N)).saveAsTextFile(outputPath + "-bottom");
		sc.parallelize(swapped.sortByKey(false).take(N)).saveAsTextFile(outputPath + "-top");
	
		sc.close();
	}
}
