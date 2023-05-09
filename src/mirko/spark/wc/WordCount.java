package mirko.spark.wc;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];
	
		SparkConf conf = new SparkConf().setAppName("Spark Word Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc.textFile(inputPath)
			.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
			.mapToPair((w)-> new Tuple2<>(w, 1))
			.reduceByKey((a, b) -> a + b)
			.saveAsTextFile(outputPath);

		sc.close();
	}
}
