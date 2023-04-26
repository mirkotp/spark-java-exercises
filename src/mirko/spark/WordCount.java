package mirko.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];
	
		SparkConf conf=new SparkConf().setAppName("Spark Word Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> linesOfText = sc.textFile(inputPath);
		JavaRDD<String> words = linesOfText.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		
		JavaPairRDD<String,Integer> interm=words.mapToPair((w)-> new Tuple2<>(w, 1));
		JavaPairRDD<String,Integer> counts=interm.reduceByKey((a, b) -> a + b);
		counts.saveAsTextFile(outputPath);

		sc.close();
	}
}
