package mirko.spark.midterm_es2;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RDD {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc
			.textFile(inputPath)
			.flatMap((line) -> 
				Arrays.asList(line.split(" ")).iterator())
			.filter(w -> w.length() == 5)
			.distinct()
			.saveAsTextFile(outputPath);

		sc.close();
	}
}
