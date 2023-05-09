package mirko.spark.midterm_es3;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;
import scala.Tuple2;

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
			.mapToPair(w -> {
				char[] w_char = w.toLowerCase().toCharArray();
				Arrays.sort(w_char);
				return new Tuple2<>(
					new String(w_char),
					w
				);
			})
			.distinct()
			.groupByKey()
			.filter(p -> Iterables.size(p._2()) > 1)
			.values()
			.saveAsTextFile(outputPath);

		sc.close();
	}
}
