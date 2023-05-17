package mirko.spark.test_es2;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDD {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaDoubleRDD wlengths = sc
			.textFile(inputPath)
			.flatMap((line) -> Arrays.asList(line.split(" ")).iterator())
			.mapToPair(s -> new Tuple2<>(s, 1))
			.reduceByKey((a, b) -> a + b)
			.filter(p -> p._2() == 5)
			.mapToDouble(w -> w._1().length())
			.cache();

		System.out.println(">>>>>> " +wlengths.min() + " " + wlengths.max() + " " + wlengths.mean());

		sc.close();
	}
}
