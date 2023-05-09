package mirko.spark.midterm_es1;

import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDD {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		Map<String, Long> m = sc
			.textFile(inputPath)
			.mapToPair((line) -> {
				String[] fields = line.split(",");
				return new Tuple2<>(fields[2], fields[1]);
			})
			.filter(r -> r._2().equals("Italian"))
			.countByKey();

		System.out.println(">>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>");
		System.out.println(m);
		System.out.println(">>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>");

		sc.close();
	}
}
