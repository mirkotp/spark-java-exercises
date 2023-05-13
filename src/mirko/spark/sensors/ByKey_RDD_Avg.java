package mirko.spark.sensors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ByKey_RDD_Avg {
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
			.combineByKey(
				t -> new Tuple2<>(t, 1), 
				(p, v) -> new Tuple2<>(p._1() + v, p._2() + 1), 
				(p1, p2) -> new Tuple2<>(p1._1() + p2._1(), p1._2() + p2._2()))
			.mapToPair(p -> new Tuple2<>(p._1(), p._2()._1() / p._2()._2()))
			.saveAsTextFile(outputPath);

		sc.close();
	}
}
