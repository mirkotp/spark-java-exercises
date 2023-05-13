package mirko.spark.sensors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class ByKey_RDD_Variance {
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
				t -> new Tuple3<>(t * t, t, 1),
				(p, v) -> new Tuple3<>(p._1() + (v * v), p._2() + v, p._3() + 1), 
				(p1, p2) -> new Tuple3<>(p1._1() + p2._1(), p1._2() + p2._2(), p1._3() + p2._3()))
			.mapToPair(p -> new Tuple2<>(p._1(), p._2()._1() / p._2()._3() - Math.pow(p._2()._2() / p._2()._3(), 2)))
			.saveAsTextFile(outputPath);

		sc.close();
	}
}
