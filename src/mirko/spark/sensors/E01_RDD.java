package mirko.spark.sensors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class E01_RDD {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaDoubleRDD temps = 
			sc
				.textFile(inputPath)
				.mapToDouble(s -> Double.parseDouble(s.split(",")[2]))
				.cache();

		Double avg = temps.mean();
		Double min = temps.min();
		Double max = temps.max();

		System.out.println(">>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>");
		System.out.println(avg);
		System.out.println(min);
		System.out.println(max);
		System.out.println(">>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>");
	
		sc.close();
	}
}
