package mirko.spark.sensors;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class E33_RDD {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("Electric Vehicles").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

 		List <Double> rdd = sc
			.textFile(inputPath)
			.mapToDouble(s -> Double.parseDouble(s.split(",")[2]))
			.top(3);

		System.out.println(">>>>>>>>>>>>>>>>>" + rdd);
	
		sc.close();
	}
}
