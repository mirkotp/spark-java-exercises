package mirko.spark.midterm_es1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDD2 {
	public static void main(String[] args) {		
		final String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		Map<String, Long> m = sc
			.textFile(inputPath)
			.flatMap((line) -> {
				String[] fields = line.split(",");

				List<String> list = new ArrayList<String>();
				if(fields[1].equals("Italian")) {
					list.add(fields[2]);
				} 
				return list.iterator();
			})
			.mapToPair(s -> new Tuple2<>(s, 1))
			.countByKey();

		System.out.println(">>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>");
		System.out.println(m);
		System.out.println(">>>>>>>>>>>");
		System.out.println(">>>>>>>>>>>");

		sc.close();
	}
}
