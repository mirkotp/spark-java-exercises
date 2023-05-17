package mirko.spark.test_es1;

import java.util.ArrayList;
import java.util.List;
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
			.flatMapToPair((line) -> {
				String[] fields = line.split(",");
				
				List<Tuple2<String, Integer>> list = new ArrayList<>();
				if(fields[1].equals("Italian")) {
					list.add(new Tuple2<>(fields[3], 1));
				}

				return list.iterator();
			})
			.countByKey();

		for (Map.Entry<String, Long> entry : m.entrySet()) {
			if(entry.getValue() > 10) {
				System.out.println(">>>>>>> " + entry.getKey());
			}
		}

		sc.close();
	}
}
