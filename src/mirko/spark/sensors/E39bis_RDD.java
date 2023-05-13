package mirko.spark.sensors;

import java.sql.Date;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class E39bis_RDD {
	public static void main(String[] args) {		
		final String inputPath = args[0];
		final String outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("Spark Exercise").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		sc
			.textFile(inputPath)
			.map(s -> {
				String[] fields = s.split(",");
				Sample sample = new Sample();
				sample.setSensorId(Integer.parseInt(fields[0]));
				sample.setDate(Date.valueOf(fields[1]));
				sample.setTemp(Float.parseFloat(fields[2]));
				return sample;
			})
			.mapToPair(s -> new Tuple2<>(s.getSensorId(), s))
			.groupByKey()
			.mapToPair(p -> {
				ArrayList<Date> l = new ArrayList<>();
				p._2().forEach(s -> {
					if(s.getTemp() > 30.8) l.add(s.getDate());
				});
				
				return new Tuple2<>(p._1(), l);
			})
			.saveAsTextFile(outputPath);
				
		sc.close();
	}
}
