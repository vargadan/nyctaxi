package dani.nyctaxi.realtime;

import java.time.LocalDateTime;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
import scala.Tuple3;

public class BestDriver implements RealTimeRunner.Task<BestDriver.Result> {

	@Override
	public Result run(JavaRDD<Tuple3<LocalDateTime, String, Double>> trips) {;
		Tuple2<String, Double> bt = trips
				.mapToPair(t -> new Tuple2<String, Double>(t._2(), t._3()))
				//sum per key
				.reduceByKey((d1, d2) -> d1 + d2)
				//key with most value
				.reduce((kv1, kv2) -> kv1._2 > kv2._2 ? kv1 : kv2);
		return new Result(bt._1, bt._2);
	}
	
	static public class Result {
		
		@Override
		public String toString() {
			return "Result [driver=" + driver + ", distance=" + distance + "]";
		}

		private final String driver;
		
		private final Double distance;
		
		public Result(String driver, Double distance) {
			super();
			this.driver = driver;
			this.distance = distance;
		}
		
		public String getDriver() {
			return driver;
		}
		
		public Double getDistance() {
			return distance;
		}
	}
	

}
