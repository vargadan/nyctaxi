package dani.nyctaxi.batch;

import java.io.Serializable;
import java.text.DecimalFormat;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import dani.nyctaxi.TripDataItem;
import scala.Tuple2;

/**
 * Calculates the driver who drove the most distance each year.
 * 
 */
public class DriverOfTheYear implements BatchTask<DriverOfTheYear.Result> {

	@Override
	public JavaRDD<Result> getResultRDD(JavaRDD<TripDataItem> trips) {
		// 1st we create an RDD with distance drivern per driver per year
		JavaPairRDD<Tuple2<Integer, String>, Double> drivenDistances = getDrivenDistances(
				trips);

		return drivenDistances
				// then we map these distances key value pairs where the key is
				// the year and the value is a tuple of driver and his distance
				// in the year
				.mapToPair(kv -> new Tuple2<Integer, Tuple2<String, Double>>(
						kv._1._1, new Tuple2<>(kv._1._2, kv._2)))
				// so that each driver-distance values in each year can be
				// reduced to the one with the highest distance driven
				.reduceByKey((t1, t2) -> t1._2() > t2._2() ? t1 : t2)
				// finally the reduced key value pairs are transformed into the
				// expected format
				.map(kv -> new Result(kv._1, kv._2._1, kv._2._2));
	}

	protected static JavaPairRDD<Tuple2<Integer, String>, Double> getDrivenDistances(
			JavaRDD<TripDataItem> trips) {
		return trips.mapToPair(t -> new Tuple2<Tuple2<Integer, String>, Double>(
				new Tuple2<>(t.getPeriod().getYear(), t.getDriver()),
				t.getDistance())).reduceByKey((a, b) -> a + b);
	}

	static public class Result implements Serializable {

		private static final long serialVersionUID = 3944280641083454892L;

		private static final DecimalFormat df = new DecimalFormat("#.00");

		private final Integer year;

		private final String driver;

		private final Double distance;

		public Result(Integer year, String driver, Double distance) {
			super();
			this.year = year;
			this.driver = driver;
			this.distance = distance;
		}

		public Integer getYear() {
			return year;
		}

		public String getDriver() {
			return driver;
		}

		public String getDistance() {
			return df.format(distance);
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder(31, 1).append(year).append(driver)
					.append(distance).toHashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Result) {
				Result or = (Result) obj;
				return new EqualsBuilder().append(this.year, or.year)
						.append(this.driver, or.driver)
						.append(this.distance, or.distance).build();
			} else {
				return false;
			}
		}
	}

	@Override
	public Class<Result> getResultItemClass() {
		return Result.class;
	}

}
