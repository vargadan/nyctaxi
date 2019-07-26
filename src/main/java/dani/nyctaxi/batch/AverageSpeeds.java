package dani.nyctaxi.batch;

import static dani.nyctaxi.TripDateUtils.getPeriodFragments;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import dani.nyctaxi.TripDataItem;
import dani.nyctaxi.TripDateUtils;
import scala.Tuple2;
import scala.Tuple3;

/**
 * 
 * Calculates averages speeds of taxis in traffic for every 30 minutes long
 * periods. The average takes only vehicles with passengers into consideration.
 * Since the actual velocity of vehicles at times is not available, the speed is
 * calculated from the duration and the distance of the trip. The speed of taxis
 * in each period is calculated using weighted averages; the weight of a trip in
 * a period is its duration (in seconds) within that period. Hence, if a trip
 * stretches throughout a whole period its weight is 1800.
 * 
 * For example, a trip from from 2013-01-01 13:20 to 14:05 of distance 15 miles
 * has an average speed of 20 MPH, and spans the periods of 2013-01-01 13:00
 * (for 10 mins), 13:30 and 14:00 (for 5 mins). Therefore, its 20 MPH speed is
 * taken into account with weights of 600, 1800 and 300 respectively.
 * 
 * Honestly, the main reason of using weighed averages is making the case to
 * demonstrate the use of the "flatMap" function and the calculation of weighted
 * averages using map-reduce.
 *
 */
public class AverageSpeeds implements BatchTask<AverageSpeeds.Result> {

	@Override
	public JavaRDD<Result> getResultRDD(JavaRDD<TripDataItem> input) {
		// First, the trip data is transformed into items of trips per period of
		// speed
		// and duration within the period
		JavaRDD<Tuple3<LocalDateTime, Long, Double>> flattened = flattenTripsToPeriodSecondsAndSpeeds(
				input);
		// Then, the average speeds of trips per period are calculated,
		// where the weight of a trip is its duration within the period
		JavaPairRDD<LocalDateTime, Double> averages = getWeightedAverages(
				flattened);
		// Finally, these averages are transformed into a sorted RDD of Result
		// objects.
		JavaRDD<Result> periodAvgSpeeds = averages.repartition(1).sortByKey()
				.map(kv -> new Result(TripDateUtils.dateToString(kv._1),
						kv._2));
		return periodAvgSpeeds;
	}

	/**
	 * Transforms trip data items into items (as tuple) of a trip per bucker
	 * (datetime), its stretch in seconds within the period (long) and its
	 * average speed (double)
	 * 
	 * @param input
	 *            of trip data items
	 * @return tuples of period as datetime, trip stretch within period in
	 *         seconds as long, and trips average speed as double
	 */
	protected static JavaRDD<Tuple3<LocalDateTime, Long, Double>> flattenTripsToPeriodSecondsAndSpeeds(
			JavaRDD<TripDataItem> input) {
		return input.flatMap(t -> {

			List<Tuple3<LocalDateTime, Long, Double>> tripDates = new ArrayList<>();

			Double durationInHours = t.getDuration().doubleValue() / (60 * 60);
			Double avgSpeedinMPH = t.getDistance() / durationInHours;

			try {
				SortedMap<LocalDateTime, Long> weightedDateBuckets = getPeriodFragments(
						t.getStart(), t.getEnd());
				for (LocalDateTime periodDate : weightedDateBuckets.keySet()) {
					long periodWeight = weightedDateBuckets.get(periodDate);
					tripDates.add(new Tuple3<>(periodDate, periodWeight,
							avgSpeedinMPH));
				}
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
				return null;
			}

			return tripDates;
		});
	}

	/**
	 * Created weighted averages per key of 3 item tuples
	 * 
	 * @param inputRDD
	 *            of tuples of 3 items; where the 1st item is the key, the 2nd
	 *            one is the weight or importance of an item, and the 3rd one is
	 *            the value of the item
	 * 
	 * @return a JavaPairRDD holding the averages for each key
	 */
	protected static <T> JavaPairRDD<T, Double> getWeightedAverages(
			JavaRDD<Tuple3<T, Long, Double>> inputRDD) {
		JavaPairRDD<T, Tuple2<Double, Long>> v1 = inputRDD
				.mapToPair(t -> new Tuple2<>(t._1(),
						new Tuple2<>(t._3() * t._2(), t._2())));
		JavaPairRDD<T, Tuple2<Double, Long>> v2 = v1.reduceByKey(
				(t1, t2) -> new Tuple2<>(t1._1() + t2._1(), t1._2() + t2._2()));
		return v2.mapToPair(
				kv -> new Tuple2<T, Double>(kv._1, kv._2._1 / kv._2._2));
	}

	@Override
	public Class<Result> getResultItemClass() {
		return Result.class;
	}

	public static class Result implements Serializable {

		private static final long serialVersionUID = 9016291911620036274L;

		private static final DecimalFormat df = new DecimalFormat("#.00");

		public Result(String period, double value) {
			super();
			this.period = period;
			this.value = value;
		}

		final String period;

		final double value;

		public String getPeriod() {
			return period;
		}

		public String getValue() {
			return df.format(value);
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder(31, 1).append(value).append(period)
					.toHashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Result) {
				Result or = (Result) obj;
				return new EqualsBuilder().append(this.value, or.value)
						.append(this.period, or.period).build();
			} else {
				return false;
			}
		}
	}

}
