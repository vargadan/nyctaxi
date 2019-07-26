package dani.nyctaxi.batch;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import dani.nyctaxi.TripDataItem;
import scala.Tuple2;

/**
 * Calculates top earner vehicles for periods according to a given granularity.
 * The granularity can be daily, monthly and annual; thus top earner vehicles
 * are calculated for every day, month or year, respectively.
 * 
 * The class itself is abstract and its inner subclasses should be instantiated
 * for ease of use, such as: - TopVehicleEarnings_PerDay -
 * TopVehicleEarnings_PerMonth - TopVehicleEarnings_PerYear
 */
public abstract class TopVehicleEarnings
		implements BatchTask<TopVehicleEarnings.Result> {

	private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter
			.ofPattern("yyyy-MM-dd");

	private static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter
			.ofPattern("yyyy-MM");

	/**
	 * Instances of this enum represent a possible granularity (day, month,
	 * year) for the TopVehicleEarnings calculation.
	 * 
	 * The enum instances have 2 function properties:
	 * 
	 * - keyDateTransfomer : is used when creating keys in the
	 * getVehicleEarnings method for the given granularity, these keys are
	 * LocalDateTime instances with only a limited set according to the level of
	 * granularity with other properties set to 0
	 * 
	 * - keyFormatter : is called to format the LocalDateTime instance to a
	 * String in the results as per the granularity; Months and days should not
	 * be displayed for the results of top earners per year
	 */
	private static enum Granularity {

		DAYLY(dt -> LocalDateTime.of(dt.getYear(), dt.getMonth(),
				dt.getDayOfMonth(), 0, 0), dt -> DAY_FORMATTER.format(dt)),

		MONTHLY(dt -> LocalDateTime.of(dt.getYear(), dt.getMonth(), 1, 0, 0),
				dt -> MONTH_FORMATTER.format(dt)),

		ANNUAL(dt -> LocalDateTime.of(dt.getYear(), 1, 1, 0, 0),
				dt -> Integer.toString(dt.getYear()));

		Granularity(Function<LocalDateTime, LocalDateTime> keyDateTransfomer,
				Function<LocalDateTime, String> keyFormatter) {
			this.keyDateTransfomer = keyDateTransfomer;
			this.keyFormatter = keyFormatter;
		}

		private final Function<LocalDateTime, LocalDateTime> keyDateTransfomer;

		private final Function<LocalDateTime, String> keyFormatter;
	}

	private final Granularity granularity;

	private TopVehicleEarnings(Granularity granularity) {
		this.granularity = granularity;
	}

	@Override
	public JavaRDD<Result> getResultRDD(JavaRDD<TripDataItem> input) {
		// 1st we get the vehicle earnings according to the granularity
		JavaPairRDD<Tuple2<LocalDateTime, String>, Double> vehicleEarnings = getVehicleEarnings(
				input, granularity.keyDateTransfomer);
		// then we return the top earners per date unit according to granularity
		return getTopEarnerVehicles(vehicleEarnings, granularity.keyFormatter);
	}

	/**
	 * Calculates vehicle earnings per date unit according to granularity
	 * 
	 * @param input
	 *            the RDD of TripDataItem objects
	 * @param keyDateTransfomer
	 *            transforms the period of a trip into a key according to the
	 *            given granularity
	 * @return the sums of earnings (tips + fares) per vehicle and date unit
	 *         according to keyDateTransfomer
	 */
	protected static JavaPairRDD<Tuple2<LocalDateTime, String>, Double> getVehicleEarnings(
			JavaRDD<TripDataItem> input,
			Function<LocalDateTime, LocalDateTime> keyDateTransfomer) {
		return input
				// 1st the input is transformed to key value pairs where the key
				// is a composite of the vehicleId
				// and date unit returned by the keyDateTransfomer function
				// according to the given granularity,
				// and the value is the earning of the trip (fare + tip)
				.mapToPair(i -> new Tuple2<>(
						new Tuple2<>(keyDateTransfomer.call(i.getPeriod()),
								i.getVehicle()),
						i.getFare() + i.getTip()))
				// then earnings are summed per key
				.reduceByKey((a, b) -> a + b);
	}

	/**
	 * Gets the top earner vehicles for each date unit
	 * 
	 * @param total
	 *            earnings the earnings per vehicle per date unit
	 * @param keyFormatter
	 *            formatter function for date unit in result objects according
	 *            to the granularity
	 * @return an RDD of Result object holding the top earning vehicle its
	 *         earnings and the period as per granularity
	 */
	protected static JavaRDD<Result> getTopEarnerVehicles(
			JavaPairRDD<Tuple2<LocalDateTime, String>, Double> earnings,
			Function<LocalDateTime, String> keyFormatter) {
		return earnings
				//
				.mapToPair(
						kv -> new Tuple2<LocalDateTime, Tuple2<String, Double>>(
								kv._1._1, new Tuple2<>(kv._1._2, kv._2)))
				//
				.reduceByKey((t1, t2) -> t1._2() > t2._2() ? t1 : t2)
				.repartition(1).sortByKey()
				//
				.map(kv -> new Result(keyFormatter.call(kv._1), kv._2._1,
						kv._2._2));
	}

	@Override
	public Class<Result> getResultItemClass() {
		return Result.class;
	}

	/**
	 * Actual implementation of TopVehicleEarnings with DAYly granularity
	 */
	public static class TopVehicleEarnings_PerDay extends TopVehicleEarnings {

		public TopVehicleEarnings_PerDay() {
			super(Granularity.DAYLY);
		}
	}

	/**
	 * Actual implementation of TopVehicleEarnings with MONTHly granularity
	 */
	public static class TopVehicleEarnings_PerMonth extends TopVehicleEarnings {

		public TopVehicleEarnings_PerMonth() {
			super(Granularity.MONTHLY);
		}
	}

	/**
	 * Actual implementation of TopVehicleEarnings with YEARly granularity
	 */
	public static class TopVehicleEarnings_PerYear extends TopVehicleEarnings {

		public TopVehicleEarnings_PerYear() {
			super(Granularity.ANNUAL);
		}
	}

	public static class Result implements Serializable {

		private static final long serialVersionUID = 5450059790598236970L;

		private static final DecimalFormat df = new DecimalFormat("#.00");

		public String getPeriod() {
			return period;
		}

		public String getVehicle() {
			return vehicle;
		}

		public String getEarnings() {
			return df.format(earnings);
		}

		public Result(String period, String vehicle, Double earnings) {
			super();
			this.period = period;
			this.vehicle = vehicle;
			this.earnings = earnings;
		}

		final private String period;

		final private String vehicle;

		final private Double earnings;

		@Override
		public int hashCode() {
			return new HashCodeBuilder(31, 1).append(period).append(vehicle)
					.append(earnings).toHashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Result) {
				Result or = (Result) obj;
				return new EqualsBuilder().append(this.period, or.period)
						.append(this.vehicle, or.vehicle)
						.append(this.earnings, or.earnings).build();
			} else {
				return false;
			}
		}
	}

}
