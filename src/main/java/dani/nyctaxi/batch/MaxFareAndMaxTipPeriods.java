package dani.nyctaxi.batch;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import dani.nyctaxi.TripDataItem;
import scala.Tuple2;

import static dani.nyctaxi.TripDateUtils.dateToString;

/** 
 * Returns the periods with highest total fares and tips 
 * 
 * @author dani
 *
 */
public class MaxFareAndMaxTipPeriods implements BatchTask<MaxFareAndMaxTipPeriods.Result> {
	
	public static final String MAX_FARE_COMMENT = "MAX FARE";
	
	public static final String MAX_TIP_COMMENT = "MAX TIP";
	
	@Override
	public JavaRDD<Result> getResultRDD(JavaRDD<TripDataItem> trips) {
	
		//1st and RDD is created that holds only the tips and fares of each period
		JavaPairRDD<LocalDateTime, Tuple2<Double, Double>> faresAndTipsPerBucket = trips
			.mapToPair(i -> new Tuple2<>(i.getPeriod(), new Tuple2<>(i.getFare(), i.getTip())))
			.reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));
		
		//then we cache this rdd as it's the base for further calculations
		//so should not be recalculated over and over again.
		faresAndTipsPerBucket.cache();
		
		//then we carete an RDD holding only the sum fares of each period
		JavaRDD<Tuple2<LocalDateTime, Double>> fares = faresAndTipsPerBucket.map(t -> new Tuple2<>(t._1, t._2._1));
		//and another one holding only the sum tips of each period
		JavaRDD<Tuple2<LocalDateTime, Double>> tips = faresAndTipsPerBucket.map(t -> new Tuple2<>(t._1, t._2._2));

		//the max of fares is calculated using the max method that takes
		//the tuple of highest value using RDD.reduce behind the scenes
		Tuple2<LocalDateTime, Double> maxFare = max(fares);		
		//the max of tips is calculated, too
		Tuple2<LocalDateTime, Double> maxTip = max(tips);
		
		List<Result> results = new ArrayList<>();
		
		results.add(new Result(dateToString(maxFare._1()), maxFare._2(), MAX_FARE_COMMENT));
		results.add(new Result(dateToString(maxTip._1()), maxTip._2(), MAX_TIP_COMMENT));

		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
		
		return jsc.parallelize(results);
	}
	
	/**
	 * Returns the tuple with the max value from tuples of key value pairs, where the value class implements comparable   
	 * 
	 * @param input an RDD of Tuple2 items where the 2nd item of the tuple is Comparable 
	 * @return the tuple with the highest value according to the compareTo function of the value class.
	 */
	private static <K, V extends Comparable<V>> Tuple2<K, V> max(JavaRDD<Tuple2<K, V>> input) {
		return input.reduce((t1, t2) -> t1._2().compareTo(t2._2()) >= 0 ? t1 : t2);
	}

	@Override
	public Class<Result> getResultItemClass() {
		return Result.class;
	}
	
	public static class Result implements Serializable {

		private static final long serialVersionUID = 9016291911620036274L;
		
		private static final DecimalFormat df = new DecimalFormat("#.00"); 

		public Result(String period, double value, String comment) {
			super();
			this.period = period;
			this.value = value;
			this.comment = comment;
		}
		
		final String period;
		final double value;
		final String comment;
		
		public String getPeriod() {
			return period;
		}
		
		public String getValue() {
			return df.format(value);
		}
		
		public String getComment() {
			return comment;
		}
		
		@Override
		public int hashCode() {
			return new HashCodeBuilder(31, 1)
					.append(value).append(period).toHashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Result) {
				Result or = (Result) obj;
				return new EqualsBuilder()
						.append(this.value, or.value)
						.append(this.period, or.period)
						.build();
			} else {
				return false;
			}
		}		
	}

}
