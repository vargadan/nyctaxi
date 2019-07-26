package dani.nyctaxi;

import static dani.nyctaxi.Consts.*;
import static dani.nyctaxi.TripDateUtils.toDate;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class StatisticsRunner {
	
	private static SparkConf sc;
	
	private static JavaSparkContext jsc;
	
	private static String inputFile;
	
	private static String outputRoot;
	
	private static SQLContext sqlContext;

	public static void main(String... args) {

		{
			sc = new SparkConf().setAppName(StatisticsRunner.class.getName());
			jsc = new JavaSparkContext(sc);
			sqlContext = new SQLContext(jsc);
			
			jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY);
			jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY);
			
			inputFile = jsc.isLocal() ? LOCAL_INPUT_FILE : HADOOP_INPUT_FILE;
			outputRoot = jsc.isLocal() ? LOCAL_OUTPUT_ROOT : S3_OUTPUT_ROOT;
		}

		Instant startX = Instant.now();

		try {			

			DataFrame inputDF = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "false")
					.option("header", "false")
					.schema(INPUT_SCHEMA)
					.load(inputFile);
			
			inputDF = inputDF.select("dropoff_datetime_s");
			
			JavaPairRDD<LocalDateTime, Long> trafficPerMinutes = trafficPerMinutes(inputDF);
			JavaPairRDD<LocalDateTime, Long> trafficPerHours = trafficPerHours(trafficPerMinutes);
			JavaPairRDD<LocalDateTime, Long> trafficPerDays = trafficPerDays(trafficPerHours);
			
			List<Result> busiestMinutes = trafficPerMinutes.map(t -> new Result(t._1, t._2.doubleValue())).top(20);
			List<Result> busiestHours = trafficPerHours.map(t -> new Result(t._1, t._2.doubleValue())).top(20);
			List<Result> busiestDays = trafficPerDays.map(t -> new Result(t._1, t._2.doubleValue())).top(20);
		
			busiestMinutes = new ArrayList<>(busiestMinutes);
			busiestHours = new ArrayList<>(busiestHours);
			busiestDays = new ArrayList<>(busiestDays);
			
			Collections.sort(busiestMinutes, Comparator.comparing(Result::getValue));
			Collections.sort(busiestHours, Comparator.comparing(Result::getValue));
			Collections.sort(busiestDays, Comparator.comparing(Result::getValue));
			
			saveAsCSV2(busiestMinutes, "busiestMinutes.csv");
			saveAsCSV2(busiestHours, "busiestHours.csv");
			saveAsCSV2(busiestDays, "busiestDays.csv");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Instant endX = Instant.now();
			
			long duration = ChronoUnit.SECONDS.between(startX, endX);
			System.out.println("...was running for " + duration + " seconds.");
			
			jsc.stop();
		}
		
	}
	
	final static Function2<Long, Long, Long> SUM = (c1, c2) -> c1 + c2;
	
	private static final JavaPairRDD<LocalDateTime, Long> trafficPerMinutes(DataFrame inputDF){
		return inputDF.toJavaRDD().mapToPair(row -> new Tuple2<LocalDateTime, Long>(toDate(row.getAs(0)).truncatedTo(ChronoUnit.MINUTES), 1l)).reduceByKey(SUM);
	}
	
	private static final JavaPairRDD<LocalDateTime, Long> trafficPerHours(JavaPairRDD<LocalDateTime, Long> trafficPerMinutes){
		return trafficPerMinutes.mapToPair(t -> new Tuple2<LocalDateTime, Long>(t._1.truncatedTo(ChronoUnit.HOURS), t._2)).reduceByKey(SUM);
	}
	
	private static final JavaPairRDD<LocalDateTime, Long> trafficPerDays(JavaPairRDD<LocalDateTime, Long> trafficPerHours){
		return trafficPerHours.mapToPair(t -> new Tuple2<LocalDateTime, Long>(t._1.truncatedTo(ChronoUnit.DAYS), t._2)).reduceByKey(SUM);
	}
	
	public static <T> void saveAsCSV2(List<T> result, String fileName) {
		if (result.isEmpty()) {
			return;
		}
		DataFrame df = sqlContext.createDataFrame(jsc.parallelize(result), result.get(0).getClass());
		df.repartition(1).write().format("com.databricks.spark.csv").option("header", "false").save(outputRoot.concat(fileName));
	}
	
	public static class Result implements Serializable, Comparable<Result> {

		private static final long serialVersionUID = 9016291911620036274L;

		public Result(LocalDateTime period, double value) {
			super();
			this.period = TripDateUtils.dateToString(period);
			this.value = value;
		}
		
		final String period;
		final double value;
		
		public String getPeriod() {
			return period;
		}
		public double getValue() {
			return value;
		}
		
		@Override
		public int compareTo(Result o) {
			return this.period.compareToIgnoreCase(o.period);
		}
	}

}