package dani.nyctaxi.realtime;

import static dani.nyctaxi.Consts.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import dani.nyctaxi.TripDataItem;
import dani.nyctaxi.TripDateUtils;
import scala.Tuple3;

public class RealTimeRunner {
	
	private static SparkConf sc;
	
	private static JavaSparkContext jsc;
	
	private static String inputFile;
	
	private static String outputRoot;
	
	private static SQLContext sqlContext;
	
	@FunctionalInterface
	public static interface Task<T> {
		T run(JavaRDD<Tuple3<LocalDateTime, String, Double>> trips);
	}

	public static void main(String... args) {

		{
			sc = new SparkConf().setAppName(RealTimeRunner.class.getName())
					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { TripDataItem.class });
			jsc = new JavaSparkContext(sc);
			sqlContext = new SQLContext(jsc);
			
			jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY);
			jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY);
			
			inputFile = "s3n://nyctaxi-input-split/compelete_sorted_000";
			outputRoot = jsc.isLocal() ? LOCAL_OUTPUT_ROOT : S3_OUTPUT_ROOT;
		}



		try {			

			DataFrame inputDF = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "false")
					.option("header", "false")
					.schema(INPUT_SCHEMA)
					.load(inputFile);
			
			final LocalDateTime beginD = LocalDateTime.of(2013, 1, 1, 0, 0);
			final LocalDateTime endD = LocalDateTime.of(2013, 1, 3, 0, 0);
			
			long totalCount;
			
			final JavaRDD<Tuple3<LocalDateTime, String, Double>> trips;
			{	
				final Broadcast<LocalDateTime> beginB = jsc.broadcast(beginD);
				final Broadcast<LocalDateTime> endB = jsc.broadcast(endD);
				trips = inputDF.select("driver", "dropoff_datetime_s", "trip_distance").toJavaRDD()
						.filter(r -> !r.anyNull())
						.map(row -> {
							String driver = row.getAs(0);
							LocalDateTime bucket = TripDateUtils.toDate(row.getAs(1));
							Double distance = row.getAs(2);
							return new Tuple3<>(bucket, driver, distance);
						}).filter(t -> 
							{
								LocalDateTime dropOff = t._1();
								if (dropOff == null) {
									return false;
								} else {
									return dropOff.isAfter(beginB.value()) && dropOff.isBefore(endB.value());
								}
							});
				totalCount = trips.count();
				trips.persist(StorageLevel.MEMORY_ONLY_SER());
				
			}

			LocalDateTime startD = beginD.plusDays(1);
			
			while (true) {
				final StringBuilder stats = new StringBuilder();
				Instant startX = Instant.now();
				stats.append("* * * * * \n");
				stats.append("totalCount : " + totalCount);
				stats.append("time : " + startD);
				{
					final Broadcast<LocalDateTime> endB = jsc.broadcast(startD);
					final Broadcast<LocalDateTime> beginB = jsc.broadcast(startD.minusHours(24));
					final JavaRDD<Tuple3<LocalDateTime, String, Double>> lessTrips = trips.filter(t -> t._1().isAfter(beginB.value()) && t._1().isBefore(endB.value()));
					stats.append("\nThe best driver of the last 24 HOURS is : " + new BestDriver().run(lessTrips) + "\n\t trips.count = " + lessTrips.count());
				}
				{
					final Broadcast<LocalDateTime> endB = jsc.broadcast(startD);
					final Broadcast<LocalDateTime> beginB = jsc.broadcast(startD.minusHours(4));
					final JavaRDD<Tuple3<LocalDateTime, String, Double>> lessTrips = trips.filter(t -> t._1().isAfter(beginB.value()) && t._1().isBefore(endB.value()));
					stats.append("\nThe best driver of the last 4 HOURS is : " + new BestDriver().run(lessTrips) + "\n\t trips.count = " + lessTrips.count());
				}
				{
					final Broadcast<LocalDateTime> endB = jsc.broadcast(startD);
					final Broadcast<LocalDateTime> beginB = jsc.broadcast(startD.minusHours(1));
					final JavaRDD<Tuple3<LocalDateTime, String, Double>> lessTrips = trips.filter(t -> t._1().isAfter(beginB.value()) && t._1().isBefore(endB.value()));
					stats.append("\nThe best driver of the last 1 HOUR is : " + new BestDriver().run(lessTrips) + "\n\t trips.count = " + lessTrips.count());
				}
				long duration = ChronoUnit.SECONDS.between(startX, Instant.now());
				stats.append("\nAll tasks were running for " + duration + " seconds.");
				stats.append("\n* * * * * * *\n");
				System.out.println(stats);
				if (duration < 20) {
					try {
						System.out.println("....SLEEPING....");
						Thread.sleep(1000 * (20 - duration));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				long totalDuration = ChronoUnit.SECONDS.between(startX, Instant.now());
				startD = startD.plusSeconds(totalDuration);
			}
		} finally {
			jsc.stop();
		}
	}

	public static <T, K> void saveAsCSV(JavaRDD<T> result, String fileName, Class<?> resultClass) {
		DataFrame df = sqlContext.createDataFrame(result, resultClass);
		df.repartition(1).write().format("com.databricks.spark.csv").option("header", "false").save(outputRoot.concat(fileName));
	}

}