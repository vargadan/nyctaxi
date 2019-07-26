package dani.nyctaxi.batch;

import static dani.nyctaxi.Consts.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import dani.nyctaxi.TripDataItem;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Runs batch analytics tasks implementing the BatchTask interface. Then saves
 * the results either in an S3 bucket if run in a spark cluster (in EC2) or on
 * the local FS when run in local mode.
 * 
 * When run locally the input data is read from a CSV file in the local FS. 
 * When run on EC2 then it's read from the persistent hdfs.
 * 
 * Since it runs multiple analytics tasks that operate on the same input data, 
 * it caches the input data after invalid and duplicate records are filtered. 
 * Filtering of invalid items is implemented in the dataItemFilter 
 * and duplicateRecuder functions, respectively. 
 * 
 * As the complete input dataset is around 18 Gigabytes, it is uses a more 
 * efficient Kyro serializer as a more efficient alternative to standard java
 * serialization.  
 * 
 */
public class BatchTaskRunner {

	private static SparkConf sc;

	private static JavaSparkContext jsc;

	private static String inputFile;

	private static String outputRoot;

	private static SQLContext sqlContext;

	private static final StringBuilder stats = new StringBuilder();

	final static Function2<TripDataItem, TripDataItem, TripDataItem> duplicateRecuder = (
			t1, t2) -> {
		if (t1.getDistance() > t2.getDistance()) {
			return t1;
		} else if (t1.getDistance() < t2.getDistance()) {
			return t2;
		} else if (t1.getFare() > t2.getFare()) {
			return t1;
		} else if (t1.getFare() < t2.getFare()) {
			return t2;
		} else if (t1.getTip() > t2.getTip()) {
			return t1;
		} else if (t1.getTip() < t2.getTip()) {
			return t2;
		} else if (t1.getDuration() > t2.getDuration()) {
			return t1;
		} else if (t1.getDuration() < t2.getDuration()) {
			return t2;
		} else {
			return t1;
		}
	};

	final static Function<TripDataItem, Boolean> dataItemFilter = t -> {
		boolean v = t != null && t.getStart() != null && t.getEnd() != null
				&& t.getStart().isBefore(t.getEnd()) && t.getDistance() > 0
				&& t.getFare() > 0 && t.getTip() >= 0;
		return v;
	};

	public static void main(String... args) {

		List<String> argsList = new ArrayList<>();
		Arrays.asList(args).forEach(arg -> argsList.add(arg.toUpperCase()));

		List<BatchTask<?>> tasks = new ArrayList<>();

		if (argsList.contains("ALL") || argsList
				.contains(AverageSpeeds.class.getSimpleName().toUpperCase()))
			tasks.add(new AverageSpeeds());
		if (argsList.contains("ALL") || argsList.contains(
				MaxFareAndMaxTipPeriods.class.getSimpleName().toUpperCase()))
			tasks.add(new MaxFareAndMaxTipPeriods());
		if (argsList.contains("ALL") || argsList
				.contains(DriverOfTheYear.class.getSimpleName().toUpperCase()))
			tasks.add(new DriverOfTheYear());
		if (argsList.contains("ALL") || argsList.contains(
				TopVehicleEarnings.class.getSimpleName().toUpperCase())) {
			tasks.add(new TopVehicleEarnings.TopVehicleEarnings_PerDay());
			tasks.add(new TopVehicleEarnings.TopVehicleEarnings_PerMonth());
			tasks.add(new TopVehicleEarnings.TopVehicleEarnings_PerYear());
		}

		if (tasks.isEmpty()) {
			System.out.println(
					"Please specify the tasks to run!\nAt least use ALL as an argument.");
			System.exit(-1);
		}

		{
			sc = new SparkConf().setAppName(BatchTaskRunner.class.getName())
					.set("spark.serializer",
							"org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { TripDataItem.class });
			jsc = new JavaSparkContext(sc);
			sqlContext = new SQLContext(jsc);

			jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",
					AWS_ACCESS_KEY);
			jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",
					AWS_SECRET_KEY);

			inputFile = jsc.isLocal() ? LOCAL_INPUT_FILE
					: argsList.contains("HDFS") ? HADOOP_INPUT_FILE
							: S3_INPUT_FILE;
			outputRoot = jsc.isLocal() ? LOCAL_OUTPUT_ROOT : S3_OUTPUT_ROOT;
		}

		final Instant startX = Instant.now();

		try {

			final DataFrame inputDF = sqlContext.read()
					.format("com.databricks.spark.csv")
					.option("inferSchema", "false").option("header", "false")
					.schema(INPUT_SCHEMA).load(inputFile);

			final Accumulator<Integer> exceptionCounter = jsc.accumulator(0,
					"Item Exception Counter");
			final Accumulator<Integer> invalidCounter = jsc.accumulator(0,
					"Ivalid Item Counter");
			final Accumulator<Integer> validCounter = jsc.accumulator(0,
					"Valid Item Counter");
			final Accumulator<Integer> duplicateCounter = jsc.accumulator(0,
					"Duplicate Item Counter");

			final Function2<TripDataItem, TripDataItem, TripDataItem> duplicateRecuder_WithCounter = (
					t1, t2) -> {
				duplicateCounter.add(1);
				return duplicateRecuder.call(t1, t2);

			};

			final Function<TripDataItem, Boolean> dataItemFilter_WithCounter = t -> {
				boolean v = dataItemFilter.call(t);
				Accumulator<Integer> counter = v ? validCounter
						: invalidCounter;
				counter.add(1);
				return v;
			};

			final JavaRDD<TripDataItem> trips = inputDF.select("vehicle",
					"driver", "pickup_datetime_s", "dropoff_datetime_s",
					"trip_distance", "fare_amount", "tip_amount").toJavaRDD()
					.map(r -> {
						try {
							return new TripDataItem(r.getAs(0), r.getAs(1),
									r.getAs(2), r.getAs(3), r.getAs(4),
									r.getAs(5), r.getAs(6));
						} catch (Exception e) {
							exceptionCounter.add(1);
							return null;
						}
					}).filter(
							dataItemFilter_WithCounter)
					.mapToPair(
							i -> new Tuple2<Tuple3<String, String, LocalDateTime>, TripDataItem>(
									new Tuple3<>(i.getDriver(), i.getVehicle(),
											i.getEnd()),
									i))
					.reduceByKey(duplicateRecuder_WithCounter).values();

			trips.persist(StorageLevel.MEMORY_ONLY_SER());
			// to force caching at the outset
			long count = trips.count();
			System.out.println("TRIPS.COUNT = " + count);

			tasks.forEach(task -> runAndSave(trips, task));

			stats.append("\nValid data items 	: " + validCounter.value());
			stats.append("\nInvalid data items  : " + invalidCounter.value());
			stats.append("\nExceptions       	: " + exceptionCounter.value());
			stats.append("\nDuplicates       	: " + duplicateCounter.value());

		} finally {
			long duration = ChronoUnit.SECONDS.between(startX, Instant.now());
			stats.append(
					"\nAll tasks were running for " + duration + " seconds.");
			System.out.println("* * * * * " + stats + "\n* * * * * *");
			jsc.stop();
		}
	}

	private static <T> void runAndSave(JavaRDD<TripDataItem> input,
			BatchTask<T> job) {
		Instant startX = Instant.now();
		try {
			saveAsCSV(job.getResultRDD(input), job.getClass().getSimpleName(),
					job.getResultItemClass());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			long duration = ChronoUnit.SECONDS.between(startX, Instant.now());
			String stat = job.getClass().getSimpleName() + " was running for "
					+ duration + " seconds.";
			System.out.println(stat);
			stats.append("\n" + stat);
		}
	}

	private static <T, K> void saveAsCSV(JavaRDD<T> result, String fileName,
			Class<?> resultClass) {
		DataFrame df = sqlContext.createDataFrame(result, resultClass);
		df.repartition(1).write().format("com.databricks.spark.csv")
				.option("header", "false").save(outputRoot.concat(fileName));
	}

}