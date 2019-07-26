package dani.nyctaxi.batch;

import static dani.nyctaxi.TripDateUtils.toDate;
import static dani.nyctaxi.batch.AverageSpeeds.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dani.nyctaxi.TripDataItem;
import dani.nyctaxi.batch.AverageSpeeds;
import scala.Tuple3;

import static dani.nyctaxi.test.util.IsVeryCloseTo.isVeryCLoseTo;

public class TestAvgSpeedFunctions {

	private AverageSpeeds task = new AverageSpeeds();

	private JavaSparkContext sc;

	@Before
	public void setUp() {
		sc = new JavaSparkContext("local", "SparkedTests");
	}

	@After
	public void tearDown() {
		sc.stop();
		sc = null;
	}

	private static Function<Tuple3<String, String, Double>, TripDataItem> toTripDataMapper = t -> {
		return new TripDataItem(UUID.randomUUID().toString(),
				UUID.randomUUID().toString(), t._1(), t._2(), t._3(), 1.0, 0.0);
	};

	@Test
	public void test_Flatten1() {
		List<Tuple3<String, String, Double>> input = new ArrayList<>();

		input.add(new Tuple3<>("2013-01-01 00:15:00", "2013-01-01 01:15:00",
				15.0));

		JavaRDD<TripDataItem> inputRDD = sc.parallelize(input, 2)
				.map(toTripDataMapper);
		List<Tuple3<LocalDateTime, Long, Double>> flattened = flattenTripsToPeriodSecondsAndSpeeds(
				inputRDD).collect();
		System.out.println(flattened);

		Assert.assertEquals(3, flattened.size());
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 00:00:00"), 900l, 15.0),
				flattened.get(0));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 00:30:00"), 1800l, 15.0),
				flattened.get(1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 01:00:00"), 900l, 15.0),
				flattened.get(2));

	}

	@Test
	public void test_Flatten2() {
		List<Tuple3<String, String, Double>> input = new ArrayList<>();

		input.add(new Tuple3<>("2013-01-01 00:45:00", "2013-01-01 04:45:00",
				80.0));

		JavaRDD<TripDataItem> inputRDD = sc.parallelize(input, 2)
				.map(toTripDataMapper);
		List<Tuple3<LocalDateTime, Long, Double>> flattened = flattenTripsToPeriodSecondsAndSpeeds(
				inputRDD).collect();
		System.out.println(flattened);

		Assert.assertEquals(9, flattened.size());
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 00:30:00"), 900l, 20.0),
				flattened.get(1 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 01:00:00"), 1800l, 20.0),
				flattened.get(2 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 01:30:00"), 1800l, 20.0),
				flattened.get(3 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 02:00:00"), 1800l, 20.0),
				flattened.get(4 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 02:30:00"), 1800l, 20.0),
				flattened.get(5 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 03:00:00"), 1800l, 20.0),
				flattened.get(6 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 03:30:00"), 1800l, 20.0),
				flattened.get(7 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 04:00:00"), 1800l, 20.0),
				flattened.get(8 - 1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 04:30:00"), 900l, 20.0),
				flattened.get(9 - 1));
	}

	@Test
	public void test_Flatten3() {
		List<Tuple3<String, String, Double>> input = new ArrayList<>();

		input.add(new Tuple3<>("2013-01-01 03:00:00", "2013-01-01 05:00:00",
				36.8));

		JavaRDD<TripDataItem> inputRDD = sc.parallelize(input, 2)
				.map(toTripDataMapper);
		List<Tuple3<LocalDateTime, Long, Double>> flattened = flattenTripsToPeriodSecondsAndSpeeds(
				inputRDD).collect();
		System.out.println(flattened);

		Assert.assertEquals(4, flattened.size());
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 03:00:00"), 1800l, 18.4),
				flattened.get(0));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 03:30:00"), 1800l, 18.4),
				flattened.get(1));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 04:00:00"), 1800l, 18.4),
				flattened.get(2));
		Assert.assertEquals(
				new Tuple3<LocalDateTime, Long, Double>(
						toDate("2013-01-01 04:30:00"), 1800l, 18.4),
				flattened.get(3));
	}

	@Test
	public void test_weightedAverages() {
		List<Tuple3<String, Long, Double>> input = new ArrayList<>();
		input.add(new Tuple3<>("A", 3l, 3.5));
		input.add(new Tuple3<>("A", 2l, 2.0));
		input.add(new Tuple3<>("A", 1l, 6.2));
		input.add(new Tuple3<>("A", 5l, 1.22));
		input.add(new Tuple3<>("B", 900l, 15.0));
		input.add(new Tuple3<>("B", 1800l, 20.0));
		input.add(new Tuple3<>("B", 1800l, 22.0));
		input.add(new Tuple3<>("B", 900l, 18.0));
		input.add(new Tuple3<>("C", 4l, 1.0));
		input.add(new Tuple3<>("C", 2l, 2.0));
		input.add(new Tuple3<>("C", 2l, 2.0));
		input.add(new Tuple3<>("C", 1l, 4.0));

		JavaRDD<Tuple3<String, Long, Double>> inputRDD = sc.parallelize(input,
				3);
		Map<String, Double> avgs = getWeightedAverages(inputRDD).collectAsMap();

		System.out.println(avgs);

		Assert.assertEquals(3, avgs.size());

		assertThat(avgs.get("A"), isVeryCLoseTo("" + 26.8 / 11));
		assertThat(avgs.get("B"), isVeryCLoseTo("" + 19.5));
		assertThat(avgs.get("C"), isVeryCLoseTo("" + 16d / 9));

	}

	@Test
	public void test_getAvgSpeeds_1() {

		List<Tuple3<String, String, Double>> input = new ArrayList<>();

		input.add(new Tuple3<>("2013-01-01 00:15:00", "2013-01-01 01:15:00",
				10.0));
		input.add(new Tuple3<>("2013-01-01 00:45:00", "2013-01-01 01:45:00",
				20.0));

		JavaRDD<TripDataItem> inputRDD = sc.parallelize(input)
				.map(toTripDataMapper);

		JavaRDD<Result> avgSpeedsRDD = task.getResultRDD(inputRDD);

		List<Result> avgSpeeds = avgSpeedsRDD.collect();

		Assert.assertEquals(4, avgSpeeds.size());
		Assert.assertEquals(new Result("2013-01-01 00:00", 10.0),
				avgSpeeds.get(0));
		Assert.assertEquals(new Result("2013-01-01 00:30", 40.0 / 3),
				avgSpeeds.get(1));
		Assert.assertEquals(new Result("2013-01-01 01:00", 50.0 / 3),
				avgSpeeds.get(2));
		Assert.assertEquals(new Result("2013-01-01 01:30", 20.0),
				avgSpeeds.get(3));
	}

	@Test
	public void test_getAvgSpeeds_2() {

		List<Tuple3<String, String, Double>> input = new ArrayList<>();

		input.add(new Tuple3<>("2013-01-01 00:15:00", "2013-01-01 01:15:00",
				15.0));
		input.add(new Tuple3<>("2013-01-01 00:45:00", "2013-01-01 04:45:00",
				80.0));
		input.add(new Tuple3<>("2013-01-01 03:15:00", "2013-01-01 04:15:00",
				30.0));
		input.add(new Tuple3<>("2013-01-01 01:15:00", "2013-01-01 02:15:00",
				20.0));
		input.add(new Tuple3<>("2013-01-01 02:45:00", "2013-01-01 03:30:00",
				16.5));
		input.add(new Tuple3<>("2013-01-01 03:00:00", "2013-01-01 03:45:00",
				21.0));
		input.add(new Tuple3<>("2013-01-01 01:45:00", "2013-01-01 03:30:00",
				31.5));
		input.add(new Tuple3<>("2013-01-01 02:15:00", "2013-01-01 04:15:00",
				42.0));

		JavaRDD<TripDataItem> inputRDD = sc.parallelize(input, 2)
				.map(toTripDataMapper);

		JavaRDD<Result> avgSpeedsRDD = task.getResultRDD(inputRDD);
		List<Result> avgSpeeds = avgSpeedsRDD.collect();

		Assert.assertEquals(10, avgSpeeds.size());
		Assert.assertEquals(new Result("2013-01-01 00:00", 15.0),
				avgSpeeds.get(0));
		Assert.assertEquals(new Result("2013-01-01 00:30", 50.0 / 3),
				avgSpeeds.get(1));
		Assert.assertEquals(new Result("2013-01-01 01:00", 75.0 / 4),
				avgSpeeds.get(2));
		Assert.assertEquals(new Result("2013-01-01 01:30", 98.0 / 5),
				avgSpeeds.get(3));
		Assert.assertEquals(new Result("2013-01-01 02:00", 117.0 / 6),
				avgSpeeds.get(4));
		Assert.assertEquals(new Result("2013-01-01 02:30", 140.0 / 7),
				avgSpeeds.get(5));
		Assert.assertEquals(new Result("2013-01-01 03:00", 248.0 / 11),
				avgSpeeds.get(6));
		Assert.assertEquals(new Result("2013-01-01 03:30", 170.0 / 7),
				avgSpeeds.get(7));
		Assert.assertEquals(new Result("2013-01-01 04:00", 91.0 / 4),
				avgSpeeds.get(8));
		Assert.assertEquals(new Result("2013-01-01 04:30", 20.0),
				avgSpeeds.get(9));
	}

}
