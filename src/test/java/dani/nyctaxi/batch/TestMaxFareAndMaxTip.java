package dani.nyctaxi.batch;

import static dani.nyctaxi.batch.MaxFareAndMaxTipPeriods.MAX_FARE_COMMENT;
import static dani.nyctaxi.batch.MaxFareAndMaxTipPeriods.MAX_TIP_COMMENT;
import static dani.nyctaxi.test.util.IsVeryCloseTo.isVeryCLoseTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dani.nyctaxi.TripDataItem;
import dani.nyctaxi.batch.MaxFareAndMaxTipPeriods;
import dani.nyctaxi.batch.MaxFareAndMaxTipPeriods.Result;
import scala.Tuple4;

public class TestMaxFareAndMaxTip {

	private JavaSparkContext sc;

	private MaxFareAndMaxTipPeriods task = new MaxFareAndMaxTipPeriods();

	@Before
	public void setUp() {
		sc = new JavaSparkContext("local", "SparkedTests");
	}

	@After
	public void tearDown() {
		sc.stop();
		sc = null;
	}

	@Test
	public void test_1() {
		List<Tuple4<String, String, Double, Double>> input = new ArrayList<>();
		input.add(new Tuple4<>("2013-01-01 00:00:00", "2013-01-01 00:30:00",
				10.0, 1.0));
		input.add(new Tuple4<>("2013-01-01 00:00:00", "2013-01-01 00:30:00",
				20.4, 1.0));
		input.add(new Tuple4<>("2013-01-01 00:00:00", "2013-01-01 00:30:00",
				30.0, 1.0));
		// 60.4, 3
		input.add(new Tuple4<>("2013-01-01 00:30:00", "2013-01-01 00:35:00",
				12.2, 5.3));
		input.add(1, new Tuple4<>("2013-01-01 00:30:00", "2013-01-01 00:50:00",
				10.4, 3.4));
		input.add(new Tuple4<>("2013-01-01 00:30:00", "2013-01-01 00:39:00",
				20.0, 1.0));
		// 54.6, 9.7
		input.add(0, new Tuple4<>("2013-01-01 01:00:00", "2013-01-01 01:20:00",
				40.0, 8.3));
		input.add(new Tuple4<>("2013-01-01 01:00:00", "2013-01-01 01:25:00",
				10.0, 1.5));
		// 50.0, 9.8
		input.add(0, new Tuple4<>("2013-01-01 01:30:00", "2013-01-01 01:31:00",
				15.1, 2.0));
		input.add(5, new Tuple4<>("2013-01-01 01:30:00", "2013-01-01 01:31:00",
				16.3, 1.0));
		input.add(new Tuple4<>("2013-01-01 01:30:00", "2013-01-01 01:32:00",
				29.1, 5.0));
		// 60.5,

		JavaRDD<TripDataItem> trips = sc.parallelize(input, 2)
				.map(t -> new TripDataItem("", "", t._1(), t._2(), 1.0, t._3(),
						t._4()));

		List<Result> r = task.getResultRDD(trips).collect();
		Result maxFare = r.stream()
				.filter(c -> MAX_FARE_COMMENT.equals(c.getComment()))
				.collect(Collectors.<Result> toList()).get(0);
		Result maxTip = r.stream()
				.filter(c -> MAX_TIP_COMMENT.equals(c.getComment()))
				.collect(Collectors.<Result> toList()).get(0);

		Assert.assertNotNull(maxTip);
		Assert.assertNotNull(maxFare);

		Assert.assertEquals("2013-01-01 01:30", maxFare.getPeriod());
		Assert.assertEquals("2013-01-01 01:00", maxTip.getPeriod());
		assertThat(60.5, isVeryCLoseTo(maxFare.getValue()));
		assertThat(9.8, isVeryCLoseTo(maxTip.getValue()));
	}

}
