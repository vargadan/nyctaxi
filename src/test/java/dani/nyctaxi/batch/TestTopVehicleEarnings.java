package dani.nyctaxi.batch;

import static dani.nyctaxi.test.util.IsVeryCloseTo.isVeryCLoseTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import dani.nyctaxi.TripDataItem;
import dani.nyctaxi.batch.TopVehicleEarnings.Result;
import dani.nyctaxi.batch.TopVehicleEarnings.TopVehicleEarnings_PerDay;
import dani.nyctaxi.batch.TopVehicleEarnings.TopVehicleEarnings_PerMonth;
import dani.nyctaxi.batch.TopVehicleEarnings.TopVehicleEarnings_PerYear;
import scala.Tuple3;

public class TestTopVehicleEarnings {

	private JavaSparkContext sc;

	JavaRDD<TripDataItem> trips;

	@Before
	public void setUp() {
		sc = new JavaSparkContext("local", "SparkedTests");
		trips = sc.parallelize(input, 2).map(t -> new TripDataItem(t._2(), "x",
				t._1(), t._1(), 1d, t._3(), 0d));
	}

	@After
	public void tearDown() {
		sc.stop();
		sc = null;
	}

	final List<Tuple3<String, String, Double>> input = new ArrayList<>();

	{
		input.add(0, new Tuple3<>("2013-01-01 10:00:00", "1", 8.1));
		input.add(0, new Tuple3<>("2013-01-01 10:30:00", "1", 2.1));

		input.add(1, new Tuple3<>("2013-02-02 11:00:00", "1", 8.01));
		input.add(1, new Tuple3<>("2013-02-02 11:30:00", "1", 11.0));

		input.add(2, new Tuple3<>("2013-03-03 10:00:00", "1", 32.4));
		input.add(0, new Tuple3<>("2013-03-03 10:30:00", "1", 1.01));
		input.add(4, new Tuple3<>("2013-03-04 10:30:00", "1", 2.0));
		input.add(4, new Tuple3<>("2013-03-04 10:30:00", "1", 3.0));
		input.add(4, new Tuple3<>("2013-03-04 10:30:00", "1", 4.0));
		input.add(4, new Tuple3<>("2013-03-04 10:30:00", "1", 5.6));

		// y 78.0
		// m 48.01
		// d

		input.add(4, new Tuple3<>("2013-01-01 10:00:00", "2", 3.1));
		input.add(4, new Tuple3<>("2013-01-01 10:30:00", "2", 1.1));
		input.add(4, new Tuple3<>("2013-01-01 11:00:00", "2", 6.1));

		input.add(1, new Tuple3<>("2013-02-02 10:00:00", "2", 5.2));
		input.add(1, new Tuple3<>("2013-02-02 10:30:00", "2", 7.1));
		input.add(1, new Tuple3<>("2013-02-02 11:00:00", "2", 9.1));

		input.add(2, new Tuple3<>("2013-03-03 10:30:00", "2", 10.0));
		input.add(new Tuple3<>("2013-03-03 10:30:00", "2", 20.7));
		input.add(7, new Tuple3<>("2013-03-04 10:00:00", "2", 4.3));
		input.add(new Tuple3<>("2013-03-04 11:00:00", "2", 5.0));
		input.add(0, new Tuple3<>("2013-03-04 11:30:00", "2", 6.0));
		// 77.3
		// 10.3,21.4,46(31,15)

		input.add(8, new Tuple3<>("2013-01-01 10:30:00", "3", 6.2));
		input.add(1, new Tuple3<>("2013-01-01 11:00:00", "3", 2.7));

		input.add(3, new Tuple3<>("2013-02-02 10:30:00", "3", 11.1));
		input.add(0, new Tuple3<>("2013-02-02 11:00:00", "3", 8.2));
		input.add(0, new Tuple3<>("2013-02-02 11:00:00", "3", 2.2));

		input.add(0, new Tuple3<>("2013-03-03 10:30:00", "3", 32.1));
		input.add(5, new Tuple3<>("2013-03-03 11:00:00", "3", 15.0));
		// 79.5
		// 10.2,21.5,48

		input.add(1, new Tuple3<>("2013-01-01 10:00:00", "4", 6.2));
		input.add(1, new Tuple3<>("2013-01-01 10:00:00", "4", 1.9));
		input.add(1, new Tuple3<>("2013-01-01 10:00:00", "4", 2.1));

		input.add(9, new Tuple3<>("2013-02-02 10:00:00", "4", 5.2));
		input.add(0, new Tuple3<>("2013-02-02 10:00:00", "4", 9.4));
		input.add(3, new Tuple3<>("2013-02-02 10:00:00", "4", 6.5));

		input.add(9, new Tuple3<>("2013-03-03 10:00:00", "4", 32.0));
		input.add(6, new Tuple3<>("2013-03-03 10:00:00", "4", 14.2));
		input.add(0, new Tuple3<>("2013-03-03 10:00:00", "4", 1.8));
		// 79.3
		// 10.2,21.1,48
	}

	@Test
	public void test_DAY() {
		List<Result> topEarners = new TopVehicleEarnings_PerDay()
				.getResultRDD(trips).collect();
		System.out.println(topEarners);
		Assert.assertEquals(4, topEarners.size());

		Result top1 = topEarners.get(0);
		Assert.assertEquals("2013-01-01", top1.getPeriod());
		Assert.assertEquals("2", top1.getVehicle());
		assertThat(10.3, isVeryCLoseTo(top1.getEarnings()));

		Result top2 = topEarners.get(1);
		Assert.assertEquals("2013-02-02", top2.getPeriod());
		Assert.assertEquals("3", top2.getVehicle());
		assertThat(21.5, isVeryCLoseTo(top2.getEarnings()));

		Result top3 = topEarners.get(2);
		Assert.assertEquals("2013-03-03", top3.getPeriod());
		Assert.assertEquals("4", top3.getVehicle());
		assertThat(48.0, isVeryCLoseTo(top3.getEarnings()));

		Result top4 = topEarners.get(3);
		Assert.assertEquals("2013-03-04", top4.getPeriod());
		Assert.assertEquals("2", top4.getVehicle());
		assertThat(15.3, isVeryCLoseTo(top4.getEarnings()));
	}

	@Test
	public void test_MONTH() {
		List<Result> topEarners = new TopVehicleEarnings_PerMonth()
				.getResultRDD(trips).collect();
		System.out.println(topEarners);
		Assert.assertEquals(3, topEarners.size());

		Result top1 = topEarners.get(0);
		Assert.assertEquals("2013-01", top1.getPeriod());
		Assert.assertEquals("2", top1.getVehicle());
		assertThat(10.3, isVeryCLoseTo(top1.getEarnings()));

		Result top2 = topEarners.get(1);
		Assert.assertEquals("2013-02", top2.getPeriod());
		Assert.assertEquals("3", top2.getVehicle());
		assertThat(21.5, isVeryCLoseTo(top2.getEarnings()));

		Result top3 = topEarners.get(2);
		Assert.assertEquals("2013-03", top3.getPeriod());
		Assert.assertEquals("1", top3.getVehicle());
		assertThat(48.01, isVeryCLoseTo(top3.getEarnings()));
		;
	}

	@Test
	public void test_YEAR() {
		List<Result> topEarners = new TopVehicleEarnings_PerYear()
				.getResultRDD(trips).collect();
		System.out.println(topEarners);
		Assert.assertEquals(1, topEarners.size());
		Result top = topEarners.get(0);
		Assert.assertEquals("2013", top.getPeriod());
		Assert.assertEquals("4", top.getVehicle());
		Assert.assertEquals("79.30", top.getEarnings());
	}
}
