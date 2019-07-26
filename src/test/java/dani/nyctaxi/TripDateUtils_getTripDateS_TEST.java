package dani.nyctaxi;

import static dani.nyctaxi.TripDateUtils.getTripDateS;
import static dani.nyctaxi.TripDateUtils.toDate;

import java.time.LocalDateTime;

import org.junit.Assert;
import org.junit.Test;

public class TripDateUtils_getTripDateS_TEST {
	
	@Test
	public void test_beginAndEnd_in_Same_Bucket_1() {
		LocalDateTime bucket = getTripDateS("2013-01-01 12:01:02", "2013-01-01 12:01:29");
		Assert.assertEquals(toDate("2013-01-01 12:00:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Same_Bucket_2() {
		LocalDateTime bucket = getTripDateS("2013-01-01 12:42:32", "2013-01-01 12:59:29");
		Assert.assertEquals(toDate("2013-01-01 12:30:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Same_Bucket_3() {
		LocalDateTime bucket = getTripDateS("2013-05-31 12:30:00", "2013-05-31 12:30:01");
		Assert.assertEquals(toDate("2013-05-31 12:30:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Same_Bucket_4() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:00:00", "2013-05-31 13:00:01");
		Assert.assertEquals(toDate("2013-05-31 13:00:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Same_Bucket_5() {
		LocalDateTime bucket = getTripDateS("2013-05-31 12:59:58", "2013-05-31 12:59:59");
		Assert.assertEquals(toDate("2013-05-31 12:30:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Adjacent_Buckets_1() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:29:59", "2013-05-31 13:30:00");
		Assert.assertEquals(toDate("2013-05-31 13:00:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Adjacent_Buckets_2A() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:25:00", "2013-05-31 13:34:00");
		Assert.assertEquals(toDate("2013-05-31 13:00:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Adjacent_Buckets_2B() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:25:00", "2013-05-31 13:34:59");
		Assert.assertEquals(toDate("2013-05-31 13:00:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Adjacent_Buckets_2C() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:25:00", "2013-05-31 13:35:00");
		Assert.assertEquals(toDate("2013-05-31 13:30:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Adjacent_Buckets_3() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:29:57", "2013-05-31 13:30:02");
		Assert.assertEquals(toDate("2013-05-31 13:00:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Adjacent_Buckets_4() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:29:59", "2013-05-31 13:30:01");
		Assert.assertEquals(toDate("2013-05-31 13:30:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_Adjacent_Buckets_5() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:19:59", "2013-05-31 13:49:01");
		Assert.assertEquals(toDate("2013-05-31 13:30:00"), bucket);
	}
	
	@Test
	public void test_beginAndEnd_in_NonAdjacent_Buckets_1() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:19:59", "2013-05-31 17:49:01");
		Assert.assertEquals(toDate("2013-05-31 15:30:00"), bucket);
	}
	
	@Test
	public void test_beginAfterEnd() {
		LocalDateTime bucket = getTripDateS("2013-05-31 13:49:02", "2013-05-31 13:49:01");
		Assert.assertNull(bucket);
	}
	
	

}
