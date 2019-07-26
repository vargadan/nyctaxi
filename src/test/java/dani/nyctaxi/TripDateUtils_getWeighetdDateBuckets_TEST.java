package dani.nyctaxi;

import org.junit.Assert;
import org.junit.Test;

import static dani.nyctaxi.TripDateUtils.getPeriodFragments;
import static dani.nyctaxi.TripDateUtils.toDate;

import java.time.LocalDateTime;
import java.util.SortedMap;

public class TripDateUtils_getWeighetdDateBuckets_TEST {
	
	@Test
	public void test() {
		LocalDateTime startD = toDate("2013-03-01 07:09:10");
		LocalDateTime endD = toDate("2013-03-01 08:19:22");
		
		SortedMap<LocalDateTime, Long> weightedBuckets = getPeriodFragments(startD, endD);
		
		Assert.assertEquals(3, weightedBuckets.size());
	}

}
