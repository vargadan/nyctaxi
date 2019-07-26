package dani.nyctaxi;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Various utility methods
 * 
 * @author dani
 *
 */
public class TripDateUtils {

	private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm:ss");

	private final static DateTimeFormatter dateTimeFormatter_OUT = DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm");

	public static LocalDateTime toDate(String s) {
		return LocalDateTime.parse(s, dateTimeFormatter);
	}

	public static String dateToString(LocalDateTime date) {
		if (date == null) {
			return null;
		}
		return dateTimeFormatter_OUT.format(date);
	}

	public static LocalDateTime getTripDateS(String start, String end) {
		return getTripDate(toDate(start), toDate(end));
	}

	public static LocalDateTime getTripDate(LocalDateTime start,
			LocalDateTime end) {
		if (start == null) {
			return null;
		}
		if (end == null) {
			return null;
		}
		long startSecs = start.toEpochSecond(ZoneOffset.UTC);
		long endSecs = end.toEpochSecond(ZoneOffset.UTC);
		long middleSecs = (startSecs + endSecs) / 2;
		LocalDateTime middleDate = LocalDateTime.ofEpochSecond(middleSecs, 0,
				ZoneOffset.UTC);
		int hour = middleDate.getHour();
		int minute = middleDate.getMinute() < 30 ? 0 : 30;
		return LocalDateTime.of(middleDate.getYear(), middleDate.getMonth(),
				middleDate.getDayOfMonth(), hour, minute);
	}

	public static LocalDateTime getPeriodOfDate(LocalDateTime dateTime) {
		int hour = dateTime.getHour();
		int minute = dateTime.getMinute() < 30 ? 0 : 30;
		return LocalDateTime.of(dateTime.getYear(), dateTime.getMonth(),
				dateTime.getDayOfMonth(), hour, minute);
	}

	/**
	 * Calculates the 30 minutes long periods in an interval with the number of
	 * seconds of each period falling within the start and the end of the
	 * interval.
	 * 
	 * @param startD
	 *            date-time object marking the start of the interval
	 * @param endD
	 *            date-time object marking the end of the interval
	 * @return a sorted map of periods as keys with seconds within the interval
	 *         marked by the startD and endD parameters
	 */
	public static SortedMap<LocalDateTime, Long> getPeriodFragments(
			LocalDateTime startD, LocalDateTime endD) {

		if (startD.isAfter(endD)) {
			throw new IllegalArgumentException(
					"oops; Start Date is after end date!!! startDate: " + startD
							+ ", endDate: " + endD);
		}

		LocalDateTime startDBucket = getPeriodOfDate(startD);
		LocalDateTime endDBucket = getPeriodOfDate(endD);

		SortedMap<LocalDateTime, Long> periods = new TreeMap<>();

		if (startDBucket.isEqual(endDBucket)) {
			periods.put(startDBucket, ChronoUnit.SECONDS.between(startD, endD));
		} else {
			Long periodMinDiff = ChronoUnit.MINUTES.between(startDBucket,
					endDBucket);
			if (periodMinDiff >= 30) {

				Long d1 = ChronoUnit.SECONDS.between(startD,
						startDBucket.plusMinutes(30));
				periods.put(startDBucket, d1);

				LocalDateTime currentDBucket = startDBucket;
				for (long i = 1; i < periodMinDiff / 30; i++) {
					currentDBucket = currentDBucket.plusMinutes(30);
					periods.put(currentDBucket, (long) 30 * 60);
				}

				Long d2 = ChronoUnit.SECONDS.between(endDBucket, endD);
				if (d2 > 0) {
					periods.put(endDBucket, d2);
				}
			} else {
				// oops
				throw new IllegalArgumentException(
						"oops; periodMinDiff: " + periodMinDiff + "startDate: "
								+ startD + ", endDate: " + endD);
			}
		}

		return periods;
	}

}
