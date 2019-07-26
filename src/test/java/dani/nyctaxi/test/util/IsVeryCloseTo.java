package dani.nyctaxi.test.util;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class IsVeryCloseTo extends TypeSafeMatcher<Double> {

	public IsVeryCloseTo(Double to) {
		super();
		this.to = to;
	}

	private Double to;

	@Override
	public void describeTo(Description desc) {
		desc.appendText("is not close enough to " + to);
	}

	@Override
	protected boolean matchesSafely(Double val) {
		return Math.abs(val - to) < 1e-3;
	}

	@Factory
	public static <T> Matcher<Double> isVeryCLoseTo(String to) {
		double v = Double.parseDouble(to);
		return new IsVeryCloseTo(v);
	}

}
