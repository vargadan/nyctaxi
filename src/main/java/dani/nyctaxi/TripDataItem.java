package dani.nyctaxi;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class TripDataItem implements Serializable {

	private final static long serialVersionUID = -1232368505701888428L;

	public TripDataItem(String vehicleId, String driverId, String pickUpDateTime, String dropOffDateTime, Double distance,
			Double fare, Double tip) {
		super();
		this.vehicle = vehicleId;
		this.driver = driverId;
		this.distance = distance != null ? distance : 0;
		this.fare = fare != null ? fare : 0;
		this.tip = tip != null ? tip : 0;
		this.start = TripDateUtils.toDate(pickUpDateTime);
		this.end = TripDateUtils.toDate(dropOffDateTime);
		
		LocalDateTime tripDT = TripDateUtils.getTripDate(start, end);
		this.period = TripDateUtils.getPeriodOfDate(tripDT);		
		this.duration = ChronoUnit.SECONDS.between(start, end);
	}

	private String vehicle;
	
	private String driver;
	
	private LocalDateTime start;
	
	private LocalDateTime end;
	
	private LocalDateTime period;

	private Double distance;
	
	private Double fare;
	
	private Double tip;
	
	private Long duration;

	public String getVehicle() {
		return vehicle;
	}

	public String getDriver() {
		return driver;
	}

	public LocalDateTime getStart() {
		return start;
	}

	public LocalDateTime getEnd() {
		return end;
	}

	public LocalDateTime getPeriod() {
		return period;
	}

	public Double getDistance() {
		return distance;
	}

	public Double getFare() {
		return fare;
	}

	public Double getTip() {
		return tip;
	}
	
	public Long getDuration() {
		return duration;
	}
}
