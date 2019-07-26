package dani.nyctaxi.chart;

import com.googlecode.charts4j.*;

import static com.googlecode.charts4j.Color.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TopEarnerCharsGenerator {
	
	private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private static final int YEAR = 2013;
	
	public static void main(String... args) {
		for (String csvPath : args) {
			System.out.println(getChartUrl(csvPath));
			System.out.println();
		}
	}
	
	public static String getChartUrl(String csvPath) {
		
		List<Double> earningsOfTheYear = new ArrayList<>();
		List<String> months_row1 = new ArrayList<>();
		List<Double> monthPositions_row1 = new ArrayList<>();
		List<String> months_row2 = new ArrayList<>();
		List<Double> monthPositions_row2 = new ArrayList<>();
		
		Double MAX = 0.0;

		try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				int x1 = line.indexOf(",");
				int x2 = line.indexOf(",", x1 + 1);
				
				String sVal = line.substring(0, x1);
				String sDate = line.substring(x1 + 1, x2);
				
				Double earning = Double.valueOf(sVal);
				LocalDate date = LocalDate.parse(sDate, dtf);
				
				if (date.getYear() == YEAR) {
					earningsOfTheYear.add(earning);
					boolean isOddMonth = date.getMonthValue() % 2 == 1;
					List<String> months = isOddMonth ? months_row1 : months_row2;
					List<Double> monthPositions = isOddMonth ? monthPositions_row1 : monthPositions_row2;
					if (!months.contains(date.getMonth().name())) {
						months.add(date.getMonth().name());
						monthPositions.add(100.0 * date.getDayOfYear() / (date.isLeapYear() ? 366 : 365));
					}
					if (earning > MAX) {
						MAX = earning;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		} 
        
     
        Line line1 = Plots.newLine(DataUtil.scale(earningsOfTheYear) , YELLOW);
        line1.setFillAreaColor(LIGHTYELLOW);

        // Defining chart.
        LineChart chart = GCharts.newLineChart(line1);
        chart.setSize(633,473);
        chart.setTitle("Top Vehicle Earning Per Day in " + YEAR, WHITE, 14);

        // Defining axis info and styles
        AxisStyle yAxisStyle = AxisStyle.newAxisStyle(BLACK, 10, AxisTextAlignment.CENTER);
        AxisStyle xAxisStyle = AxisStyle.newAxisStyle(BLACK, 10, AxisTextAlignment.RIGHT);
        
        AxisLabels yAxis = AxisLabelsFactory.newNumericRangeAxisLabels(0, MAX);
        yAxis.setAxisStyle(yAxisStyle);
        AxisLabels xAxis1 = AxisLabelsFactory.newAxisLabels(months_row1, monthPositions_row1);
        xAxis1.setAxisStyle(xAxisStyle);
        AxisLabels xAxis2 = AxisLabelsFactory.newAxisLabels(months_row2, monthPositions_row2);
        xAxis2.setAxisStyle(xAxisStyle);
        
        chart.addYAxisLabels(yAxis);
        chart.addXAxisLabels(xAxis1);
        chart.addXAxisLabels(xAxis2);
        chart.setGrid(100, 100, 0, 0);
        
        for (int i = 0; i < 6; i++) {
        	double i1 = monthPositions_row2.get(i);
        	double i2 = i < 5 ? monthPositions_row1.get(i+1) : 100; 
        	chart.addVerticalRangeMarker(i1, i2, GRAY);
        }
        
        chart.setBackgroundFill(Fills.newSolidFill(WHITE));
        chart.setAreaFill(Fills.newSolidFill(Color.newColor("708090")));
        String url = chart.toURLString();		
        return url;
	}

}
