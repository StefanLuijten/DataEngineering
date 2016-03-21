package dataengineering;

import org.knowm.xchart.ChartBuilder_XY;
import org.knowm.xchart.Chart_XY;
import org.knowm.xchart.SwingWrapper;

import java.util.Date;
import java.util.List;

/**
 * Created by Stefan on 18-Mar-16.
 */
public class XChart {

    private Chart_XY chart;

    public XChart(String title, String xAxisTitle, String yAxisTitle){
        this.chart = new ChartBuilder_XY().width(1080).height(900).xAxisTitle(xAxisTitle).yAxisTitle(yAxisTitle).build();
        chart.setTitle(title);
        chart.getStyler().setDatePattern("dd-MMM-YYYY");
    }

    public void addIntegerSeries(String name, List<Double> xAxis, List<Integer> yAxis){
        chart.addSeries(name, xAxis, yAxis);
    }

    public void addDoubleSeries(String name, List<Double> xAxis, List<Double> yAxis){
        chart.addSeries(name, xAxis, yAxis);
    }

    public void addDateSeries(String name, List<Date> xAxis, List<Integer> yAxis){
        chart.addSeries(name, xAxis, yAxis);
    }

    public void showGraph() {
        new SwingWrapper(chart).displayChart();
    }



}
