package dataengineering;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.knowm.xchart.ChartBuilder_XY;
import org.knowm.xchart.Chart_XY;
import org.knowm.xchart.SwingWrapper;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Stefan on 14-Mar-16.
 */
public class EvolutionAuthor {

    private Integer numberOfPeople = 0;
    private Chart_XY chart;
    private Graphs graph;
    private Boolean relative;

    public EvolutionAuthor(Boolean relative, Graphs inputGraph) {

        this.chart = new ChartBuilder_XY().width(1080).height(900).xAxisTitle("Date").yAxisTitle("Number of publications").build();
        this.graph = inputGraph;
        this.relative = relative;
    }

    /**
     * Create a graph for numberOfPeople random authors.
     * @param numberOfPeople
     * @throws Exception
     */
    public void setPersonsRandom(Integer numberOfPeople) throws Exception {
        long numberOfVertices = graph.getGraph().numberOfVertices();
        Integer numberOfVerticesMax = (int) numberOfVertices;
        final int[] ints = new Random().ints(1, numberOfVerticesMax).distinct().limit(numberOfPeople).toArray();
        setPersons(ints);

    }

    /**
     * Create a graph for a preselected group of authors, which are provided by an integer array
     * @param persons
     * @throws Exception
     */
    public void setPersons(int[] persons) throws Exception {
        for (Integer person : persons) {
            retrievePublicationsForPerson(person);
        }
    }

    private void retrievePublicationsForPerson(Integer nodeID) throws Exception {
        Graph<Integer, Long, Double> subGraph = graph.getEdgesPerNode(nodeID);
        DataSet<Edge<Integer, Double>> edges = subGraph.getEdges();
        List<Double> xValues = new ArrayList<>();
        List<Integer> yValues = new ArrayList<>();
        Integer counter = 1;
        Double minDate = 2100000000.0;


        for (Edge<Integer, Double> edge : edges.collect()) {
            Double weight = edge.getValue();
            if (weight < 1015887600) {
                xValues.add(weight);
                if (weight < minDate) {
                    minDate = weight;
                }

                yValues.add(counter);
                counter++;
            }
        }

        if (relative) {
            this.chart.setXAxisTitle("Relative time");
            for (Integer i = 0; i < xValues.size(); i++) {
                xValues.set(i, xValues.get(i) - minDate);
            }
        }

        if (!xValues.isEmpty()) {
            Collections.sort(xValues);
            if (!relative) {
                List<Date> xDateList = transferToDate(xValues);
                chart.addSeries(nodeID.toString(), xDateList, yValues);
            } else {
                chart.addSeries(nodeID.toString(), xValues, yValues);
            }
        }
    }

    public void showGraph() {
        if (relative) {
            chart.setTitle("Relative chart");
        } else {
            chart.setTitle("Absolute chart");
        }
        new SwingWrapper(chart).displayChart();
    }

    private List<Date> transferToDate(List<Double> integerList) {

        SimpleDateFormat format = new SimpleDateFormat("MMM dd,yyyy  hh:mm");

        List<Date> xAxisDateList = new ArrayList<Date>(integerList.size());
        for (Integer i = 0; i < integerList.size(); i++) {
            Date date = new java.util.Date( Math.round(integerList.get(i) * 1000));
            xAxisDateList.add(date);
        }
        chart.getStyler().setDatePattern("dd-MMM-YYYY");
        return xAxisDateList;
    }
}


