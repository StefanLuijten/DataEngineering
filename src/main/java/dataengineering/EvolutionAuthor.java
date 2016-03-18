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

    private Chart_XY chart;
    private Graphs graph;
    private Boolean relative;
    private ArrayList<HashMap<Integer,Double>> allPublications = new ArrayList<>();

    public EvolutionAuthor(Boolean relative, Graphs inputGraph) {

        this.chart = new ChartBuilder_XY().width(1080).height(900).xAxisTitle("Date").yAxisTitle("Number of publications").build();
        this.graph = inputGraph;
        this.relative = relative;
    }

    /**
     * Create a graph for numberOfPeople random authors.
     *
     * @param numberOfPeople
     * @throws Exception
     */
    public int[] getRandomPersons(Integer numberOfPeople) throws Exception {
        long numberOfVertices = graph.getGraph().numberOfVertices();
        Integer numberOfVerticesMax = (int) numberOfVertices;
        final int[] ints = new Random().ints(1, numberOfVerticesMax).distinct().limit(numberOfPeople).toArray();
        return ints;
    }

    /**
     * Create a graph for a preselected group of authors, which are provided by an integer array
     *
     * @param persons
     * @throws Exception
     */
    public void createGraph(int[] persons) throws Exception {
        for (Integer person : persons) {
            addSeriesToChart(retrievePublicationsForPerson(person), person);
        }
    }

    private List<Double> retrievePublicationsForPerson(Integer nodeID) throws Exception {
        Graph<Integer, Long, Double> subGraph = graph.getEdgesPerNode(nodeID);
        DataSet<Edge<Integer, Double>> edges = subGraph.getEdges();
        List<Double> xValues = new ArrayList<>();
        Double minDate = 2100000000.0;


        for (Edge<Integer, Double> edge : edges.collect()) {
            Double weight = edge.getValue();
               if (weight < 1015887600) {

            if (!xValues.contains(weight)) {
                xValues.add(weight);
                if (weight < minDate) {
                    minDate = weight;
                }
            }
                 }
        }

        if (relative) {
            xValues = makeTimeRelative(xValues, minDate);
        }
        Collections.sort(xValues);
        return xValues;
    }

    public void addSeriesToChart(List<Double> publicationTimes, Integer nodeID) {
        List<Double> xValues = publicationTimes;

        List<Integer> yValues = new ArrayList<>();
        Integer counter = 1;
        for (Integer i = 0; i < xValues.size(); i++) {
            yValues.add(counter);
            counter++;
        }

        if (!xValues.isEmpty()) {

            if (!relative) {
                chart.addSeries(nodeID.toString(), transferToDate(xValues), yValues);
            } else {
                chart.addSeries(nodeID.toString(), transferToYear(xValues), yValues);
            }
        }
    }


    public void showGraph() {
        if (relative) {
            chart.setXAxisTitle("Relative time");
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
            Date date = new java.util.Date(Math.round(integerList.get(i) * 1000));
            xAxisDateList.add(date);
        }
        chart.getStyler().setDatePattern("dd-MMM-YYYY");
        return xAxisDateList;
    }

    private List<Double> transferToYear(List<Double> integerList) {
        List<Double> xAxisDateList = new ArrayList<Double>(integerList.size());
        for (Integer i = 0; i < integerList.size(); i++) {
           Double year = (integerList.get(i) / 31556926);
            xAxisDateList.add(year);
        }
        return xAxisDateList;
    }

    private List<Double> makeTimeRelative(List<Double> list, Double minDate) {
        for (Integer i = 0; i < list.size(); i++) {
            list.set(i, list.get(i) - minDate);
        }
        return list;
    }

    private HashMap<Integer, Double> getNumberOfPublicationPerYear(List<Double> publications) {
        HashMap<Integer, Double> publicationsEachYear = new HashMap<>();

        while (!publications.isEmpty()) {
            Double time = publications.get(0);
            Integer year = (int) (time / 31556926) + 1;

            if (publicationsEachYear.containsKey(year)) {
                publicationsEachYear.put(year, publicationsEachYear.get(year) + 1);
            } else {
                publicationsEachYear.put(year, 1.0);
            }
            publications.remove(0);
        }
        return publicationsEachYear;
    }

    private HashMap<Integer,Double> combineHashMaps(ArrayList<HashMap<Integer,Double>> all){
         HashMap<Integer,Double> averagePublications = new HashMap<>();

        for (HashMap <Integer,Double> person : all){
            person.forEach((k, v) -> averagePublications.merge(k, v, (v1, v2) -> v1 + v2));
        }

        for(Map.Entry<Integer, Double> e : averagePublications.entrySet()) {
            System.out.println(averagePublications);
            averagePublications.put(e.getKey(), (e.getValue() / averagePublications.size()));
            System.out.println(averagePublications);
        }
        return averagePublications;
    }

    public void getAverage(int[] persons) throws Exception {
        for (Integer person : persons) {
            allPublications.add(getNumberOfPublicationPerYear(retrievePublicationsForPerson(person)));
        }


        System.out.println(combineHashMaps(allPublications));
    }
}


