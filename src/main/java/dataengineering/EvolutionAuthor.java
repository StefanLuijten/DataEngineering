package dataengineering;

import org.apache.avro.generic.GenericData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Stefan on 14-Mar-16.
 */
public class EvolutionAuthor {


    private Graphs graph;
    private Boolean relative;
    private ArrayList<HashMap<Integer,Double>> allPublications = new ArrayList<>();

    public EvolutionAuthor(Boolean relative, Graphs inputGraph) {
        this.graph = inputGraph;
        this.relative = relative;
    }

    public int[] getRandomPersons(Integer numberOfPeople) throws Exception {
        long numberOfVertices = graph.getGraph().numberOfVertices();
        Integer numberOfVerticesMax = (int) numberOfVertices;
        final int[] ints = new Random().ints(1, numberOfVerticesMax).distinct().limit(numberOfPeople).toArray();
        return ints;
    }

    public void createGraph(int[] persons) throws Exception {
        XChart chart = new XChart("Number of publications", "Date", "Number of publications");
        for (Integer person : persons) {
            addSeriesToChart(retrievePublicationsForPerson(person), person,chart);
        }
        chart.showGraph();
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

    public void addSeriesToChart(List<Double> publicationTimes, Integer nodeID, XChart chart) {
        List<Double> xValues = publicationTimes;

        List<Integer> yValues = new ArrayList<>();
        Integer counter = 1;
        for (Integer i = 0; i < xValues.size(); i++) {
            yValues.add(counter);
            counter++;
        }

        if (!xValues.isEmpty()) {

            if (!relative) {
                chart.addDateSeries(nodeID.toString(), transferToDate(xValues), yValues);
            } else {
                chart.addIntegerSeries(nodeID.toString(), transferToYear(xValues), yValues);
            }
        }
    }





    private List<Date> transferToDate(List<Double> integerList) {

        SimpleDateFormat format = new SimpleDateFormat("MMM dd,yyyy  hh:mm");

        List<Date> xAxisDateList = new ArrayList<Date>(integerList.size());
        for (Integer i = 0; i < integerList.size(); i++) {
            Date date = new java.util.Date(Math.round(integerList.get(i) * 1000));
            xAxisDateList.add(date);
        }

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

        // Merge all individual publications a year to one for all authors
        for (HashMap <Integer,Double> person : all){
            person.forEach((k, v) -> averagePublications.merge(k, v, (v1, v2) -> v1 + v2));
        }


        // Calculate average
        for(Map.Entry<Integer, Double> e : averagePublications.entrySet()) {
            averagePublications.put(e.getKey(), (e.getValue() / all.size()));
        }


        return averagePublications;
    }


    public void createAveragesGraph (int [] persons) throws Exception {

        HashMap<Integer,Double> combinedHashMap = getAverage(persons);
        List<Double> times = new ArrayList<>();
        List<Double> averages = new ArrayList();
        System.out.println(combinedHashMap);
        for (int i = 1; i <= combinedHashMap.size(); i++){
            System.out.println("loop");
            times.add((double) i);
            averages.add(combinedHashMap.get(i));
        }
        System.out.println(averages);
        XChart averageChart = new XChart("Averages per year", "Relative time(years)","Number of publications");
        averageChart.addDoubleSeries("Averages per year",times,averages);
        averageChart.showGraph();
    }

    public HashMap<Integer,Double> getAverage(int[] persons) throws Exception {
        for (Integer person : persons) {
            allPublications.add(getNumberOfPublicationPerYear(retrievePublicationsForPerson(person)));
        }
        return combineHashMaps(allPublications);
    }
}


