package dataengineering;

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
    private ArrayList<HashMap<Integer, Double>> allPublications = new ArrayList<>();
    private static final String seasons[] = {
            "Winter", "Winter",
            "Spring", "Spring", "Spring",
            "Summer", "Summer", "Summer",
            "Fall", "Fall", "Fall",
            "Winter"
    };
    private int[] persons;

    public EvolutionAuthor(Graphs inputGraph, Integer number) throws Exception {
        this.graph = inputGraph;
        getRandomPersons(number);
    }

    public boolean personAlreadyInList(int person) {
        for (int i = 0; i < persons.length; i++) {
            if (persons[i] == person) {
                return true;
            }
        }
        return false;
    }

    private void getRandomPersons(Integer numberOfPeople) throws Exception {
        long numberOfVertices = graph.getGraph().numberOfVertices();
        Integer numberOfVerticesMax = (int) numberOfVertices;
        this.persons = new Random().ints(1, numberOfVerticesMax).distinct().limit(numberOfPeople).toArray();
    }

    public void createGraph(boolean relative) throws Exception {
        long numberOfVertices = graph.getGraph().numberOfVertices();
        Integer numberOfVerticesMax = (int) numberOfVertices;

        XChart chart = new XChart("Number of publications", "Date", "Number of publications");
        for (int i = 0; i < persons.length; i++) {
            List<Double> publications = retrievePublicationsForPerson(persons[i], relative);
            while (publications.isEmpty()) {
                int newPerson = new Random().nextInt(numberOfVerticesMax) + 1;
                if (!personAlreadyInList(newPerson)) {
                    publications = retrievePublicationsForPerson(newPerson, relative);
                    persons[i] = newPerson;
                }
            }
            addSeriesToChart(publications, persons[i], chart, relative);
        }
        chart.showGraph();
    }

    private List<Double> retrievePublicationsForPerson(Integer nodeID, boolean relative) throws Exception {
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

    public void addSeriesToChart(List<Double> publicationTimes, Integer nodeID, XChart chart, boolean relative) {
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

    private HashMap<Integer, Double> combineNumberOfPublications(ArrayList<HashMap<Integer, Double>> all) {
        HashMap<Integer, Double> averagePublications = new HashMap<>();

        // Merge all individual publications a year to one for all authors
        for (HashMap<Integer, Double> person : all) {
            person.forEach((k, v) -> averagePublications.merge(k, v, (v1, v2) -> v1 + v2));
        }


        // Calculate average
        for (Map.Entry<Integer, Double> e : averagePublications.entrySet()) {
            averagePublications.put(e.getKey(), (e.getValue() / all.size()));
        }
        return averagePublications;
    }

    private HashMap<Integer, Integer> combineNumberOfPublicationsASeason(List<HashMap<Integer, Integer>> all) {
        HashMap<Integer, Integer> averagePublications = new HashMap<>();

        // Merge all individual publications a year to one for all authors
        for (HashMap<Integer, Integer> person : all) {
            person.forEach((k, v) -> averagePublications.merge(k, v, (v1, v2) -> v1 + v2));
        }
        return averagePublications;
    }

    public void createAveragesGraph() throws Exception {

        HashMap<Integer, Double> combinedHashMap = getAverage(persons);
        List<Double> times = new ArrayList<>();
        List<Double> averages = new ArrayList();

        for (int i = 1; i <= combinedHashMap.size(); i++) {
            times.add((double) i);
            averages.add(combinedHashMap.get(i));
        }

        XChart averageChart = new XChart("Averages per year", "Relative time(years)", "Number of publications");
        averageChart.addDoubleSeries("Averages per year", times, averages);
        averageChart.showGraph();
    }

    public HashMap<Integer, Double> getAverage(int[] persons) throws Exception {
        for (Integer person : persons) {
            allPublications.add(getNumberOfPublicationPerYear(retrievePublicationsForPerson(person, true)));
        }
        return combineNumberOfPublications(allPublications);
    }

    public String getSeason(Date date) {
        return seasons[date.getMonth()];
    }

    private HashMap<Integer, Integer> getNumberOfPublicationPerSeasonPerPerson(List<Double> publications) {
        List<Date> publicationsPerDate = transferToDate(publications);

        HashMap<Integer, Integer> publicationsPerSeason = new HashMap<>();


        while (!publicationsPerDate.isEmpty()) {
            Date date = publicationsPerDate.get(0);
            String season = getSeason(date);

            switch (season) {
                case "Winter":
                    if (publicationsPerSeason.containsKey(0)) {
                        publicationsPerSeason.put(0, publicationsPerSeason.get(0) + 1);
                    } else {
                        publicationsPerSeason.put(0, 1);
                    }
                    break;
                case "Spring":
                    if (publicationsPerSeason.containsKey(1)) {
                        publicationsPerSeason.put(1, publicationsPerSeason.get(1) + 1);
                    } else {
                        publicationsPerSeason.put(1, 1);
                    }
                    break;
                case "Summer":
                    if (publicationsPerSeason.containsKey(2)) {
                        publicationsPerSeason.put(2, publicationsPerSeason.get(2) + 1);
                    } else {
                        publicationsPerSeason.put(2, 1);
                    }
                    break;
                case "Fall":
                    if (publicationsPerSeason.containsKey(3)) {
                        publicationsPerSeason.put(3, publicationsPerSeason.get(3) + 1);
                    } else {
                        publicationsPerSeason.put(3, 1);
                    }
                    break;
            }

            publicationsPerDate.remove(0);
        }
        return publicationsPerSeason;
    }

    public void getNumberOfPublicationsPerSeasonPie() throws Exception {
        List<HashMap<Integer, Integer>> all = new ArrayList<>();
        for (Integer person : persons) {
            all.add(getNumberOfPublicationPerSeasonPerPerson(retrievePublicationsForPerson(person, false)));
        }
        HashMap<Integer, Integer> combined = combineNumberOfPublicationsASeason(all);

        PieChart chart = new PieChart("Seasons");
        chart.addData("Winter", combined.get(0));
        chart.addData("Spring", combined.get(1));
        chart.addData("Summer", combined.get(2));
        chart.addData("Fall", combined.get(3));
        chart.createDemoPanel();
    }
}


