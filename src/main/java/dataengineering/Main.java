package dataengineering;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Created by Stefan on 01-Mar-16.
 */
public class Main {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // input parser
        ParseInput input = new ParseInput("test", env);

        // Gelly graph
        Graphs graph = new Graphs(input, env);
        System.out.println(graph.getGraph().numberOfVertices());

        // Community detection
        dataengineering.CommunityDetection cd = new dataengineering.CommunityDetection(graph);

        // Visualize community detection
        GraphVisualization gv = new GraphVisualization(cd.getVertices(), cd.getEdges());
        gv.colorCommunities();
        gv.displayGraph();

        // Visualize publications
        EvolutionAuthor evolution = new EvolutionAuthor(true,graph);

        int[] persons = {5,6,123,22233};
        evolution.setPersons(persons);
        //  evolution.setPersonsRandom(3);

        evolution.showGraph();

    }
}
