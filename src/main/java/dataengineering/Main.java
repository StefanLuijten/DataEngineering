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
        ParseInput input = new ParseInput("HepPh", env);

        // Gelly graph
        Graphs graph = new Graphs(input, env);

          // Community detection
//        dataengineering.CommunityDetection cd = new dataengineering.CommunityDetection(graph);

          // Visualize community detection
//        GraphVisualization gv = new GraphVisualization(cd.getVertices(), cd.getEdges());
//        gv.colorCommunities();
//        gv.displayGraph();

        // Visualize publications per Author
        EvolutionAuthor evolution = new EvolutionAuthor(true, graph);


        int[] persons = {4,5};
        int[] randPersons = evolution.getRandomPersons(500);

      //  evolution.createGraph(randPersons);
        //  evolution.setPersonsRandom(3);
        evolution.createAveragesGraph(randPersons);
    }
}
