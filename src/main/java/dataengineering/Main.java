package dataengineering;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Scanner;

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

        // Community detection evolution
        CommunityDetectionEvolution cde = new CommunityDetectionEvolution(graph);
        cde.runPerMonth(12*4);

        // Visualize community detection
        boolean _gv = true;
        if(_gv) {
            while(true) {
                int i = 1;
                for (dataengineering.CommunityDetection cd : cde.getCommunityDetections()) {
                    boolean reset = false;
                    if(i == 1) {
                        reset = true;
                    }
                    GraphVisualization gv = new GraphVisualization(cd.getVertices(), cd.getEdges(), Integer.toString(i), reset);
                    gv.colorCommunities();
                    gv.displayGraph();

                    // Wait for user input
                    Scanner s = new Scanner(System.in);
                    s.nextLine();

                    i++;

                }
            }
        }

        // Visualize publications per Author
        boolean _evolution = false;
        if(_evolution) {
            EvolutionAuthor evolution = new EvolutionAuthor(graph, 140);

            int[] persons = {1, 2};
            //        evolution.createGraph(persons);

            //        evolution.createGraph(false);
            //        evolution.createGraph(true);
            //        evolution.createAveragesGraph();
            evolution.getNumberOfPublicationsPerSeasonPie();
            //        evolution.testPie();
        }
    }
}
