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

        // Community detection evolution
        CommunityDetectionEvolution cd = new CommunityDetectionEvolution(graph);

        // Visualize community detection
        boolean _gv = false;
        if(_gv) {
            GraphVisualization gv = new GraphVisualization(cd.getCommunityDetection().getVertices(), cd.getCommunityDetection().getEdges());
            gv.colorCommunities();
            gv.displayGraph();
        }

        // Visualize publications per Author
        EvolutionAuthor evolution = new EvolutionAuthor(graph,140);

        int[] persons = {1,2};
//        evolution.createGraph(persons);

//        evolution.createGraph(false);
//        evolution.createGraph(true);
//        evolution.createAveragesGraph();
        evolution.getNumberOfPublicationsPerSeasonPie();
//        evolution.testPie();
    }
}
