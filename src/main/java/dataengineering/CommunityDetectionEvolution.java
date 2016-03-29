package dataengineering;

import java.util.ArrayList;

/**
 * Created by giedomak on 21/03/2016.
 */
public class CommunityDetectionEvolution {

    private Graphs graph;
    private ArrayList<Graphs> subgraphPerYearArray;

    public CommunityDetectionEvolution(Graphs graph) throws Exception {

        this.graph = graph;
        this.subgraphPerYearArray = graph.getSubgraphPerYearArray();

        for(Graphs graph2 : subgraphPerYearArray) {
            // Community detection
            dataengineering.CommunityDetection cd = new dataengineering.CommunityDetection(graph2);
            cd.printStats();
        }
    }
}
