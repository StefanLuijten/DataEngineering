package dataengineering;

import java.util.ArrayList;

/**
 * Created by giedomak on 21/03/2016.
 */
public class CommunityDetectionEvolution {

    private Graphs graph;
    private dataengineering.CommunityDetection cd;
    private ArrayList<Graphs> subgraphPerYearArray;
    private ArrayList<dataengineering.CommunityDetection> cds = new ArrayList();

    public CommunityDetectionEvolution(Graphs graph) throws Exception {

        this.graph = graph;
        this.subgraphPerYearArray = graph.getSubgraphPerYearArray();

        for(Graphs graph2 : subgraphPerYearArray) {
            // Community detection
            dataengineering.CommunityDetection cd = new dataengineering.CommunityDetection(graph2);
            cd.calcStats();
            cds.add(cd);
        }
        printStats();
    }

    public dataengineering.CommunityDetection getCommunityDetection() {
        return cd;
    }

    private void printStats() throws Exception {
        int i = 1;
        for(dataengineering.CommunityDetection cd : cds) {
            System.out.println("# " + i);
            cd.printStats();
            i++;
        }
    }

}
