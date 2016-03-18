package dataengineering;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import java.util.List;

/**
 * Created by giedomak on 18/03/2016.
 */
public class CommunityDetection {

    private Graph<Integer, Long, Double> result;


    public CommunityDetection(Graphs graph) throws Exception {

        // Max number op hops: 1, Delta: 0.5
        result = graph.getGraph().run(new org.apache.flink.graph.library.CommunityDetection<Integer>(1, 0.5));

    }

    public Graph<Integer, Long, Double> getResult() {
        return result;
    }

    public List<Vertex<Integer, Long>> getVertices() throws Exception {
        return result.getVertices().collect();
    }

    public List<Edge<Integer, Double>> getEdges() throws Exception {
        return result.getEdges().collect();
    }

}
