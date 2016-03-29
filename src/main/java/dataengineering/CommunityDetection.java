package dataengineering;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

import java.util.HashMap;
import java.util.List;

/**
 * Created by giedomak on 18/03/2016.
 */
public class CommunityDetection {

    private Graph<Integer, Long, Double> result;
    private HashMap<Long, Integer> community_map = new HashMap<Long, Integer>();
    private long vertices;
    private long edges;
    private List<Vertex<Integer, Long>> vertices2;
    private List<Edge<Integer, Double>> edges2;

    public CommunityDetection(Graphs graph) throws Exception {

        // Max number op hops: 1, Delta: 0.5
        result = graph.getGraph().run(new org.apache.flink.graph.library.CommunityDetection<Integer>(1, 0.5));

    }

    public void calcStats() throws Exception {
        createCommunityMap();
        vertices = result.numberOfVertices();
        edges = result.numberOfEdges();
    }

    public void printStats() throws Exception {

        // Print stats
        System.out.println("# Vertices: " + vertices);
        System.out.println("# Edges: " + edges);
//        System.out.println(community_map);
        System.out.println("# Communities: " + community_map.size());
        System.out.println("# Avg nodes per community: " + (double) vertices / community_map.size());
    }


    public void createCommunityMap() throws Exception {
        // Count number of nodes within community and color the graph
        for (Vertex<Integer, Long> vertex : getVertices()) {
            Long c = vertex.getValue();
            if (community_map.containsKey(c)) {
                community_map.put(c, community_map.get(c) + 1);
            } else {
                community_map.put(c, 1);
            }
        }
    }

    public Graph<Integer, Long, Double> getResult() {
        return result;
    }

    public List<Vertex<Integer, Long>> getVertices() throws Exception {
        if(vertices2 == null) {
            this.vertices2 = result.getVertices().collect();
        }
        return vertices2;
    }

    public List<Edge<Integer, Double>> getEdges() throws Exception {
        if(edges2 == null) {
            this.edges2 = result.getEdges().collect();
        }
        return edges2;
    }

}
