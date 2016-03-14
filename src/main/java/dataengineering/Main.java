package dataengineering;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.CommunityDetection;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created by Stefan on 01-Mar-16.
 */
public class Main {
    static HashMap<Long, String> colors = new HashMap<Long, String>();

    public static void main(String[] args) throws Exception {

        // Set graph renderer to use CSS
        System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParseInput input = new ParseInput("test", env);
        DataSet<Tuple3<Integer, Integer, Double>> edgeSet;
        DataSet<Tuple2<Integer, Long>> vertexSet;
        edgeSet = input.getEdgeSet();
        vertexSet = input.getVerticeSet();

        Graphs graph = new Graphs(vertexSet, edgeSet);
        System.out.println(graph.getGraph().numberOfVertices());

        // Max number op hops: 1, Delta: 0.5
        Graph result = graph.getGraph().run(new CommunityDetection<Integer>(1, 0.5));
        List<Vertex<Integer, Long>> vertices = result.getVertices().collect();
        List<Tuple3<Integer, Integer, Double>> edges = result.getEdges().collect();

        GraphVisualization gv = new GraphVisualization(vertices, edges);

        // Count number of nodes within community and color the graph
        HashMap<Long, Integer> community_map = new HashMap<Long, Integer>();
        for(Vertex<Integer, Long> vertex : vertices) {
            Long c = vertex.getValue();
            if (community_map.containsKey(c)) {
                community_map.put(c, community_map.get(c) + 1);
            } else {
                community_map.put(c, 1);
            }
            gv.getGraph().getNode(vertex.getId().toString()).addAttribute("ui.style", "fill-color: " + getCommunityColor(c) +";");
        }


        System.out.println(community_map);
        System.out.println("# Communities: " + community_map.size());
        System.out.println("# Vertices: " + vertices.size());
        System.out.println("# Edges: " + edges.size());

        gv.displayGraph();

    }

    static private String getCommunityColor(Long c) {
        if (colors.containsKey(c)) {
            return colors.get(c);
        } else {
            Random rand = new Random();
            int r = rand.nextInt(255);
            int g = rand.nextInt(255);
            int b = rand.nextInt(255);
            String color = "rgb("+r+","+g+","+b+")";
            colors.put(c, color);
            return color;
        }
    }
}
