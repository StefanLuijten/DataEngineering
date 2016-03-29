package dataengineering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.SingleGraph;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created by Stefan on 07-Mar-16.
 */
public class GraphVisualization {

    private static HashMap<Long, String> colors = new HashMap<Long, String>();
    private static Graph graph = new SingleGraph("Tutorial 1");
    private List<Vertex<Integer, Long>> verticeSet;
    private static boolean displaying = false;

    public GraphVisualization(List<Vertex<Integer, Long>> verticeSet, List<Edge<Integer, Double>> edgeSet, String title) throws Exception {

        this.verticeSet = verticeSet;

        // Set graph renderer to use CSS
        System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        graph.addAttribute("ui.title", title);

        System.out.println("Mapping...");

        int i = 0;
        int j = 0;
        int max = 100000;
        for(Vertex<Integer, Long> tuple : verticeSet) {
            String id = Integer.toString(tuple.f0);
            try {
                graph.addNode(Integer.toString(tuple.f0));
                if(i >= max) {
                    break;
                }
                i++;
            } catch(org.graphstream.graph.IdAlreadyInUseException e) {}
        }
        for(Edge<Integer, Double> tuple: edgeSet) {
            try {
                graph.addEdge((tuple.f0.toString()+";"+tuple.f1.toString()), tuple.f0.toString(), tuple.f1.toString());
                j++;
            } catch(Exception e) {}

        }

        System.out.println("Mapping done");

        // Print stats
        System.out.println("GV: # Vertices: " + i);
        System.out.println("GV: # Edges: " + j);

    }

    public Graph getGraph() {
        return graph;
    }

    public void displayGraph() {
        if(!displaying) {
            graph.display();
            displaying = true;
        }
    }

    public void colorCommunities() {

        // Count number of nodes within community and color the graph
        HashMap<Long, Integer> community_map = new HashMap<Long, Integer>();
        for(Vertex<Integer, Long> vertex : verticeSet) {
            Long c = vertex.getValue();
            if (community_map.containsKey(c)) {
                community_map.put(c, community_map.get(c) + 1);
            } else {
                community_map.put(c, 1);
            }
            graph.getNode(vertex.getId().toString()).addAttribute("ui.style", "fill-color: " + getCommunityColor(c) +";");
        }

        // Print stats
        System.out.println(community_map);
        System.out.println("# Communities: " + community_map.size());

    }

    private String getCommunityColor(Long c) {
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

    public class VertexAdder implements MapFunction<Tuple2<Integer, NullValue>, Object> {
        @Override
        public NullValue map(Tuple2<Integer, NullValue> vertexSet) throws Exception {
            graph.addNode(Integer.toString(vertexSet.f0));
            return NullValue.getInstance();
        }
    }

    public class EdgeAdder implements MapFunction<Tuple3<Integer, Integer, Integer>, NullValue> {

        @Override
        public NullValue map(Tuple3<Integer, Integer, Integer> edgeSet) throws Exception {
            graph.addEdge("",edgeSet.f0.toString(),edgeSet.f1.toString());
            return NullValue.getInstance();
        }
    }
}
