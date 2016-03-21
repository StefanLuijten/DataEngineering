package dataengineering;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * Created by giedomak on 29/02/2016.
 */

public class Graphs {

    private Graph<Integer, Long, Double> graph;

    public Graphs(ParseInput input, ExecutionEnvironment env) throws Exception {

        graph = Graph.fromTupleDataSet(input.getVerticeSet(),input.getEdgeSet(),env);

        // Print stats
//        System.out.println("# Vertices: " + graph.numberOfVertices());
//        System.out.println("# Edges: " + graph.numberOfEdges());

    }

    public Graph<Integer, Long, Double> getGraph() {
        return graph;
    }
    public Graph<Integer, Long, Double> getEdgesPerNode(final Integer nodeID){
        return graph.subgraph(new FilterFunction<Vertex<Integer, Long>>() {
            @Override
            public boolean filter(Vertex<Integer, Long> integerNullValueVertex) throws Exception {
                return true;
            }
        }, new FilterFunction<Edge<Integer, Double>>() {
            @Override
            public boolean filter(Edge<Integer, Double> integerIntegerEdge) throws Exception {
                  return ((integerIntegerEdge.getSource().equals(nodeID)) || (integerIntegerEdge.getTarget().equals(nodeID)));
            }
        });
    }
}
