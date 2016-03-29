package dataengineering;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;

import java.util.ArrayList;

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

    public Graphs(Graph graph) {
        this.graph = graph;
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

    // user-defined function to select the minimum weight
    static final class SelectMinWeight implements ReduceEdgesFunction<Double> {

        @Override
        public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
            return Math.min(firstEdgeValue, secondEdgeValue);
        }
    }

    // get subgraphs with only edges which were created before timestamp
    private Graph getSubgraphUntilTimestamp(final long timestamp) throws Exception {
        Graph tempGraph = graph.subgraph(new FilterFunction<Vertex<Integer, Long>>() {
            @Override
            public boolean filter(Vertex<Integer, Long> integerNullValueVertex) throws Exception {
                return true;
            }
        }, new FilterFunction<Edge<Integer, Double>>() {
            @Override
            public boolean filter(Edge<Integer, Double> integerIntegerEdge) throws Exception {
                return ((integerIntegerEdge.getValue() < (timestamp)));
            }
        });
        return filterNodesWithoutEdges(tempGraph);
    }

    // remove orphans (nodes without edges)
    private Graph filterNodesWithoutEdges(Graph graph) throws Exception {
        DataSet<Tuple2<Integer, Long>> degrees = graph.getDegrees();
        DataSet<Tuple2<Integer, Long>> nodesWithoutEdges = degrees.filter(new FilterFunction<Tuple2<Integer, Long>>() {
            @Override
            public boolean filter(Tuple2<Integer, Long> integerLongTuple2) throws Exception {
                return integerLongTuple2.f1.equals(0l);
            }
        });
        ArrayList<Integer> ids = new ArrayList<Integer>();
        for (Tuple2<Integer, Long> node : nodesWithoutEdges.collect()) {
            ids.add(node.f0);
        }
        return graph.filterOnVertices(new FilterFunction<Vertex<Integer, Long>>() {
            @Override
            public boolean filter(Vertex<Integer, Long> integerLongVertex) throws Exception {
                return !(ids.contains(integerLongVertex.getId()));
            }
        });
    }

    // return an arraylist with subgraphs per year
    public ArrayList<Graphs> getSubgraphPerTimeArray(int time, int limit) throws Exception {
        // Reduce to min edge per vertex
        DataSet<Tuple2<Integer, Double>> minWeights = graph.reduceOnEdges(new SelectMinWeight(), EdgeDirection.ALL);
        // reduce to lowest edge overall
        DataSet<Tuple2<Integer, Double>> reducedMinWeights = minWeights.min(1);
        long minWeight = Math.round(reducedMinWeights.collect().get(0).f1);

        ArrayList<Graphs> subgraphs = new ArrayList<>();
        for(int i = 1; i <= limit; i++) {
            // create acculumative subgraphs for each year
            subgraphs.add(new Graphs(getSubgraphUntilTimestamp(minWeight + (i*time))));
        }
        return subgraphs;
    }
}
