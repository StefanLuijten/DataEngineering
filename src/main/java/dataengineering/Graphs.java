package dataengineering;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
    private Graph getSubgraphUntilTimestamp(final long timestamp) {
        return graph.subgraph(new FilterFunction<Vertex<Integer, Long>>() {
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
    }

    private Graph filterNodesWithoutEdges() {
        return graph.filterOnVertices(new FilterFunction<Vertex<Integer, Long>>() {
            @Override
            public boolean filter(Vertex<Integer, Long> integerNullValueVertex) throws Exception {
                return true;
            }
        });
    }

    // return an arraylist with subgraphs per year
    public ArrayList<Graphs> getSubgraphPerYearArray() throws Exception {
        // Reduce to min edge per vertex
        DataSet<Tuple2<Integer, Double>> minWeights = graph.reduceOnEdges(new SelectMinWeight(), EdgeDirection.ALL);
        // reduce to lowest edge overall
        DataSet<Tuple2<Integer, Double>> reducedMinWeights = minWeights.reduce(new ReduceFunction<Tuple2<Integer, Double>>() {
            @Override
            public Tuple2<Integer, Double> reduce(Tuple2<Integer, Double> integerDoubleTuple2, Tuple2<Integer, Double> t1) throws Exception {
                if(Math.min(integerDoubleTuple2.f1, t1.f1) == t1.f1) {
                    return t1;
                } else {
                    return integerDoubleTuple2;
                }
            }
        });
        long minWeight = Math.round(reducedMinWeights.collect().get(0).f1);

        ArrayList<Graphs> subgraphs = new ArrayList<>();
        for(int i = 1; i < 10; i++) {
            // create acculumative subgraphs for each year
            subgraphs.add(new Graphs(getSubgraphUntilTimestamp(minWeight + (i*31556926))));
        }
        return subgraphs;
    }
}
