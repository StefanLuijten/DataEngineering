package dataengineering;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;

/**
 * Created by giedomak on 29/02/2016.
 */

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class Graphs {

    private Graph<Integer, Long, Double> graph;

    public Graphs(DataSet<Tuple2<Integer, Long>> verticeSet, DataSet<Tuple3<Integer, Integer, Double>> edgeSet){


        // set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        graph = Graph.fromTupleDataSet(verticeSet,edgeSet,env);

    }

    public Graph<Integer, Long, Double> getGraph() {
        return graph;
    }

//    public Graph<Integer, NullValue, Integer> getEdgesPerNode(final Integer nodeID){
//        return graph.subgraph(new FilterFunction<Vertex<Integer, NullValue>>() {
//            @Override
//            public boolean filter(Vertex<Integer, NullValue> integerNullValueVertex) throws Exception {
//                return true;
//            }
//        }, new FilterFunction<Edge<Integer, Integer>>() {
//            @Override
//            public boolean filter(Edge<Integer, Integer> integerIntegerEdge) throws Exception {
//            System.out.println("EDGE:" + integerIntegerEdge.getSource() + " "  +integerIntegerEdge.getTarget());
//                System.out.println((integerIntegerEdge.getSource().equals(nodeID)) || (integerIntegerEdge.getTarget().equals(nodeID)));
//                return ((integerIntegerEdge.getSource().equals(nodeID)) || (integerIntegerEdge.getTarget().equals(nodeID)));
//            }
//        });
//    }
}
