package dataengineering;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

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

    private Graph<Integer, NullValue, Integer> graph;

    public Graphs(DataSet<Tuple2<Integer, NullValue>> verticeSet, DataSet<Tuple3<Integer, Integer, Integer>> edgeSet){


        // set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        graph = Graph.fromTupleDataSet(verticeSet,edgeSet,env);

    }

    public Graph<Integer, NullValue, Integer> getGraph() {
        return graph;
    }
}
