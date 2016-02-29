package dataengineering;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

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
public class LoadGraph {

    //
    //	Program
    //

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        List<Vertex<Long, String>> vertexList = new ArrayList();
        vertexList.add(new Vertex<Long,String>(1L, "Henk"));
        vertexList.add(new Vertex<Long,String>(2L, "Piet"));
        vertexList.add(new Vertex<Long,String>(3L, "Jan"));

        List<Edge<Long, Double>> edgeList = new ArrayList();
        edgeList.add(new Edge<Long, Double>(1L, 2L, 1.0));
        edgeList.add(new Edge<Long, Double>(1L, 3L, 1.0));
        edgeList.add(new Edge<Long, Double>(2L, 3L, 1.0));

        Graph<Long, String, Double> graph = Graph.fromCollection(vertexList, edgeList, env);

        System.out.println("hey");
        System.out.println(graph.numberOfEdges());

    }
}
