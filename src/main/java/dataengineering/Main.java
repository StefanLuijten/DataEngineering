package dataengineering;

import eu.stratosphere.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;

/**
 * Created by Stefan on 01-Mar-16.
 */
public class Main {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParseInput input = new ParseInput("test");
        DataSet<Tuple3<Integer, Integer, Integer>> edgeSet;
        DataSet<Tuple2<Integer, NullValue>> vertexSet;
        edgeSet = input.getEdgeSet();
        vertexSet = input.getVerticeSet();
        System.out.println("Test");
        Graphs graph = new Graphs(vertexSet,edgeSet);
        graph.getGraph().getEdges().print();

         GraphVisualization gv = new GraphVisualization(vertexSet,edgeSet);
        }
    }


