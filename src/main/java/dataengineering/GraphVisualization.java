package dataengineering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;

/**
 * Created by Stefan on 07-Mar-16.
 */
public class GraphVisualization {

    public Graph graph = new SingleGraph("Tutorial 1");

    public GraphVisualization(DataSet<Tuple2<Integer, NullValue>> verticeSet, DataSet<Tuple3<Integer, Integer, Integer>> edgeSet) throws Exception {

        verticeSet.map(new VertexAdder());
     //   edgeSet.map(new EdgeAdder());
        graph.display();

    }

    public class VertexAdder implements MapFunction<Tuple2<Integer, NullValue>, Object> {
        @Override
        public NullValue map(Tuple2<Integer, NullValue> vertexSet) throws Exception {
            
          //  graph.addNode(Integer.toString(vertexSet.f0));
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
