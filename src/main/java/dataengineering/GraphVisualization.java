package dataengineering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.SingleGraph;

import java.util.List;

/**
 * Created by Stefan on 07-Mar-16.
 */
public class GraphVisualization {

    public static Graph graph = new SingleGraph("Tutorial 1");

    public GraphVisualization(List<Vertex<Integer, Long>> verticeSet, List<Tuple3<Integer, Integer, Double>> edgeSet) throws Exception {

        System.out.println("Mapping...");
//        verticeSet.map(new VertexAdder());
//        edgeSet.map(new EdgeAdder());
        int i = 0;
        int max = 100000;
        for(Vertex<Integer, Long> tuple : verticeSet) {
            String id = Integer.toString(tuple.f0);
            try {
                graph.addNode(Integer.toString(tuple.f0));
                if(i >= max) {
                    break;
                }
                i++;
            } catch(org.graphstream.graph.IdAlreadyInUseException e) {
//                System.out.println(e);
            }
        }
        for(Tuple3<Integer, Integer, Double> tuploe: edgeSet) {
            try {
                graph.addEdge((tuploe.f0.toString()+tuploe.f1.toString()), tuploe.f0.toString(), tuploe.f1.toString());
            } catch(Exception e) {
//                System.out.println(e);
            }

        }

        System.out.println("Mapping done");
//        verticeSet.collect();
//        edgeSet.collect();
//        graph.getNodeSet();
//        graph.addNode(Integer.toString(1));
//        graph.addNode(Integer.toString(2));
//        graph.addNode(Integer.toString(3));

//        graph.display();


    }

    public Graph getGraph() {
        return graph;
    }

    public void displayGraph() {
        graph.display();
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
