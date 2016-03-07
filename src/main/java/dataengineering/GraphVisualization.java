package dataengineering;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;

import java.util.List;

/**
 * Created by Stefan on 07-Mar-16.
 */
public class GraphVisualization {

    public Graph graph = new SingleGraph("...");

    public void setEdges(List<Tuple3<Integer,Integer,Integer>> edgeList){
        edgeList.forEach((temp) ->{
        graph.addEdge(temp.f0.toString()+temp.f1.toString(),temp.f0.toString(),temp.f1.toString());
        });
    }

    public void setVertices(List<Tuple2<Integer,NullValue>> vertexList){
        vertexList.forEach((temp) -> {
            graph.addNode(temp.f0.toString());
        });
    }

    public void display(){
        graph.display();
    }
}
