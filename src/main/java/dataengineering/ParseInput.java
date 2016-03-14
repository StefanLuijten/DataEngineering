package dataengineering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;


public class ParseInput{

   private DataSet<String> input = null;
   private DataSet<Tuple3<Integer, Integer, Integer>> edgeSet;
   private DataSet<Tuple2<Integer, NullValue>> verticeSet;


    // set up the execution environment
    private ExecutionEnvironment env;


    public ParseInput(String filelocations, ExecutionEnvironment env) throws Exception {
        this.env = env;

        // retrieve data from given file location

        switch (filelocations) {
            case "Dblp":
                input = env.readTextFile("src/main/resources/datasets/Dblp");
                break;
            case "HepPh":
                input = env.readTextFile("src/main/resources/datasets/HepPh");
                break;
            case "HepTh":
                input = env.readTextFile("src/main/resources/datasets/HepTh");
                break;
            case "test":
                input = env.readTextFile("src/main/resources/datasets/test");
                break;
        }

        // parse received data for each line.
        this.setEdgeSet(input.flatMap(new LineSplitterEdges()));

        // parse the edges set to retrieve all vertices and store them.
        this.setVerticeSet(this.getEdgeSet().flatMap(new LineSplitterVertices()));

        // remove duplicate vertices
        this.verticeSet = verticeSet.distinct();
    }

    public DataSet<Tuple3<Integer, Integer, Integer>> getEdgeSet() {
        return this.edgeSet;
    }

    private void setEdgeSet(DataSet<Tuple3<Integer, Integer, Integer>> edgeSet) {
        this.edgeSet = edgeSet;
    }

    public DataSet<Tuple2<Integer, NullValue>> getVerticeSet() {
        return verticeSet;
    }

    private void setVerticeSet(DataSet<Tuple2<Integer, NullValue>> verticeSet) {
        this.verticeSet = verticeSet;
    }

    private static class LineSplitterEdges implements FlatMapFunction<String, Tuple3<Integer,Integer,Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple3<Integer,Integer,Integer>> out) {
            String[] strArray = line.split(" ");
            int[] intArray = new int[strArray.length];
            for (int i =0; i < intArray.length; i++) {
                intArray[i] = Integer.parseInt(strArray[i]);
              }
                out.collect(new Tuple3<>(intArray[0],intArray[1],intArray[3]));
            }
        }

    private static class LineSplitterVertices implements FlatMapFunction<Tuple3<Integer,Integer,Integer>,Tuple2<Integer,NullValue>> {

        @Override
        public void flatMap(Tuple3<Integer, Integer, Integer> edgeSet, Collector<Tuple2<Integer, NullValue>> collector) throws Exception {
          collector.collect(new Tuple2<>(edgeSet.f0, NullValue.getInstance()));
          collector.collect(new Tuple2<>(edgeSet.f1, NullValue.getInstance()));
           }
        }
    }

