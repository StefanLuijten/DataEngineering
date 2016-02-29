package dataengineering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class ParseInput{

    //
    //	Program
    //

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input dataEngineering\\src\\main
        DataSet<String> text = env.readTextFile("src/main/resources/datasets/HepPh");


        DataSet<Tuple3<Integer,Integer,Integer>> publication = text
                .flatMap(new LineSplitter());

        publication.print();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple3<Integer,Integer,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<Integer,Integer,Integer>> out) {
            String[] strArray = line.split(" ");
            int[] intArray = new int[strArray.length];
            for (int i =0; i < intArray.length; i++) {
                intArray[i] = Integer.parseInt(strArray[i]);
              }
                out.collect(new Tuple3<Integer,Integer,Integer>(intArray[0],intArray[1],intArray[3]));
            }
        }
    }

